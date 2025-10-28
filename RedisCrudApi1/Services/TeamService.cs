using Microsoft.Extensions.Configuration;
using Microsoft.VisualBasic;
using StackExchange.Redis;
using System.Data.Common;
using System.Windows.Markup;
using Models;

namespace RedisCrudApi.Services
{
    public class TeamService
    {
        private readonly IConnectionMultiplexer _mux;
        private readonly IConfiguration _config;

        private readonly Dictionary<string, IDatabase> nodes = new Dictionary<string, IDatabase>();


        public TeamService(IConnectionMultiplexer mux, IConfiguration config)
        {
            var connection = ConnectionMultiplexer.Connect("localhost:6379");

            _mux = mux;
            _config = config;

            string[] nodeNames = _config.GetSection("Redis:Databases").GetChildren().Select(x => x.Key).ToArray();
            for (int i = 0; i < nodeNames.Length; i++)
            {
                this.nodes.Add(nodeNames[i], connection.GetDatabase(i + 1));
            }
        }

        public async Task<Dictionary<string, TimeSpan>> TestConnectionsAsync()
        {
            var result = new Dictionary<string, TimeSpan>();

            foreach (var kvp in nodes)
            {
                var db = kvp.Value;
                var ping = await db.PingAsync();
                result[kvp.Key] = ping;
            }

            return result;
        }

        public async Task<string> CreateAsync(string dbName, string table, TeamModel model)
        {
            char idStart = '.';
            char hzNode = dbName[3];
            if (hzNode == '1')
            {
                idStart = 'A';
            }
            else
            {
                idStart = 'B';
            }


            string key = idStart + Guid.NewGuid().ToString("N")[..8];

            key = table + ":" + key;

            var db = nodes[dbName];


            var entries = new HashEntry[]
            {
                new HashEntry("country", model.Country),
                new HashEntry("name", model.Name),
                new HashEntry("city", model.City),
                new HashEntry("value", model.Value),
                new HashEntry("created_at", model.Created_at)
            };

            await db.HashSetAsync(key, entries);

            bool exists = await db.KeyExistsAsync(key);
            // OPTIONAL: throw if not there
            if (!exists)
                throw new InvalidOperationException($"Write failed: key '{key}' not found in {db.Database}.");

            // CUT VERSION

            var entries2 = new HashEntry[]
            {
                new HashEntry("country", model.Country),
                new HashEntry("name", model.Name),
            };


            
            string dbName2 = "DB1" + hzNode;
            db = nodes[dbName2];
            await db.HashSetAsync(key, entries2);
            

            dbName2 = "DB3" + hzNode;
            db = nodes[dbName2];
            await db.HashSetAsync(key, entries2);
            
            return key;

        }

        public async Task<bool> TeamNameExistsAsync(string dbName, string table, string name)
        {
            if (!nodes.TryGetValue(dbName, out var db))
                throw new ArgumentException($"Database '{dbName}' is not configured.");

            if (string.IsNullOrWhiteSpace(table))
                throw new ArgumentException(nameof(table));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException(nameof(name));

            var targetDb = db.Database;                 // logical DB index to scan
            var pattern = $"{table}:*";                // e.g. "Team:*"
            var normName = name.Trim();

            foreach (var ep in _mux.GetEndPoints())
            {
                var server = _mux.GetServer(ep);
                if (!server.IsConnected) continue;
                if (server.IsReplica) continue;     // scan masters only

                // Iterate only keys in the target logical DB
                foreach (var key in server.Keys(
                             database: targetDb,
                             pattern: pattern,
                             pageSize: 1000,            // tune as needed
                             flags: CommandFlags.DemandMaster))
                {
                    var existingName = await db.HashGetAsync(key, "name").ConfigureAwait(false);
                    if (existingName.HasValue &&
                        existingName.ToString().Equals(normName, StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        public async Task<bool> DeleteRecordAsync(string dbName, string table, string id)
        {
            if (string.IsNullOrWhiteSpace(dbName)) throw new ArgumentException(nameof(dbName));
            if (string.IsNullOrWhiteSpace(table)) throw new ArgumentException(nameof(table));
            if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException(nameof(id));

            if (!nodes.TryGetValue(dbName, out var db))
                throw new ArgumentException($"Database '{dbName}' is not configured.");

            var key = $"{table}:{id}";
            // DEL returns true if a key was removed
            var removed = await db.KeyDeleteAsync(key).ConfigureAwait(false);

            if (removed)
            {
                string newDB = "DB1" + dbName[3];
                var db2 = nodes[newDB];
                await db2.KeyDeleteAsync(key).ConfigureAwait(false);

                newDB = "DB3" + dbName[3];
                db2 = nodes[newDB];
                await db2.KeyDeleteAsync(key).ConfigureAwait(false);
            }

            return removed;
        }


        public async Task<object?> GetAsync(string dbName, string table, string id)
        {
            if (!nodes.ContainsKey(dbName))
                throw new ArgumentException($"Database '{dbName}' is not configured.");

            var db = nodes[dbName];

            var value = await db.HashGetAllAsync(table + ":" + id);
            if (value.Length == 0)
            {
                return null;
            }

            var dict = value.ToDictionary(e => (string)e.Name, e => (string)e.Value);

            return dict;
        }

        public async Task<List<Dictionary<string, string>>> GetListAsync(string dbName, string table)
        {
            if (!nodes.TryGetValue(dbName, out var db))
                throw new ArgumentException($"Database '{dbName}' is not configured.");

            var result = new List<Dictionary<string, string>>();
            var pattern = $"{table}:*";
            var databaseIndex = db.Database; // <- use the selected logical DB

            // Iterate all endpoints (needed for cluster/sharded setups)
            foreach (var endpoint in _mux.GetEndPoints())
            {
                var server = _mux.GetServer(endpoint);
                if (!server.IsConnected) continue;

                // IMPORTANT: specify database
                foreach (var key in server.Keys(database: databaseIndex, pattern: pattern))
                {
                    var entries = await db.HashGetAllAsync(key);
                    if (entries.Length == 0) continue;

                    var dict = entries.ToDictionary(e => (string)e.Name, e => (string)e.Value);

                    // optional: include id parsed from key "table:id"
                    var parts = key.ToString().Split(':', 2);
                    if (parts.Length == 2) dict["id"] = parts[1];

                    result.Add(dict);
                }
            }

            return result;
        }

        public async Task<bool> UpdateAsync(string dbName, string table, string id, TeamModel model)
{
    if (!nodes.TryGetValue(dbName, out var db))
        throw new ArgumentException($"Database '{dbName}' is not configured.", nameof(dbName));
    if (string.IsNullOrWhiteSpace(table)) throw new ArgumentException(nameof(table));
    if (string.IsNullOrWhiteSpace(id))    throw new ArgumentException(nameof(id));
    if (model is null)                    throw new ArgumentNullException(nameof(model));

    string sourceKey = $"{table}:{id}";

    // 1) Team must exist in current DB
    if (!await db.KeyExistsAsync(sourceKey).ConfigureAwait(false))
        return false;

    // 2) Read current fields we need for logic
    var currentCountryVal = await db.HashGetAsync(sourceKey, "country").ConfigureAwait(false);
    var currentNameVal    = await db.HashGetAsync(sourceKey, "name").ConfigureAwait(false);

    string? currentCountry = currentCountryVal.HasValue ? currentCountryVal.ToString() : null;
    string? currentName    = currentNameVal.HasValue    ? currentNameVal.ToString()    : null;

            // 3) Compute current vs target HZ (fallback to dbName if missing)
            int currentHz = HzFromCountry(currentCountry!);

    int targetHz = HzFromCountry(model.Country ?? string.Empty);

    // Extract vNode: "DB" + vNode + hz  => e.g., DB2A -> vNode='2'
    char vNode = dbName[2];

    // 4) If name is changing, run duplicate check in the node that will end up holding the record
    bool nameChanged = !string.Equals(currentName, model.Name, StringComparison.OrdinalIgnoreCase);
    if (nameChanged)
    {
        string dupCheckDbName = (currentHz == targetHz) ? dbName : $"DB{vNode}{targetHz}";
        bool dup = await TeamNameExistsAsync(dupCheckDbName, table, model.Name).ConfigureAwait(false);
        if (dup)
            throw new InvalidOperationException($"A team with the name '{model.Name}' already exists.");
    }

    // 5) Prepare payloads
    var entries = new HashEntry[]
    {
        new HashEntry("country",    model.Country ?? string.Empty),
        new HashEntry("name",       model.Name ?? string.Empty),
        new HashEntry("city",       model.City ?? string.Empty),
        new HashEntry("value",      model.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        new HashEntry("created_at", model.Created_at.ToString())
    };

    var mirrorEntries = new HashEntry[]
    {
        new HashEntry("country", model.Country ?? string.Empty),
        new HashEntry("name",    model.Name ?? string.Empty)
    };

    // 6) Same-node update → in-place + mirrors
    if (currentHz == targetHz)
    {
        await db.HashSetAsync(sourceKey, entries).ConfigureAwait(false);

        string mirror1 = $"DB1{currentHz}";
        if (!nodes.TryGetValue(mirror1, out var dbMirror1))
            throw new InvalidOperationException($"Mirror database '{mirror1}' is not configured.");
        await dbMirror1.HashSetAsync(sourceKey, mirrorEntries).ConfigureAwait(false);

        string mirror3 = $"DB3{currentHz}";
        if (!nodes.TryGetValue(mirror3, out var dbMirror3))
            throw new InvalidOperationException($"Mirror database '{mirror3}' is not configured.");
        await dbMirror3.HashSetAsync(sourceKey, mirrorEntries).ConfigureAwait(false);

        return true;
    }

    // 7) Cross-node move → flip first letter of ID (A<->B), write to target, mirror, TTL, then delete old
    string newId    = FlipFirstLetterAandB(id);
    string targetKey = $"{table}:{newId}";

    string targetDbName = $"DB{vNode}{targetHz}";
    if (!nodes.TryGetValue(targetDbName, out var targetDb))
        throw new InvalidOperationException($"Target database '{targetDbName}' is not configured.");

    // Preserve TTL
    var ttl = await db.KeyTimeToLiveAsync(sourceKey).ConfigureAwait(false);

    // Write to target primary
    await targetDb.HashSetAsync(targetKey, entries).ConfigureAwait(false);
    if (ttl.HasValue)
        await targetDb.KeyExpireAsync(targetKey, ttl.Value).ConfigureAwait(false);

    // Write to target mirrors
    string targetMirror1 = $"DB1{targetHz}";
    if (!nodes.TryGetValue(targetMirror1, out var dbTargetMirror1))
        throw new InvalidOperationException($"Mirror database '{targetMirror1}' is not configured.");
    await dbTargetMirror1.HashSetAsync(targetKey, mirrorEntries).ConfigureAwait(false);

    string targetMirror3 = $"DB3{targetHz}";
    if (!nodes.TryGetValue(targetMirror3, out var dbTargetMirror3))
        throw new InvalidOperationException($"Mirror database '{targetMirror3}' is not configured.");
    await dbTargetMirror3.HashSetAsync(targetKey, mirrorEntries).ConfigureAwait(false);

    // Delete from source primary and its mirrors (use ORIGINAL key!)
    await db.KeyDeleteAsync(sourceKey).ConfigureAwait(false);

    string sourceMirror1 = $"DB1{currentHz}";
    if (nodes.TryGetValue(sourceMirror1, out var dbSourceMirror1))
        await dbSourceMirror1.KeyDeleteAsync(sourceKey).ConfigureAwait(false);

    string sourceMirror3 = $"DB3{currentHz}";
    if (nodes.TryGetValue(sourceMirror3, out var dbSourceMirror3))
        await dbSourceMirror3.KeyDeleteAsync(sourceKey).ConfigureAwait(false);

    return true;
}

/// <summary>
/// A–M -> 'A', N–Z -> 'B' (uses the first letter of the country; non-letters default to 'A').
/// </summary>
private static int HzFromCountry(string country)
{
    if (string.IsNullOrWhiteSpace(country)) return 'A';
    char c = char.ToUpperInvariant(country.Trim()[0]);
    if (c < 'A' || c > 'Z') return 'A';
    return (c <= 'M') ? 1 : 2;
}

/// <summary>
/// Flip first character of ID between a<->b and A<->B. Leave unchanged otherwise.
/// </summary>
private static string FlipFirstLetterAandB(string id)
{
    if (string.IsNullOrEmpty(id)) return id;
    char first = id[0];
    char flipped = first switch
    {
        'a' => 'b',
        'b' => 'a',
        'A' => 'B',
        'B' => 'A',
        _   => first
    };
    return (flipped == first) ? id : flipped + id.Substring(1);
}



    }
}
