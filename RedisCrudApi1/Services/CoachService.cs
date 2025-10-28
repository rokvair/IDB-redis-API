using Microsoft.Extensions.Configuration;
using Microsoft.VisualBasic;
using Models;
using StackExchange.Redis;
using System.Data.Common;
using System.Windows.Markup;

namespace RedisCrudApi.Services
{
    public class CoachService
    {
        private readonly IConnectionMultiplexer _mux;
        private readonly IConfiguration _config;

        private readonly Dictionary<string, IDatabase> nodes = new Dictionary<string, IDatabase>();


        public CoachService(IConnectionMultiplexer mux, IConfiguration config)
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

        public async Task<string> CreateAsync(string dbName, string table, CoachModel model)
        {

            if (!await TeamIdExistsAsync(dbName, "Team", model.FK_Team_Id))
            {
                throw new InvalidOperationException("The team with given ID does not exist.");
            }

            char hzNode = dbName[3]; // '1' or '2'
            char idStart = (hzNode == '1') ? 'A' : 'B';


            string key = idStart + Guid.NewGuid().ToString("N")[..8];

            key = table + ":" + key;

            var db = nodes[dbName];


            var entries = new HashEntry[]
            {
                new HashEntry("first_name",  model.First_name  ?? string.Empty),
                new HashEntry("last_name",   model.Last_name   ?? string.Empty),
                new HashEntry("nationality", model.Nationality ?? string.Empty),
                new HashEntry("experience",  model.Experience.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                new HashEntry("fk_team_id",  model.FK_Team_Id.ToString())
            };

            await db.HashSetAsync(key, entries);

            bool exists = await db.KeyExistsAsync(key);
            // OPTIONAL: throw if not there
            if (!exists)
                throw new InvalidOperationException($"Write failed: key '{key}' not found in {db.Database}.");


            return key;

        }

        public async Task<bool> TeamIdExistsAsync(string dbName, string table, string id)
        {
            if (!nodes.TryGetValue(dbName, out var db))
                throw new ArgumentException($"Database '{dbName}' is not configured.");

            if (string.IsNullOrWhiteSpace(table))
                throw new ArgumentException(nameof(table));

            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException(nameof(id));

            var key = $"{table}:{id}";  // e.g. Team:1234
            return await db.KeyExistsAsync(key).ConfigureAwait(false);
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

        public async Task<bool> UpdateAsync(string dbName, string table, string id, CoachModel model)
{
    if (!nodes.TryGetValue(dbName, out var db))
        throw new ArgumentException($"Database '{dbName}' is not configured.", nameof(dbName));
    if (string.IsNullOrWhiteSpace(table)) throw new ArgumentException(nameof(table));
    if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException(nameof(id));
    if (model is null) throw new ArgumentNullException(nameof(model));

    // "table:id"
    string sourceKey = $"{table}:{id}";

    // 1) Record must exist in current DB
    if (!await db.KeyExistsAsync(sourceKey).ConfigureAwait(false))
        return false;

    // 2) Build updated fields
    var entries = new HashEntry[]
    {
        new HashEntry("first_name",  model.First_name  ?? string.Empty),
        new HashEntry("last_name",   model.Last_name   ?? string.Empty),
        new HashEntry("nationality", model.Nationality ?? string.Empty),
        new HashEntry("experience",  model.Experience.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        new HashEntry("fk_team_id",  model.FK_Team_Id.ToString())
    };

    // 3) Determine current and target hz-nodes based on TEAM ids (existing vs new)
    var existingFkVal = await db.HashGetAsync(sourceKey, "fk_team_id").ConfigureAwait(false);
    var existingFk = existingFkVal.HasValue ? existingFkVal.ToString() : string.Empty;

    char currentHzFromTeam = string.IsNullOrEmpty(existingFk) ? dbName[3] : HzFromTeamId(existingFk);
    char newHzFromTeam     = HzFromTeamId(model.FK_Team_Id);

    // Extract vNode from dbName: "DB" + vNode + hz  (indexes: [0]='D',[1]='B',[2]=vNode,[3]=hz)
    char vNode = dbName[2];

    // 4) If hz changes -> move record to target DB and flip only the first character of the ID
    if (currentHzFromTeam != newHzFromTeam)
    {
        // Compute new ID by flipping first letter a<->b (case-aware). If it's neither a/A nor b/B, leave as-is.
        string newId = FlipFirstLetterAandB(id);
        string targetKey = $"{table}:{newId}";

        string targetDbName = $"DB{vNode}{newHzFromTeam}";
        if (!nodes.TryGetValue(targetDbName, out var targetDb))
            throw new InvalidOperationException($"Target database '{targetDbName}' is not configured.");

        // Preserve TTL if present
        var ttl = await db.KeyTimeToLiveAsync(sourceKey).ConfigureAwait(false);

        // Write to target (new key)
        await targetDb.HashSetAsync(targetKey, entries).ConfigureAwait(false);
        if (ttl.HasValue)
            await targetDb.KeyExpireAsync(targetKey, ttl.Value).ConfigureAwait(false);

        // Delete from source (old key)
        await db.KeyDeleteAsync(sourceKey).ConfigureAwait(false);

        return true;
    }
    else
    {
        // 5) Hz unchanged -> in-place update on current DB using the original key
        await db.HashSetAsync(sourceKey, entries).ConfigureAwait(false);
        return true;
    }
}

/// <summary>
/// Flips the first character of the id between a<->b, A<->B. Leaves id unchanged if first char is neither a/A nor b/B.
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

    if (flipped == first) return id; // no flip needed
    return flipped + id.Substring(1);
}

private static char HzFromTeamId(string teamId)
{
    if (string.IsNullOrWhiteSpace(teamId))
        throw new ArgumentException(nameof(teamId));

    // Your convention: A => hz '1', B => hz '2'
    return teamId[0] switch
    {
        'A' => '1',
        'B' => '2',
        _   => throw new InvalidOperationException($"Unknown team id prefix '{teamId[0]}'. Expected 'A' or 'B'.")
    };
}



    }
}
