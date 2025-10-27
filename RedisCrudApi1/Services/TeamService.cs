using Microsoft.Extensions.Configuration;
using Microsoft.VisualBasic;
using RedisCrudApi.Models;
using StackExchange.Redis;
using System.Data.Common;
using System.Windows.Markup;

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

        public async Task<string> CreateAsync(string dbName, string table, TeamCreateModel model)
        {
            string idStart = "";
            char hzNode = dbName[2];
            if (hzNode == 1)
            {
                idStart = "A";
            }
            else
            {
                idStart = "B";
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
            var type = await db.KeyTypeAsync(key);

            // OPTIONAL: throw if not there
            if (!exists)
                throw new InvalidOperationException($"Write failed: key '{key}' not found in {db.Database}.");

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

        public async Task<bool> UpdateTeamAsync(string dbName, string table, string id, TeamCreateModel model)
{
    if (!nodes.TryGetValue(dbName, out var db))
        throw new ArgumentException($"Database '{dbName}' is not configured.");
    if (string.IsNullOrWhiteSpace(table)) throw new ArgumentException(nameof(table));
    if (string.IsNullOrWhiteSpace(id))    throw new ArgumentException(nameof(id));
    if (model is null)                    throw new ArgumentNullException(nameof(model));

    var key = $"{table}:{id}";

    // 1) Team must exist
    if (!await db.KeyExistsAsync(key).ConfigureAwait(false))
        return false;

    // 2) Read current name; only run duplicate check if name actually changes
    var currentNameVal = await db.HashGetAsync(key, "name").ConfigureAwait(false);
    var currentName = currentNameVal.HasValue ? currentNameVal.ToString() : null;

    if (!string.Equals(currentName, model.Name, StringComparison.OrdinalIgnoreCase))
    {
        // Uses your scan-one-DB method (as you requested)
        var dup = await TeamNameExistsAsync(dbName, table, model.Name).ConfigureAwait(false);
        if (dup)
            throw new InvalidOperationException($"A team with the name '{model.Name}' already exists.");
    }

    // 3) Update all fields from the JSON body
    var entries = new HashEntry[]
    {
        new HashEntry("country",    model.Country ?? string.Empty),
        new HashEntry("name",       model.Name ?? string.Empty),
        new HashEntry("city",       model.City ?? string.Empty),
        new HashEntry("value",      model.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        new HashEntry("created_at", model.Created_at.ToString())
    };

    await db.HashSetAsync(key, entries).ConfigureAwait(false);
    return true;
}




    }
}
