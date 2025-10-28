using Microsoft.Extensions.Configuration;
using Microsoft.VisualBasic;
using Models;
using StackExchange.Redis;
using System.Data.Common;
using System.Windows.Markup;

namespace RedisCrudApi.Services
{
    public class PlayerService
    {
        private readonly IConnectionMultiplexer _mux;
        private readonly IConfiguration _config;

        private readonly Dictionary<string, IDatabase> nodes = new Dictionary<string, IDatabase>();


        public PlayerService(IConnectionMultiplexer mux, IConfiguration config)
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

        public async Task<string> CreateAsync(string dbName, string table, PlayerModel model)
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
                new HashEntry("first_name", model.First_name),
                new HashEntry("last_name", model.Last_name),
                new HashEntry("birthdate", model.Birthyear),
                new HashEntry("position", model.Position),
                new HashEntry("nationality", model.Nationality),
                new HashEntry("height", model.Height),
                new HashEntry("goals", model.Goals),
                new HashEntry("assists", model.Assists),
                new HashEntry("fk_team_id", model.FK_Team_Id)
            };

            await db.HashSetAsync(key, entries);

            bool exists = await db.KeyExistsAsync(key);
            // OPTIONAL: throw if not there
            if (!exists)
                throw new InvalidOperationException($"Write failed: key '{key}' not found in {db.Database}.");


            // CUT VERSION

            var entries2 = new HashEntry[]
            {
                new HashEntry("first_name", model.First_name),
                new HashEntry("last_name", model.Last_name)
            };

            string dbName2 = "DB3" + hzNode;
            db = nodes[dbName2];
            await db.HashSetAsync(key, entries2);

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
                string newDB = "DB3" + dbName[3];
                var db2 = nodes[newDB];
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

        public async Task<bool> UpdateAsync(string dbName, string table, string id, PlayerModel model)
        {
            var currentHzNode = dbName[3];
            var projectedHzNode = (model.FK_Team_Id[0] == 'A') ? '1' : '2';
            var currentDb = nodes[dbName];


            if (currentHzNode == projectedHzNode)
            {
                var result = await TeamExists(dbName, model.FK_Team_Id);
                if (result)
                {
                    //egizstuoja

                    var entries = new HashEntry[]
                    {
                        new HashEntry("first_name",  model.First_name  ?? string.Empty),
                        new HashEntry("last_name",   model.Last_name   ?? string.Empty),
                        new HashEntry("birthyear",   model.Birthyear.ToString()),
                        new HashEntry("position",    model.Position    ?? string.Empty),
                        new HashEntry("nationality", model.Nationality ?? string.Empty),
                        new HashEntry("height",      model.Height.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        new HashEntry("goals",       model.Goals.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        new HashEntry("assists",     model.Assists.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        new HashEntry("fk_team_id",  model.FK_Team_Id  ?? string.Empty)
                    };

                    var entries2 = new HashEntry[]
                    
                    {
                        new HashEntry("first_name",  model.First_name  ?? string.Empty),
                        new HashEntry("last_name",   model.Last_name   ?? string.Empty),
                    };


                    await currentDb.HashSetAsync(table + ":" + id, entries);

                    var mirrorDb = dbName.ToCharArray();
                    mirrorDb[2] = '3';
                    var mirrorDbStr = new string(mirrorDb);
                    await nodes[mirrorDbStr].HashSetAsync(table + ":" + id, entries2);

                    return true;
                }
                
            }
            else
            {

                //projectintam sukurt (jeigu yra komanda), esamam istrint

                char[] dbn = dbName.ToCharArray();
                dbn[3] = projectedHzNode;
                var projectedDbName = new string(dbn);
                var projectedDb = nodes[projectedDbName];

                var result = await TeamExists(projectedDbName, model.FK_Team_Id);
                if (result)
                {
                    //egizstuoja

                    var entries = new HashEntry[]
                    {
                        new HashEntry("first_name",  model.First_name  ?? string.Empty),
                        new HashEntry("last_name",   model.Last_name   ?? string.Empty),
                        new HashEntry("birthyear",   model.Birthyear.ToString()),
                        new HashEntry("position",    model.Position    ?? string.Empty),
                        new HashEntry("nationality", model.Nationality ?? string.Empty),
                        new HashEntry("height",      model.Height.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        new HashEntry("goals",       model.Goals.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        new HashEntry("assists",     model.Assists.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        new HashEntry("fk_team_id",  model.FK_Team_Id  ?? string.Empty)
                    };

                    var entries2 = new HashEntry[]

                    {
                        new HashEntry("first_name",  model.First_name  ?? string.Empty),
                        new HashEntry("last_name",   model.Last_name   ?? string.Empty),
                    };
                    
                    //istrint esamam db

                    await currentDb.KeyDeleteAsync(table+":"+id).ConfigureAwait(false);

                    char[] mirrorCurDb = dbName.ToCharArray();
                    mirrorCurDb[2] = '3';
                    var mirrorCurDbStr = new string(mirrorCurDb);
                    await nodes[mirrorCurDbStr].KeyDeleteAsync(table+":"+id).ConfigureAwait(false);


                    //sukurt naujam

                    string newID = (id[0] == 'A') ? "B" + id[1..] : "A" + id[1..];
                    id = newID;
                    

                    await projectedDb.HashSetAsync(table + ":" + id, entries);

                    var mirrorDb = projectedDbName.ToCharArray();
                    mirrorDb[2] = '3';
                    var mirrorDbStr = new string(mirrorDb);
                    await nodes[mirrorDbStr].HashSetAsync(table + ":" + id, entries2);


                    return true;
                }

            }
            throw new InvalidOperationException("NEGALIMA, KOMANDOS NERA");
            return false;
        }

        public async Task<bool> TeamExists(string dbName, string teamID)
        {
            var db = nodes[dbName];
            return await db.KeyExistsAsync("Team:"+teamID);

        }

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
                _ => first
            };

            return (flipped == first) ? id : flipped + id.Substring(1);
        }



    }
}
