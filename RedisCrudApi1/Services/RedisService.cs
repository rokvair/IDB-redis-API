using Microsoft.Extensions.Configuration;
using RedisCrudApi.Models;
using StackExchange.Redis;
using System.Data.Common;

namespace RedisCrudApi.Services
{
    public class RedisService
    {
        private readonly IConnectionMultiplexer _mux;
        private readonly IConfiguration _config;

        public RedisService(IConnectionMultiplexer mux, IConfiguration config)
        {
            _mux = mux;
            _config = config;
        }

        // Read DB index for an entity type; default 0 if missing
        private int GetDbIndex(string entityType)
        {
            var index = _config[$"Redis:Databases:{entityType}"];
            return int.TryParse(index, out var i) ? i : 0;
        }

        private IDatabase GetDb(string entityType) =>
            _mux.GetDatabase(GetDbIndex(entityType));

        private string Key(string type, string id) => $"{type}:{id}";

        // Create or Update hash for entity
        public async Task CreateOrUpdateEntityAsync(EntityBase entity)
        {
            if (string.IsNullOrWhiteSpace(entity.Type) || string.IsNullOrWhiteSpace(entity.Id))
                throw new ArgumentException("Entity Type and Id are required.");

            var db = GetDb(entity.Type);
            var entries = entity.Fields.Select(kv => new HashEntry(kv.Key, kv.Value)).ToArray();
            await db.HashSetAsync(Key(entity.Type, entity.Id), entries);

            // sync relationships (create side effects)
            await SyncAfterCreateOrUpdateAsync(entity);
        }

        public async Task<Dictionary<string, string>?> ReadEntityAsync(string type, string id)
        {
            var db = GetDb(type);
            var entries = await db.HashGetAllAsync(Key(type, id));
            if (entries.Length == 0) return null;
            return entries.ToDictionary(e => e.Name.ToString(), e => e.Value.ToString());
        }

        public async Task<bool> DeleteEntityAsync(string type, string id)
        {
            // When deleting, also remove reverse-set links
            var db = GetDb(type);
            var key = Key(type, id);
            if (!await db.KeyExistsAsync(key)) return false;

            // read fields that might contain foreign keys to clean up
            var fields = await db.HashGetAllAsync(key);
            var dict = fields.ToDictionary(x => x.Name.ToString(), x => x.Value.ToString());

            // Remove reverse links across other DBs & sets
            await CleanupRelationshipsOnDeleteAsync(type, id, dict);

            // delete the hash itself
            await db.KeyDeleteAsync(key);
            return true;
        }

        // Basic update wrapper
        public async Task<bool> UpdateEntityAsync(string type, string id, Dictionary<string, string> updates)
        {
            var db = GetDb(type);
            var key = Key(type, id);
            if (!await db.KeyExistsAsync(key)) return false;

            var entries = updates.Select(kv => new HashEntry(kv.Key, kv.Value)).ToArray();
            await db.HashSetAsync(key, entries);

            // sync any changed relationships
            var entity = new EntityBase { Type = type, Id = id, Fields = updates };
            await SyncAfterCreateOrUpdateAsync(entity);

            return true;
        }

        // Relationship sync rules (kept generic and data-driven)
        // This function handles the following relationships by convention:
        // - If Entity has "fk_team_id" -> add to Team:<teamId>:<EntityType>s set (e.g. Team:100:Players)
        // - If Entity has "fk_sponsor_id" -> add to Sponsor:<sponsorId>:<EntityType>s
        // - If Entity has "fk_championship_id" -> add to Championship:<championId>:<EntityType>s
        // Also maintains reciprocal sets for the Team (Team:<id>:Players/Coaches/Sponsors/Championships)
        private async Task SyncAfterCreateOrUpdateAsync(EntityBase entity)
        {
            // Only string operations and sets are required
            if (entity.Fields == null || entity.Fields.Count == 0) return;

            // fk_team_id
            if (entity.Fields.TryGetValue("fk_team_id", out var teamId) && !string.IsNullOrEmpty(teamId))
            {
                // Add to Team db (read mapping from config)
                var teamDb = GetDb("Team");
                var relationKey = $"Team:{teamId}:{entity.Type}s";
                await teamDb.SetAddAsync(relationKey, entity.Id);
            }

            // fk_sponsor_id
            if (entity.Fields.TryGetValue("fk_sponsor_id", out var sponsorId) && !string.IsNullOrEmpty(sponsorId))
            {
                var sponsorDb = GetDb("Sponsor");
                var relationKey = $"Sponsor:{sponsorId}:{entity.Type}s";
                await sponsorDb.SetAddAsync(relationKey, entity.Id);

                // also keep Team-level link if both exist
                if (entity.Fields.TryGetValue("fk_team_id", out var teamId2) && !string.IsNullOrEmpty(teamId2))
                {
                    var teamDb2 = GetDb("Team");
                    await teamDb2.SetAddAsync($"Team:{teamId2}:Sponsors", sponsorId);
                    // Also add reverse set on Sponsor for team membership
                    await sponsorDb.SetAddAsync($"Sponsor:{sponsorId}:Teams", teamId2);
                }
            }

            // fk_championship_id
            if (entity.Fields.TryGetValue("fk_championship_id", out var champId) && !string.IsNullOrEmpty(champId))
            {
                var champDb = GetDb("Championship");
                var relationKey = $"Championship:{champId}:{entity.Type}s";
                await champDb.SetAddAsync(relationKey, entity.Id);

                // Also add Team:<teamId>:Championships and Championship:<id>:Teams if fk_team_id is present
                if (entity.Fields.TryGetValue("fk_team_id", out var teamId3) && !string.IsNullOrEmpty(teamId3))
                {
                    var teamDb3 = GetDb("Team");
                    await teamDb3.SetAddAsync($"Team:{teamId3}:Championships", champId);
                    await champDb.SetAddAsync($"Championship:{champId}:Teams", teamId3);
                }

                // Sponsor-championship link (if sponsor exists)
                if (entity.Fields.TryGetValue("fk_sponsor_id", out var sponsorId2) && !string.IsNullOrEmpty(sponsorId2))
                {
                    var sponsorDb2 = GetDb("Sponsor");
                    await sponsorDb2.SetAddAsync($"Sponsor:{sponsorId2}:Championships", champId);
                    await champDb.SetAddAsync($"Championship:{champId}:Sponsors", sponsorId2);
                }
            }
        }

        private async Task CleanupRelationshipsOnDeleteAsync(string type, string id, Dictionary<string, string> fields)
        {
            // If the deleted entity had fk_team_id (it was child), remove it from Team:set
            if (fields.TryGetValue("fk_team_id", out var teamId) && !string.IsNullOrEmpty(teamId))
            {
                var teamDb = GetDb("Team");
                await teamDb.SetRemoveAsync($"Team:{teamId}:{type}s", id);
            }

            // If the deleted entity had fk_sponsor_id, remove it from Sponsor:set
            if (fields.TryGetValue("fk_sponsor_id", out var sponsorId) && !string.IsNullOrEmpty(sponsorId))
            {
                var sponsorDb = GetDb("Sponsor");
                await sponsorDb.SetRemoveAsync($"Sponsor:{sponsorId}:{type}s", id);
            }

            // If deletion is Team, remove references to team across Players/Coaches/Sponsors/Championships
            if (type == "Team")
            {
                var teamDb = GetDb("Team");
                // read members from sets and remove corresponding fk fields if desired (here we only remove set entries)
                var sets = new[] { "Players", "Coaches", "Sponsors", "Championships" };
                foreach (var set in sets)
                {
                    var members = await teamDb.SetMembersAsync($"Team:{id}:{set}");
                    if (members?.Length > 0)
                    {
                        foreach (var memberId in members)
                        {
                            // Attempt to remove reverse link (only if we can find which DB stores that entity)
                            // Determine entity type from set name: Players -> Player, Coaches -> Coach
                            var entityType = set.TrimEnd('s'); // crude but matches naming convention
                            try
                            {
                                var memberDb = GetDb(entityType);
                                // remove fk_team_id in the member (optional): here we simply remove membership set entry
                                // Optionally: update member hash to remove fk_team_id
                                await memberDb.HashDeleteAsync($"{entityType}:{memberId}", "fk_team_id");
                            }
                            catch
                            {
                                // ignore if no mapping
                            }
                        }
                    }
                    await teamDb.KeyDeleteAsync($"Team:{id}:{set}");
                }
            }

            // If deletion is Sponsor: remove sponsor from any Team:<id>:Sponsors sets
            if (type == "Sponsor")
            {
                var sponsorDb = GetDb("Sponsor");
                var teams = await sponsorDb.SetMembersAsync($"Sponsor:{id}:Teams");
                if (teams?.Length > 0)
                {
                    var teamDb = GetDb("Team");
                    foreach (var t in teams)
                        await teamDb.SetRemoveAsync($"Team:{t}:Sponsors", id);
                }
                await sponsorDb.KeyDeleteAsync($"Sponsor:{id}:Teams");
                await sponsorDb.KeyDeleteAsync($"Sponsor:{id}:Players");
                await sponsorDb.KeyDeleteAsync($"Sponsor:{id}:Championships");
            }

            // If deletion is Championship: clean up Championship sets on teams and sponsor
            if (type == "Championship")
            {
                var champDb = GetDb("Championship");
                var teams = await champDb.SetMembersAsync($"Championship:{id}:Teams");
                if (teams?.Length > 0)
                {
                    var teamDb = GetDb("Team");
                    foreach (var t in teams)
                        await teamDb.SetRemoveAsync($"Team:{t}:Championships", id);
                }
                await champDb.KeyDeleteAsync($"Championship:{id}:Teams");
                await champDb.KeyDeleteAsync($"Championship:{id}:Sponsors");
            }
        }
        /// <summary>
        /// Test connection to Redis server
        /// </summary>
        /// <returns></returns>
        public async Task<bool> TestConnectionAsync()
        {
            try
            {
                var pong = await _mux.GetDatabase().PingAsync();
                Console.WriteLine($"Redis responded: {pong}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Redis connection failed: {ex.Message}");
                return false;
            }
        }


    }
}
