using Microsoft.AspNetCore.Mvc;
using RedisCrudApi.Models;
using RedisCrudApi.Services;

namespace RedisCrudApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class EntitiesController : ControllerBase
    {
        private readonly TeamService _redis;

        public EntitiesController(TeamService redis)
        {
            _redis = redis;
        }

        [HttpGet("test")]
        public async Task<IActionResult> Test()
        {
            var result = await _redis.TestConnectionsAsync();
            return Ok(result);
        }

        [HttpGet("{table}")]
        public async Task<IActionResult> GetListAsync([FromRoute] string table, [FromQuery] string db = "DB21")
        {

            if (!db.StartsWith("DB", StringComparison.OrdinalIgnoreCase))
                db = "DB" + db;

            var teams = await _redis.GetListAsync(db, table);

            if (teams.Count == 0)
                return NotFound($"No records found in table '{table}'");

            return Ok(teams);
        }


        [HttpPost("create/{table}")]
        public async Task<IActionResult> CreateAsync(string table, [FromBody] TeamCreateModel model)
        {



            string vNode = ComputeVNode(table);


            string hzNode = ComputeHzNode(model.Country);

            string dbName = "DB" + vNode + hzNode;

            bool exists = await _redis.TeamNameExistsAsync(dbName, table, model.Name);
            if (exists)
                return Conflict($"A team with the name '{model.Name}' already exists.");

            var id = await _redis.CreateAsync(dbName, table, model);
            return Ok(new { id, table, dbName });
        }

        [HttpDelete("{table}/{id}")]
        public async Task<IActionResult> DeleteAsync(string table, string id)
        {
            if (string.IsNullOrWhiteSpace(table)) return BadRequest("table is required.");
            if (string.IsNullOrWhiteSpace(id)) return BadRequest("id is required.");

            // Match your existing sharding logic
            string hzNode = ComputeHzNodeFromID(id);
            string vNode = ComputeVNode(table);
            string dbName = "DB" + vNode + hzNode;

            var deleted = await _redis.DeleteRecordAsync(dbName, table, id);
            if (!deleted) return NotFound(); // key didn’t exist

            return NoContent();
        }

        [HttpGet("list/{table}/{id}")]
        public async Task<IActionResult> GetAsync(string table, string id)
        {

            string hzNode = ComputeHzNodeFromID(id);

            string vNode = ComputeVNode(table);

            string dbName = "DB" + vNode + hzNode;
            var result = await _redis.GetAsync(dbName, table, id);
            if (result is null)
            {
                return NotFound();
            }

            return Ok(result);

        }


        [HttpPut("{table}/{id}")]
public async Task<IActionResult> UpdateAsync(string table, string id, [FromBody] TeamCreateModel body)
{
    if (string.IsNullOrWhiteSpace(table)) return BadRequest("table is required.");
    if (string.IsNullOrWhiteSpace(id))    return BadRequest("id is required.");
    if (!ModelState.IsValid)              return ValidationProblem(ModelState);

    // Match your sharding logic
    string hzNode = ComputeHzNodeFromID(id);
    string vNode  = ComputeVNode(table);
    string dbName = "DB" + vNode + hzNode;

    try
    {
        var updated = await _redis.UpdateTeamAsync(dbName, table, id, body);
        if (!updated) return NotFound($"Team with id '{id}' does not exist.");
        return NoContent();
    }
    catch (InvalidOperationException ex)
    {
        // duplicate name
        return Conflict(ex.Message);
    }
}

        private static string ComputeVNode(string table) => table switch
        {
            "Player" => "2",
            "Team" => "2",
            "Coach" => "2",
            "Championship_team" => "1",
            "Championship" => "1",
            "Sponsor" => "3",
            "Player_sponsor" => "3",
            "Team_sponsor" => "3",
            "Championship_sponsor" => "3",
            _ => throw new ArgumentException($"Unknown table '{table}'")
        };

        private static string ComputeHzNode(string key)
        {
            var first = char.ToUpperInvariant(key.Trim()[0]);
            return first >= 'A' && first <= 'M' ? "1" : "2";
        }

        private static string ComputeHzNodeFromID(string key)
        {
            return key[0] switch { 'A' => "1", 'B' => "2", _ => throw new ArgumentException("id must start with A or B") };
        }


        

    }
}
