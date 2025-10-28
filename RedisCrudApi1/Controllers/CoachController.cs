using Microsoft.AspNetCore.Mvc;
using Models;
using RedisCrudApi.Services;

namespace RedisCrudApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CoachController : ControllerBase
    {
        private readonly CoachService _redis;

        public CoachController(CoachService redis)
        {
            _redis = redis;
        }

        [HttpGet("test")]
        public async Task<IActionResult> Test()
        {
            var result = await _redis.TestConnectionsAsync();
            return Ok(result);
        }

        [HttpGet]
        public async Task<IActionResult> GetListAsync([FromQuery] string db = "DB21")
        {
            string table = "Coach";

            if (!db.StartsWith("DB", StringComparison.OrdinalIgnoreCase))
                db = "DB" + db;

            var coaches = await _redis.GetListAsync(db, table);

            if (coaches.Count == 0)
                return NotFound($"No records found in table '{table}'");

            return Ok(coaches);
        }


        [HttpPost("create")]
        public async Task<IActionResult> CreateAsync([FromBody] CoachModel model)
        {
            string table = "Coach";


            string vNode = ComputeVNode(table);


            string hzNode = ComputeHzNodeFromID(model.FK_Team_Id);
            string dbName = "DB" + vNode + hzNode;

            var id = await _redis.CreateAsync(dbName, table, model);
            return Ok(new { id, table, dbName });
        }

        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteAsync(string id)
        {
            string table = "Coach";
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

        [HttpGet("list/{id}")]
        public async Task<IActionResult> GetAsync(string id)
        {
            string table = "Coach";
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


        [HttpPut("{id}")]
public async Task<IActionResult> UpdateAsync(string id, [FromBody] CoachModel body)
        {
            string table = "Coach";
    if (string.IsNullOrWhiteSpace(table)) return BadRequest("table is required.");
    if (string.IsNullOrWhiteSpace(id))    return BadRequest("id is required.");
    if (!ModelState.IsValid)              return ValidationProblem(ModelState);

    // Match your sharding logic
    string hzNode = ComputeHzNodeFromID(id);
    string vNode  = ComputeVNode(table);
    string dbName = "DB" + vNode + hzNode;


    try
    {
        var updated = await _redis.UpdateAsync(dbName, table, id, body);
        if (!updated) return NotFound($"Coach with id '{id}' does not exist.");
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
            return (first >= 'A' && first <= 'M') ? "1" : "2";
        }

        private static string ComputeHzNodeFromID(string key)
        {
            return key[0] switch { 'A' => "1", 'B' => "2", _ => throw new ArgumentException("Team id must start with A or B") };
        }


        

    }
}
