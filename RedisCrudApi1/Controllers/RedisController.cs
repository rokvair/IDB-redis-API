using Microsoft.AspNetCore.Mvc;
using RedisCrudApi.Models;
using RedisCrudApi.Services;

namespace RedisCrudApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class RedisController : ControllerBase
    {
        private readonly RedisService _redis;

        public RedisController(RedisService redis)
        {
            _redis = redis;
        }

        // Generic Create or Upsert
        [HttpPost("create")]
        public async Task<IActionResult> Create([FromBody] EntityBase entity)
        {
            if (entity == null || string.IsNullOrEmpty(entity.Type) || string.IsNullOrEmpty(entity.Id))
                return BadRequest("Entity must include Type and Id.");

            await _redis.CreateOrUpdateEntityAsync(entity);
            return Ok(new { status = "created", key = $"{entity.Type}:{entity.Id}" });
        }

        // Read
        [HttpGet("read/{type}/{id}")]
        public async Task<IActionResult> Read(string type, string id)
        {
            var data = await _redis.ReadEntityAsync(type, id);
            if (data == null) return NotFound();
            return Ok(data);
        }

        // Update (partial)
        [HttpPut("update/{type}/{id}")]
        public async Task<IActionResult> Update(string type, string id, [FromBody] Dictionary<string, string> updates)
        {
            if (updates == null || updates.Count == 0) return BadRequest("No updates provided.");
            var ok = await _redis.UpdateEntityAsync(type, id, updates);
            if (!ok) return NotFound();
            return Ok(new { status = "updated" });
        }

        // Delete
        [HttpDelete("delete/{type}/{id}")]
        public async Task<IActionResult> Delete(string type, string id)
        {
            var ok = await _redis.DeleteEntityAsync(type, id);
            if (!ok) return NotFound();
            return Ok(new { status = "deleted" });
        }
    }
}
