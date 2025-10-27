using Microsoft.AspNetCore.Mvc;
using RedisCrudApi.Models;
using RedisCrudApi.Services;

namespace RedisCrudApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class EntitiesController : ControllerBase
    {
        private readonly RedisService _redis;

        public EntitiesController(RedisService redis)
        {
            _redis = redis;
        }

        // GET api/entities/{type}/{id}
        [HttpGet("{type}/{id}")]
        public async Task<IActionResult> GetEntity(string type, string id)
        {
            var result = await _redis.ReadEntityAsync(type, id);
            if (result == null)
                return NotFound();
            return Ok(result);
        }

        // POST api/entities
        [HttpPost]
        public async Task<IActionResult> CreateEntity([FromBody] EntityBase entity)
        {
            await _redis.CreateOrUpdateEntityAsync(entity);
            return CreatedAtAction(nameof(GetEntity),
                new { type = entity.Type, id = entity.Id },
                entity);
        }

        // PUT api/entities/{type}/{id}
        [HttpPut("{type}/{id}")]
        public async Task<IActionResult> UpdateEntity(string type, string id, [FromBody] Dictionary<string, string> updates)
        {
            var ok = await _redis.UpdateEntityAsync(type, id, updates);
            if (!ok)
                return NotFound();
            return Ok();
        }

        // DELETE api/entities/{type}/{id}
        [HttpDelete("{type}/{id}")]
        public async Task<IActionResult> DeleteEntity(string type, string id)
        {
            var ok = await _redis.DeleteEntityAsync(type, id);
            if (!ok)
                return NotFound();
            return NoContent();
        }
    }
}
