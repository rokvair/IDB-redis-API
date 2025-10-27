namespace RedisCrudApi.Models
{
    // Generic flexible entity used by API
    public class EntityBase
    {
        // id as string to accept numeric or GUID; keep consistent in client
        public string Id { get; set; } = "";
        // Type must match keys in appsettings "Databases" (e.g. Team, Player, Coach, Sponsor, Championship)
        public string Type { get; set; } = "";
        // Flexible map of fields; all values are strings for simplicity
        public Dictionary<string, string> Fields { get; set; } = new Dictionary<string, string>();
    }
}
