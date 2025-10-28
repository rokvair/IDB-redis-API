using Microsoft.OpenApi.Models;
using StackExchange.Redis;
using RedisCrudApi.Services;

var builder = WebApplication.CreateBuilder(args);

// Add configuration
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new OpenApiInfo { Title = "RedisCrudApi", Version = "v1" });
});

// Register Redis multiplexer as singleton
var redisHost = builder.Configuration["Redis:Host"] ?? "localhost";
var redisPort = builder.Configuration["Redis:Port"] ?? "6379";

var connectionString = $"{redisHost}:{redisPort}";
builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
    ConnectionMultiplexer.Connect(connectionString)
);


builder.Services.AddSingleton<IDictionary<string, IDatabase>>(sp =>
{
    var mux = sp.GetRequiredService<IConnectionMultiplexer>();

    // map your logical DB names to actual databases
    // (examples—adjust to your real mapping)
    return new Dictionary<string, IDatabase>
    {
        ["DB11"] = mux.GetDatabase(11),
        ["DB12"] = mux.GetDatabase(12),
        ["DB21"] = mux.GetDatabase(21),
        ["DB22"] = mux.GetDatabase(22),
        ["DB31"] = mux.GetDatabase(31),
        // ...
    };
});

// Register TeamService
builder.Services.AddScoped<TeamService>();
builder.Services.AddScoped<PlayerService>();
builder.Services.AddScoped<CoachService>();

var app = builder.Build();
using (var scope = app.Services.CreateScope())
{
    var redis = scope.ServiceProvider.GetRequiredService<TeamService>();
    var result = await redis.TestConnectionsAsync();

if (result.Count > 0)
{
    Console.WriteLine("✅ Connected to Redis!");
    foreach (var db in result)
    {
        Console.WriteLine($" - {db.Key}: {db.Value}");
    }
}
else
{
    Console.WriteLine("❌ Failed to connect to any Redis database");
}
}
app.UseSwagger();
app.UseSwaggerUI();
app.UseRouting();
app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

app.Run();
