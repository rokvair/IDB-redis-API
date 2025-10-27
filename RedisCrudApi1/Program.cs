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


// Register TeamService
builder.Services.AddScoped<TeamService>();

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
