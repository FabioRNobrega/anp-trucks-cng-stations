var builder = WebApplication.CreateBuilder(args);

// Add OpenAPI support
builder.Services.AddOpenApi();

var app = builder.Build();

// Configure the HTTP request pipeline
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

var startTime = DateTime.UtcNow;

app.MapGet("/health", () =>
{
    var uptime = DateTime.UtcNow - startTime;

    var healthInfo = new
    {
        status = "Healthy",
        environment = app.Environment.EnvironmentName,
        uptime = $"{uptime.Days}d {uptime.Hours}h {uptime.Minutes}m {uptime.Seconds}s",
        serverTimeUtc = DateTime.UtcNow.ToString("u"),
        version = "1.0.0"
    };

    return Results.Ok(healthInfo);
})
.WithName("Health")
.WithDescription("Returns basic health information for the API");

app.Run();
