using System.Globalization;
using System.Text;
using System.Text.Json;


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
var httpClient = new HttpClient();
httpClient.DefaultRequestHeaders.Add("User-Agent", "AnpCnGStation/1.0");

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



app.MapGet("/truck-cgn-stations", async () =>
{
    const string url = "https://revendedoresapi.anp.gov.br/v1/combustivel?numeropagina=1";

    // Get ANP API response
    var response = await httpClient.GetAsync(url);
    if (!response.IsSuccessStatusCode)
    {
        return Results.Problem($"Failed to Fetch data from ANP API. Status: {response.StatusCode}");
    };

    var json = await response.Content.ReadAsStringAsync();
    var options = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true,
        NumberHandling = System.Text.Json.Serialization.JsonNumberHandling.AllowReadingFromString
    };

    var anpResponse = JsonSerializer.Deserialize<AnpResponse>(json, options);
    if (anpResponse?.Data == null) return Results.Problem("No data returned from ANP API.");

    var filtered = FilterStations(anpResponse.Data).ToList();

    return Results.Ok(new { total = filtered.Count, stations = filtered });
})
.WithName("GetTruckCngStations")
.WithDescription("Returns all ANP stations from page 1 that provide GNV");

// Helpers

static IEnumerable<Station> FilterStations(IEnumerable<Station> data)
{
    return data.Where(HasCng)
        .Where(HasRoadHints);
}

static bool HasCng(Station s) => s.Produtos?.Any(p => string.Equals(p.Produto, "GÁS NATURAL VEICULAR", StringComparison.OrdinalIgnoreCase)) == true;

static bool HasRoadHints(Station s)
{
    var haystack = Normalize($"{s.Endereco} {s.Complemento}");
    string[] needles = { "RODOVIA", "DUTRA", "KM" };

    return needles.Any(n => haystack.Contains(n, StringComparison.Ordinal));
}

static string Normalize(string? value)
{
    if (string.IsNullOrWhiteSpace(value)) return string.Empty;

    var formD = value.Normalize(NormalizationForm.FormD);
    var sb = new StringBuilder(capacity: formD.Length);
    foreach (var ch in formD)
    {
        var uc = CharUnicodeInfo.GetUnicodeCategory(ch);
        if (uc != UnicodeCategory.NonSpacingMark) sb.Append(ch);
    }

    return sb.ToString().Normalize(NormalizationForm.FormC).ToUpperInvariant();
}

app.Run();

// Models

public class AnpResponse
{
    public int Status { get; set; }
    public string? Title { get; set; }
    public bool Succeeded { get; set; }
    public List<Station>? Data { get; set; }
}

public class Station
{
    public string? CodigoSIMP { get; set; }
    public string? RazaoSocial { get; set; }
    public string? Cnpj { get; set; }
    public string? Endereco { get; set; }
    public string? Complemento { get; set; }
    public string? Municipio { get; set; }
    public string? Uf { get; set; }
    public string? SituacaoConstatada { get; set; }
    public List<Product>? Produtos { get; set; }
    public string? Latitude { get; set; }
    public string? Longitude { get; set; }
}

public class Product
{
    public string? Produto { get; set; }
    public double? Tancagem { get; set; }
    public string? UnidMedidaTancagem { get; set; }
    public int? QtdeBicos { get; set; }
}
