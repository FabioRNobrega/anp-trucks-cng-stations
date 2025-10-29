using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using CsvHelper;
using CsvHelper.Configuration;


var builder = WebApplication.CreateBuilder(args);

// Prevent timeout for ANP rate limit
builder.WebHost.ConfigureKestrel(o =>
{
    o.Limits.KeepAliveTimeout = TimeSpan.FromMinutes(10);
    o.Limits.RequestHeadersTimeout = TimeSpan.FromMinutes(10);
});

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
var httpClient = new HttpClient{ Timeout = TimeSpan.FromMinutes(5)}; // Prevent timeout for ANP rate limit
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



app.MapGet("/truck-cgn-stations", async (HttpContext context) =>
{
    const string baseUrl = "https://revendedoresapi.anp.gov.br/v1/combustivel?numeropagina=";

    var allStations = new List<Station>();
    var options = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true,
        NumberHandling = System.Text.Json.Serialization.JsonNumberHandling.AllowReadingFromString
    };
    int currentPage = 1;
    int totalPages = 1;
    int retries = 0;
    const int maxRetries = 5;

    do
    {
        // Get ANP API response
        var response = await httpClient.GetAsync($"{baseUrl}{currentPage}");

        // Try after header how long to wait
        if (response.StatusCode == HttpStatusCode.TooManyRequests)
        {
            var retryAfter = response.Headers.RetryAfter?.Delta?.TotalMilliseconds ?? (1000 * Math.Pow(2, retries));
            Console.WriteLine($" Rate-limited, waiting {retryAfter / 1000:F1}s before retry ({retries + 1}/{maxRetries})");
            await Task.Delay((int)retryAfter);
            retries++;
            if (retries >= maxRetries) break;
            continue;
        }

        if (!response.IsSuccessStatusCode)
        {
            Console.WriteLine($"Failed to Fetch data from ANP API page {currentPage}. Status: {response.StatusCode}");
            break;
        };

        // success -> reset retries
        retries = 0;

        var json = await response.Content.ReadAsStringAsync();
        var anpResponse = JsonSerializer.Deserialize<AnpResponse>(json, options);

        if (anpResponse?.Data == null) break;

        // Merge
        allStations.AddRange(anpResponse.Data);

        // Get total pages
        if (anpResponse.SearchPageFilter?.TotalPagina > 0)
            totalPages = anpResponse.SearchPageFilter.TotalPagina;

        Console.Write($"Loaded page {currentPage}/{totalPages} ({allStations.Count} stations)");
        currentPage++;

        // Prevent ANP API request rate limit
        await Task.Delay(5000);

    } while (currentPage <= totalPages);
    
    if (allStations.Count == 0)
    {
        return Results.Problem("No data Returned from ANP API");
    }

    var filtered = FilterStations(allStations).ToList();
    var enriched = EnrichStations(filtered, AddNaturgyFlag, AddAccuracyScore).ToList();

    var csvBytes = CsvExporter.ExportStationsToScv(enriched);

    context.Response.Headers.Append(
        "Content-Disposition",
        "attachment; filename=truck_cng_stations_by_anp.csv"
    );

    return Results.File(csvBytes, "text/csv");

})
.WithName("GetTruckCngStations")
.WithDescription("Returns all ANP stations from page 1 that provide GNV");

// Filters
static IEnumerable<Station> FilterStations(IEnumerable<Station> data)
{
    return data.Where(IsActive)
        .Where(HasCng)
        .Where(HasRoadHints)
        .Where(HasDieselWithCapacity);
}

static bool HasCng(Station s)
{
    if (s.Produtos == null) return false;
    return s.Produtos.Any(p =>
        string.Equals(p.Produto, "GÁS NATURAL VEICULAR", StringComparison.OrdinalIgnoreCase)
        && (p.QtdeBicos ?? 0) > 0
    );
}  

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

static bool HasDieselWithCapacity(Station s)
{
    if (s.Produtos == null) return false;

    var dieselTanks = s.Produtos
         .Where(p => !string.IsNullOrWhiteSpace(p.Produto) &&
         (p.Produto.Contains("S10", StringComparison.OrdinalIgnoreCase) || p.Produto.Contains("S500", StringComparison.OrdinalIgnoreCase)))
         .Select(p => p.Tancagem ?? 0)
         .ToList();

    if (dieselTanks.Count == 0) return false;

    var totalDieselCapacity = dieselTanks.Sum();

    return totalDieselCapacity >= 30;
}

static bool IsActive(Station s)
{
    return string.Equals(s.SituacaoConstatada, "200", StringComparison.OrdinalIgnoreCase);
}

// Data Additions
static IEnumerable<Station> EnrichStations(IEnumerable<Station> stations, params StationEnricher[] enrichers)
{
    foreach (var station in stations)
    {
        var enriched = station;
        foreach (var enricher in enrichers)
            enriched = enricher(enriched);

        yield return enriched;
    }
}

static Station AddNaturgyFlag(Station s)
{
    s.NaturgyVerified = s.Cnpj != null && Naturgy.Cnpjs.Contains(s.Cnpj);
    return s;
}

static Station AddAccuracyScore(Station s)
{
    double score = 0;

    if (HasCng(s)) score += 20;
    if (HasDieselWithCapacity(s)) score += 20;
    if (HasRoadHints(s)) score += 20;
    if (IsActive(s)) score += 20;

    // estimativaAcuracia fro GPS: lower = better
    if (double.TryParse(s.EstimativaAcuracia, out double est))
    {
        double positionScore = 10 - Math.Min(est, 10);
        if (positionScore > 0) score += positionScore;
    }

    if (s.NaturgyVerified)
    {
        s.AccuracyScore = 100;
        return s;
    } 

    s.AccuracyScore = Math.Min(score, 100);
    return s;
}



app.Run();

// Models
delegate Station StationEnricher(Station s);

public class AnpResponse
{
    public int Status { get; set; }
    public string? Title { get; set; }
    public bool Succeeded { get; set; }
    public List<Station>? Data { get; set; }
    public SearchPageFilter? SearchPageFilter { get; set; }
}

public class SearchPageFilter
{
    public int NumeroPagina { get; set; }
    public int TamanhoPagina { get; set; }
    public int TotalRegistro { get; set; }
    public int TotalPagina { get; set; }
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
    public string? Cep { get; set; }
    public string? SituacaoConstatada { get; set; }
    public string? Distribuidora { get; set; }
    public List<Product>? Produtos { get; set; }
    public string? Latitude { get; set; }
    public string? Longitude { get; set; }
    public string? EstimativaAcuracia { get; set; }
    public bool NaturgyVerified { get; set; }
    public double AccuracyScore { get; set; }
}

public class Product
{
    public string? Produto { get; set; }
    public double? Tancagem { get; set; }
    public string? UnidMedidaTancagem { get; set; }
    public int? QtdeBicos { get; set; }
}

public static class Naturgy
{
    public static readonly HashSet<string> Cnpjs = new()
    {
        "01797812000172",
        "30243299000176",
        "29178001000102",
        "00624710000192",
        "07187563000180",
        "31465255000153",
        "08064380000130",
        "06012414000117"
    };
}

public static class CsvSchema
{
    public static readonly string[] Headers =
    {
        "status",
        "site_name",
        "street",
        "zip_code",
        "city",
        "country",
        "country_code",
        "latitude",
        "longitude",
        "operator",
        "verified_for_trucks",
        "green_certified",
        "lots_partner",
        "restrooms",
        "food",
        "wifi",
        "card_terminal",
        "truck_parking",
        "showers",
        "truck_wash",
        "google_maps_url",
        "date_when_added_to_list",
        "source",
        "comments",
        "accuracy_score"
    };
}


public static class CsvExporter
{
    public static byte[] ExportStationsToScv(IEnumerable<Station> stations)
    {
        using var memory = new MemoryStream();
        using var writer = new StreamWriter(memory, Encoding.UTF8);
        using var csv = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = ",",
            Quote = '"',
            ShouldQuote = args => true,
        });

        foreach (var header in CsvSchema.Headers)
            csv.WriteField(header);
        csv.NextRecord();

        // Write station rows
        foreach (var s in stations)
        {
            csv.WriteField(s.SituacaoConstatada);
            csv.WriteField(s.RazaoSocial);
            csv.WriteField(s.Endereco);
            csv.WriteField(s.Cep);
            csv.WriteField(s.Municipio);
            csv.WriteField("Brazil");
            csv.WriteField("BR");
            csv.WriteField("");
            csv.WriteField("");
            csv.WriteField(s.Distribuidora);
            csv.WriteField(s.NaturgyVerified ? "true" : "false"); // verified_for_trucks
            csv.WriteField(""); // green_certified
            csv.WriteField("false"); // lots_partner
            csv.WriteField(""); // restrooms
            csv.WriteField(""); // food
            csv.WriteField(""); // wifi
            csv.WriteField(""); // card_terminal
            csv.WriteField(""); // truck_parking
            csv.WriteField(""); // showers
            csv.WriteField(""); // truck_wash
            csv.WriteField(GetGoogleMapsUrl(s));
            csv.WriteField(DateTime.UtcNow.ToString("yyyy-MM-dd")); // date_when_added_to_list
            csv.WriteField("ANP API v1");
            csv.WriteField(""); // comments
            csv.WriteField(s.AccuracyScore.ToString("0.0", CultureInfo.InvariantCulture));
            csv.NextRecord();
        }

        writer.Flush();
        return memory.ToArray();

    }
    
    private static string GetGoogleMapsUrl(Station s)
    {
        if (string.IsNullOrWhiteSpace(s.RazaoSocial) &&
        string.IsNullOrWhiteSpace(s.Endereco) &&
        string.IsNullOrWhiteSpace(s.Municipio))
            return "";

        var query = Uri.EscapeDataString($"{s.RazaoSocial} {s.Endereco} {s.Municipio} {s.Uf}");
        return $"https://www.google.com/maps?q={query}";
    }
}
