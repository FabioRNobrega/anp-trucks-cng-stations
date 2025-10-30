using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using CsvHelper;
using CsvHelper.Configuration;

DotNetEnv.Env.Load();

var builder = WebApplication.CreateBuilder(args);

// Prevent timeout for ANP rate limit
builder.WebHost.ConfigureKestrel(o =>
{
    o.Limits.KeepAliveTimeout = TimeSpan.FromMinutes(10);
    o.Limits.RequestHeadersTimeout = TimeSpan.FromMinutes(10);
});

// Add OpenAPI support
builder.Services.AddOpenApi();


// Mapbox Config
var mapboxKey = builder.Configuration["MAPBOX_API_KEY"] ?? Environment.GetEnvironmentVariable("MAPBOX_API_KEY") ?? throw new Exception("MAPBOX_API_KEY not set");

builder.Services.AddSingleton(new MapboxGeocoder(new HttpClient(), mapboxKey));

var app = builder.Build();

// Configure the HTTP request pipeline
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

var startTime = DateTime.UtcNow;
var httpClient = new HttpClient{ Timeout = TimeSpan.FromMinutes(10)}; // Prevent timeout for ANP rate limit
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

app.MapPost("/truck-cng-stations/add-coords/upload-csv", async (HttpRequest request, MapboxGeocoder geocoder) =>
{
    Console.WriteLine("Received request to upload CSV for geocoding...");

    if (!request.HasFormContentType)
    {
        Console.WriteLine($"Invalid content type: {request.ContentType}");
        return Results.BadRequest("Expected multipart/form-data with CSV file");
    }

    var form = await request.ReadFormAsync();
    var file = form.Files.GetFile("file");

    if (file == null || file.Length == 0)
    {
        Console.WriteLine("Missing or empty file in request.");
        return Results.BadRequest("Missing or empty file");
    }

    Console.WriteLine($"File received: {file.FileName} ({file.Length} bytes)");

    using var reader = new StreamReader(file.OpenReadStream());
    using var csvReader = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture) { Delimiter = "," });
    var records = csvReader.GetRecords<dynamic>().ToList();

    Console.WriteLine($"CSV loaded with {records.Count} rows");

    var enriched = new List<IDictionary<string, object>>();
    int successCount = 0, failCount = 0;
    int rowIndex = 0;

    foreach (var record in records)
    {
        rowIndex++;
        var dict = (IDictionary<string, object>)record;

        string street = dict.ContainsKey("street") ? dict["street"]?.ToString() ?? "" : "";
        string zip = dict.ContainsKey("zip_code") ? dict["zip_code"]?.ToString() ?? "" : "";
        string city = dict.ContainsKey("city") ? dict["city"]?.ToString() ?? "" : "";
        string country = dict.ContainsKey("country") ? dict["country"]?.ToString() ?? "" : "";
        string code = dict.ContainsKey("country_code") ? dict["country_code"]?.ToString() ?? "" : "";

        var address = $"{street}, {zip}, {city}, {country ?? code}";
        Console.WriteLine($"[{rowIndex}] Geocoding: {address}");

        try
        {
            var coords = await geocoder.GetCoordinatesAsync(street, zip, city, country, code);

            if (coords.HasValue)
            {
                dict["latitude"] = coords.Value.lat.ToString(CultureInfo.InvariantCulture);
                dict["longitude"] = coords.Value.lon.ToString(CultureInfo.InvariantCulture);
                successCount++;
                Console.WriteLine($"[{rowIndex}] Coordinates found: ({coords.Value.lat}, {coords.Value.lon})");
            }
            else
            {
                dict["latitude"] = "";
                dict["longitude"] = "";
                failCount++;
                Console.WriteLine($"[{rowIndex}] No coordinates found");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[{rowIndex}] Error during geocoding: {ex.Message}");
            failCount++;
        }

        enriched.Add(dict);
    }

    Console.WriteLine($"Processing complete: {successCount} successes, {failCount} failures");

    using var memory = new MemoryStream();
    using (var writer = new StreamWriter(memory, Encoding.UTF8, leaveOpen: true))
    using (var csv = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture)))
    {
        var headers = enriched.First().Keys;
        foreach (var h in headers) csv.WriteField(h);
        csv.NextRecord();

        foreach (var row in enriched)
        {
            foreach (var val in row.Values)
                csv.WriteField(val);
            csv.NextRecord();
        }

        writer.Flush();
    }

    memory.Position = 0;
    Console.WriteLine($"Returning enriched CSV file with {enriched.Count} rows");
    return Results.File(memory.ToArray(), "text/csv", "enriched_with_coords.csv");
})
.WithName("UploadCsv")
.WithDescription("Uploads a CSV and returns it enriched with latitude/longitude from Mapbox API");


app.MapGet("/truck-cgn-stations", async (HttpContext context, int? page) =>
{
    const string baseUrl = "https://revendedoresapi.anp.gov.br/v1/combustivel?numeropagina=";
    const string progressFile = "progress.json";
    const string csvFile = "anp_station_partial.csv";

    var options = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true,
        NumberHandling = System.Text.Json.Serialization.JsonNumberHandling.AllowReadingFromString
    };
    int startPage = page ?? 1;
    int totalFetched = 0;
    int pagesFetched = 0;

    // Check for progress.json
    if (!page.HasValue && File.Exists(progressFile))
    {
        var progress = JsonSerializer.Deserialize<ProgressInfo>(File.ReadAllText(progressFile));
        if (progress != null)
        {
            startPage = progress.LastCompletedPage + 1;
            totalFetched = progress.StationSaved;
            Console.WriteLine($"Resuming from page {startPage}, {totalFetched} stations saved so far");
        }
    }

    Console.WriteLine("#### START REQUEST WITH PAGINATION");

    do
    {
        // Get ANP API response
        var response = await httpClient.GetAsync($"{baseUrl}{startPage}");
        if (!response.IsSuccessStatusCode)
        {
            Console.WriteLine($"Failed to Fetch data from ANP API page {startPage}. Status: {response.StatusCode}");
            break;
        };

        var json = await response.Content.ReadAsStringAsync();
        var anpResponse = JsonSerializer.Deserialize<AnpResponse>(json, options);

        // stop when no data
        if (anpResponse?.Data == null || anpResponse?.Data.Count == 0)
        {
            Console.WriteLine($"No more data after page {startPage - 1}.");
            break;
        }

        var filtered = FilterStations(anpResponse!.Data!).ToList();
        var enriched = EnrichStations(filtered, AddNaturgyFlag, AddAccuracyScore).OrderByDescending(s => s.AccuracyScore).ToList();

        // Save wach page
        CsvExporter.AppendStationToScv(enriched, csvFile);

        totalFetched += enriched.Count;
        pagesFetched++;
        Console.WriteLine($"Loaded page {startPage} (+{anpResponse!.Data.Count}, total {totalFetched})");

        // Update progress.json
        var progressInfo = new ProgressInfo
        {
            LastCompletedPage = startPage,
            StationSaved = totalFetched,
            Timestamp = DateTime.UtcNow
        };
        File.WriteAllText(progressFile, JsonSerializer.Serialize(progressInfo, new JsonSerializerOptions { WriteIndented = true }));

        // Prevent ANP API request rate limit
        await Task.Delay(5000);
        startPage++;
    } while (!page.HasValue);

    Console.WriteLine($"############## FETCH COMPLETED: {pagesFetched} pages processed, {totalFetched} stations saved (last page: {startPage - 1})");

    if (page.HasValue)
    {
        var csvBytes = await File.ReadAllBytesAsync(csvFile);
        context.Response.Headers.Append(
            "Content-Disposition",
            "attachment; filename=truck_cng_stations_by_anp.csv"
        );

        return Results.File(csvBytes, "text/csv");
    }

    // Full fetch mode
   return Results.Json(new
    {
        status = "completed",
        pages_processed = pagesFetched,
        stations_saved = totalFetched,
        last_page = startPage - 1,
        file = csvFile,
        progress = progressFile
    });

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
        "accuracy_score",
        "identification_document"
    };
}


public static class CsvExporter
{
    public static byte[] ExportStationsToCsv(IEnumerable<Station> stations)
    {
        using var memory = new MemoryStream();
        using var writer = new StreamWriter(memory, Encoding.UTF8);
        WriteStationToCsv(writer, stations, writeHeader: true);
        writer.Flush();
        return memory.ToArray();
    }

    public static void AppendStationToScv(IEnumerable<Station> stations, string filePath)
    {
        bool fileExists = File.Exists(filePath);
        using var stream = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.Read);
        using var writer = new StreamWriter(stream, Encoding.UTF8);
        WriteStationToCsv(writer, stations, writeHeader: !fileExists);
    }

    private static void WriteStationToCsv(TextWriter writer, IEnumerable<Station> stations, bool writeHeader)
    {
        using var csv = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = ",",
            Quote = '"',
            ShouldQuote = args => true,
        });

        if (writeHeader)
        {
            foreach (var header in CsvSchema.Headers)
                csv.WriteField(header);
            csv.NextRecord();
        }

        // ✅ Write consistent rows
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
            csv.WriteField(DateTime.UtcNow.ToString("yyyy-MM-dd"));
            csv.WriteField("ANP API v1");
            csv.WriteField("");
            csv.WriteField(s.AccuracyScore.ToString("0.0", CultureInfo.InvariantCulture));
            csv.WriteField(s.Cnpj);
            csv.NextRecord();
        }

        writer.Flush();
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

public class ProgressInfo
{
    public int LastCompletedPage { get; set; }
    public int StationSaved { get; set; }
    public DateTime Timestamp { get; set; }
}

public class MapboxGeocoder
{
    private readonly HttpClient _httpClient;
    private readonly string _apiKey;

    public MapboxGeocoder(HttpClient httpClient, string apiKey)
    {
        _httpClient = httpClient;
        _apiKey = apiKey;
    }

    public async Task<(double lat, double lon)?> GetCoordinatesAsync(string street, string zip, string city, string country, string countryCode)
    {
        if (string.IsNullOrWhiteSpace(street) && string.IsNullOrWhiteSpace(city)) return null;

        var address = $"{street}, {zip}, {country ?? country}";
        var encodedAddress = Uri.EscapeDataString(address);
        var url = $"https://api.mapbox.com/geocoding/v5/mapbox.places/{encodedAddress}.json?access_token={_apiKey}&limit=1";

        var resp = await _httpClient.GetAsync(url);
        if (!resp.IsSuccessStatusCode) return null;

        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
        var features = doc.RootElement.GetProperty("features");
        if (features.GetArrayLength() == 0) return null;

        var coords = features[0].GetProperty("center");
        return (coords[1].GetDouble(), coords[0].GetDouble()); // lat, lon
    }
}
