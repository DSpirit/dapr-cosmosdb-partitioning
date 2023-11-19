using System.Text.Json;
using System.Text.Json.Serialization;
using Dapr.Client;
using Microsoft.AspNetCore.Mvc;

namespace DaprPartitioning.Controllers;

[ApiController]
[Route("[controller]/[action]")]
public class WeatherForecastController : ControllerBase
{
    private DaprClient _client;
    private static readonly string[] Summaries = new[]
    {
        "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
    };

    private readonly ILogger<WeatherForecastController> _logger;

    public WeatherForecastController(ILogger<WeatherForecastController> logger)
    {
        _logger = logger;
        _client = new DaprClientBuilder().Build();
    }

    [HttpGet(Name = "AddRecords")]
    public async Task<IEnumerable<WeatherForecast>> Add()
    {
        var values = Enumerable.Range(1, 5).Select(index => new WeatherForecast
        {
            Date = DateTime.Now.AddDays(index),
            TemperatureC = Random.Shared.Next(-20, 55),
            Summary = Summaries[Random.Shared.Next(Summaries.Length)]
        })
        .ToArray();

        await _client.SaveBulkStateAsync("statestore", values.Select(
            n => new SaveStateItem<WeatherForecast>(Guid.NewGuid().ToString(), n, string.Empty, metadata: new Dictionary<string, string>()
            {
                ["partitionKey"] = n.Summary!
            })).ToList());

        return values;
    }

    [HttpGet(Name = "GetEntries")]
    public async Task<IEnumerable<WeatherForecast>> Get(string type)
    {
        var query = new
        {
            filter = new { }
        };

        var entries = await _client.QueryStateAsync<WeatherForecast>($"statestore",
            JsonSerializer.Serialize(query),
            metadata: new Dictionary<string, string>()
            {
                ["partitionKey"] = type,
            });

        return entries.Results.Select(n => n.Data);
    }
}
