using Microsoft.AspNetCore.Mvc;

namespace NA_meeting_API.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController : ControllerBase
    {
        private readonly ILogger<WeatherForecastController> _logger;

        public WeatherForecastController(ILogger<WeatherForecastController> logger)
        {
            _logger = logger;
        }

        // Removed the Get method that returns IEnumerable<WeatherForecast>
    }
}
