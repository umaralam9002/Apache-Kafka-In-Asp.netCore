using Microsoft.AspNetCore.Mvc;
using KafkaProducerAPI.Services;

namespace KafkaProducerAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class MessageController : ControllerBase
    {
        private readonly KafkaProducerService _producer;

        public MessageController()
        {
            _producer = new KafkaProducerService();
        }

        [HttpPost("send")]
        public async Task<IActionResult> Send([FromBody] string message)
        {
            await _producer.ProduceAsync("producer-to-consumer", message);
            return Ok("Message sent to Kafka!");
        }
    }
}
