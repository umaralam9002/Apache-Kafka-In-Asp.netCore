using Microsoft.AspNetCore.Mvc;
using KafkaConsumerAPI.Services;

namespace KafkaConsumerAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ReplyController : ControllerBase
    {
        private readonly KafkaProducerService _producer;

        public ReplyController()
        {
            _producer = new KafkaProducerService();
        }

        [HttpPost("send")]
        public async Task<IActionResult> SendReply([FromBody] string message)
        {
            await _producer.ProduceAsync("consumer-to-producer", message);
            return Ok("Reply sent to Producer!");
        }
    }
}
