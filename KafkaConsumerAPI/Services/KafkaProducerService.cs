using Confluent.Kafka;

namespace KafkaConsumerAPI.Services
{
    public class KafkaProducerService
    {
        private readonly ProducerConfig _config;

        public KafkaProducerService()
        {
            _config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };
        }

        public async Task ProduceAsync(string topic, string message)
        {
            using var producer = new ProducerBuilder<Null, string>(_config).Build();
            await producer.ProduceAsync(topic, new Message<Null, string> { Value = message });
        }
    }
}
