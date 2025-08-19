using Confluent.Kafka;

namespace KafkaProducerAPI.Services
{
    public class KafkaConsumerService : BackgroundService
    {
        private readonly ConsumerConfig _config;

        public KafkaConsumerService()
        {
            _config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "producer-listener-group", // unique group for Producer API
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(() =>
            {
                using var consumer = new ConsumerBuilder<Null, string>(_config).Build();

                // Producer ko Consumer ka reply sunna hai
                consumer.Subscribe("consumer-to-producer");

                Console.WriteLine("ProducerAPI is listening on topic 'consumer-to-producer'...");

                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var cr = consumer.Consume(stoppingToken);
                        Console.WriteLine($"[Producer API] Reply received: {cr.Message.Value}");
                    }
                    catch (ConsumeException ex)
                    {
                        Console.WriteLine($" Error while consuming: {ex.Error.Reason}");
                    }
                }
            }, stoppingToken);
        }
    }
}
