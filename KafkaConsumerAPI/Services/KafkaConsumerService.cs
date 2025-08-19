using Confluent.Kafka;

namespace KafkaConsumerAPI.Services
{
    public class KafkaConsumerService : BackgroundService
    {
        private readonly ConsumerConfig _consumerConfig;
        private readonly ProducerConfig _producerConfig;

        public KafkaConsumerService()
        {
            _consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "consumer-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            _producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(() =>
            {
                using var consumer = new ConsumerBuilder<Null, string>(_consumerConfig).Build();
                using var producer = new ProducerBuilder<Null, string>(_producerConfig).Build();

                consumer.Subscribe("producer-to-consumer"); 

                Console.WriteLine("Kafka Consumer started...");

                while (!stoppingToken.IsCancellationRequested)
                {
                    var cr = consumer.Consume(stoppingToken);
                    Console.WriteLine($"[Consumer API] Message received: {cr.Message.Value}");

                    
                    var reply = $"Reply from Consumer API: Received '{cr.Message.Value}'";
                    producer.Produce("consumer-topic", new Message<Null, string> { Value = reply });
                    producer.Flush(TimeSpan.FromSeconds(5));
                }
            }, stoppingToken);
        }
    }
}
