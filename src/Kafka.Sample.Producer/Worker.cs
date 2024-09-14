using Confluent.Kafka;

namespace Kafka.Sample.Producer
{
    public class Worker(ILogger<Worker> logger) : BackgroundService
    {
        private readonly string _bootstrapServers = "localhost:9092";
        private readonly string _topic = "my-topic";

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var config = new ProducerConfig { BootstrapServers = _bootstrapServers };

            using var producer = new ProducerBuilder<Null, string>(config).Build();

            while (!stoppingToken.IsCancellationRequested)
            {
                var message = $"Hello Kafka at {DateTimeOffset.Now}";

                try
                {
                    var deliveryResult = await producer.ProduceAsync(_topic, new Message<Null, string> { Value = message });
                    logger.LogInformation("Message delivered to {topicPartition}", deliveryResult.TopicPartitionOffset);
                }
                catch (ProduceException<Null, string> e)
                {
                    logger.LogError($"Delivery failed: {e.Error.Reason}");
                }

                await Task.Delay(1000, stoppingToken);
            }
        }
    }
}
