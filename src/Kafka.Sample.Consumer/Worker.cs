using Confluent.Kafka;

namespace Kafka.Sample.Consumer
{
    public class Worker(ILogger<Worker> logger) : BackgroundService
    {
        private readonly string _bootstrapServers = "localhost:9092";
        private readonly string _topic = "my-topic";
        private readonly string _groupId = "my-group";

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = _groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(_topic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var cr = consumer.Consume(stoppingToken);
                    logger.LogInformation("Message: {message} received from {topicPartition}", cr.Value, cr.TopicPartitionOffset);

                    await Task.Delay(1000, stoppingToken);
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "An error occurred");
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}
