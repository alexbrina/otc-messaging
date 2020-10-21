namespace Otc.Messaging.RabbitMQ.PredefinedTopologies
{
    public class QueueTopology
    {
        public string Name { get; }
        public int DelayMilliseconds { get; }
        public object[] RetryMilliseconds { get; }

        public QueueTopology(string name, int delayMilliseconds, object[] retryMilliseconds = null)
        {
            Name = name;
            DelayMilliseconds = delayMilliseconds;
            RetryMilliseconds = retryMilliseconds ?? new object[0];
        }
    }
}
