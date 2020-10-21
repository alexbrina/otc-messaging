using Microsoft.Extensions.Logging;
using Otc.Messaging.Abstractions;
using Otc.Messaging.RabbitMQ.Configurations;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Xunit;

namespace Otc.Messaging.RabbitMQ.PredefinedTopologies.Tests
{
    public class MultipleQueuesWithRetryTopologyIntegrationTests
    {
        [Fact]
        public void TwoQueues_TwoRetries_IntegratedTest_Success()
        {
            var configuration = new RabbitMQConfiguration
            {
                Hosts = new List<string> { "localhost" },
                User = "guest",
                Password = "guest"
            };

            configuration.AddTopology<MultipleQueuesWithRetryTopologyFactory>(
                "test-funout", "test-funout-q1", "test-funout-q2", 4000, 8000);

            using (var bus = new RabbitMQMessaging(configuration, new LoggerFactory()))
            {
                bus.EnsureTopology("test-funout");

                bus.CreatePublisher().
                    Publish(Encoding.UTF8.GetBytes("Simple Message"), "test-funout");

                var messages = new List<IMessageContext>();
                var sub = bus.Subscribe((message, messageContext) =>
                {
                    messages.Add(messageContext);
                }, "test-funout-q1", "test-funout-q2");

                sub.Start();
                Thread.Sleep(500);
                sub.Stop();

                Assert.Equal(2, messages.Count);
                Assert.Contains(messages, x => x.Queue == "test-funout-q1");
                Assert.Contains(messages, x => x.Queue == "test-funout-q2");
            }
        }
    }
}
