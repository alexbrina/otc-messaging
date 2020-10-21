using Microsoft.Extensions.Logging;
using Otc.Messaging.Abstractions;
using Otc.Messaging.RabbitMQ.Configurations;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using Xunit;

namespace Otc.Messaging.RabbitMQ.PredefinedTopologies.Tests
{
    public class MultipleQueuesWithDelayAndRetryTopologyIntegrationTests
    {
        [Fact]
        public void TwoQueues_TwoRetries_IntegratedTest_Success()
        {
            var configuration = new RabbitMQConfiguration
            {
                Hosts = new List<string> { "localhost" },
                User = "guest",
                Password = "guest",
                MessageHandlerErrorBehavior = MessageHandlerErrorBehavior.RejectOnFistDelivery
            };

            var delay = 5000;

            configuration.AddTopology<MultipleQueuesWithDelayAndRetryTopologyFactory>(
                "test-multi-with-delay",
                new QueueTopology("test-multi-with-delay-q1", delay, new object[] { 1000, 1000 }),
                new QueueTopology("test-multi-with-delay-q2", delay, new object[] { 1000, 1000 }));

            using (var bus = new RabbitMQMessaging(configuration, new LoggerFactory()))
            {
                bus.EnsureTopology("test-multi-with-delay");

                var sw1 = new Stopwatch();
                var sw2 = new Stopwatch();

                // Timers started with publishing
                sw1.Start();
                sw2.Start();
                bus.CreatePublisher().
                    Publish(Encoding.UTF8.GetBytes("Simple Message"), "test-multi-with-delay");

                var deliveries = new List<IMessageContext>();
                var messages = new List<IMessageContext>();

                var sub = bus.Subscribe((message, messageContext) =>
                {
                    deliveries.Add(messageContext);

                    if (messageContext.Queue == "test-multi-with-delay-q1")
                    {
                        sw1.Stop();
                    }

                    if (messageContext.Queue == "test-multi-with-delay-q2")
                    {
                        sw2.Stop();
                    }

                    if (!messageContext.Queue.Contains("retry-1"))
                    {
                        throw new InvalidOperationException("Not the queue I want!");
                    }

                    messages.Add(messageContext);
                },
                "test-multi-with-delay-q1",
                "test-multi-with-delay-q1-retry-0",
                "test-multi-with-delay-q1-retry-1",
                "test-multi-with-delay-q2",
                "test-multi-with-delay-q2-retry-0",
                "test-multi-with-delay-q2-retry-1"
                );

                sub.Start();
                Thread.Sleep(delay + 2000 + 500);
                sub.Stop();

                Assert.Equal(6, deliveries.Count);
                Assert.Equal(2, messages.Count);

                Assert.True(delay <= sw1.Elapsed.TotalMilliseconds);
                Assert.True(delay <= sw2.Elapsed.TotalMilliseconds);

                Assert.Contains(messages, x => x.Queue == "test-multi-with-delay-q1-retry-1");
                Assert.Contains(messages, x => x.Queue == "test-multi-with-delay-q2-retry-1");
            }
        }
    }
}
