using Microsoft.Extensions.Logging;
using Otc.Messaging.Abstractions;
using Otc.Messaging.RabbitMQ.Configurations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Xunit;

namespace Otc.Messaging.RabbitMQ.PredefinedTopologies.Tests
{
    public class MultipleQueuesWithDelayAndRetryTopologyTests
    {
        [Fact]
        public void TwoQueuesBothWithDelay_ThreeRetries_Success()
        {
            var configuration = new RabbitMQConfiguration();

            configuration.AddTopology<MultipleQueuesWithDelayAndRetryTopologyFactory>(
                "mytopic",
                new QueueTopology("q1", 1000, new object[] { 4000, 12000, 36000 }),
                new QueueTopology("q2", 2000, new object[] { 8000, 16000, 32000 }));

            var exchanges = configuration.Topologies["mytopic"].Exchanges;
            var exchange = exchanges.First();

            Assert.Equal("mytopic", exchange.Name);
            Assert.Equal(2, exchange.Queues.Count);

            Assert.Equal("q1-delay", GetSingleQueueDlxName(exchanges, "mytopic", "q1-delay"));
            Assert.Equal("q2-delay", GetSingleQueueDlxName(exchanges, "mytopic", "q2-delay"));

            for (int i = 1; i < 3; i++)
            {
                Assert.Equal($"q{i}-wait-0", GetSingleQueueDlxName(exchanges, $"q{i}-delay", $"q{i}"));
                Assert.Equal($"q{i}-retry-0", GetSingleQueueDlxName(exchanges, $"q{i}-wait-0"));
                Assert.Equal($"q{i}-wait-1", GetSingleQueueDlxName(exchanges, $"q{i}-retry-0"));
                Assert.Equal($"q{i}-retry-1", GetSingleQueueDlxName(exchanges, $"q{i}-wait-1"));
                Assert.Equal($"q{i}-wait-2", GetSingleQueueDlxName(exchanges, $"q{i}-retry-1"));
                Assert.Equal($"q{i}-retry-2", GetSingleQueueDlxName(exchanges, $"q{i}-wait-2"));
                Assert.Equal($"q{i}-dead", GetSingleQueueDlxName(exchanges, $"q{i}-retry-2"));
            }

            Assert.Equal(1000, GetSingleQueueTtl(exchanges, "mytopic", "q1-delay"));
            Assert.Equal(2000, GetSingleQueueTtl(exchanges, "mytopic", "q2-delay"));

            Assert.Equal(4000, GetSingleQueueTtl(exchanges, "q1-wait-0"));
            Assert.Equal(12000, GetSingleQueueTtl(exchanges, "q1-wait-1"));
            Assert.Equal(36000, GetSingleQueueTtl(exchanges, "q1-wait-2"));

            Assert.Equal(8000, GetSingleQueueTtl(exchanges, "q2-wait-0"));
            Assert.Equal(16000, GetSingleQueueTtl(exchanges, "q2-wait-1"));
            Assert.Equal(32000, GetSingleQueueTtl(exchanges, "q2-wait-2"));
        }

        [Fact]
        public void TwoQueuesOnlyOneWithDelay_TowRetries_Success()
        {
            var configuration = new RabbitMQConfiguration();

            configuration.AddTopology<MultipleQueuesWithDelayAndRetryTopologyFactory>(
                "mytopic",
                new QueueTopology("q1", 0, new object[] { 4000, 12000 }),
                new QueueTopology("q2", 2000, new object[] { 8000, 16000 }));

            var exchanges = configuration.Topologies["mytopic"].Exchanges;
            var exchange = exchanges.First();

            Assert.Equal("mytopic", exchange.Name);
            Assert.Equal(2, exchange.Queues.Count);

            Assert.Equal("q1-wait-0", GetSingleQueueDlxName(exchanges, "mytopic", "q1"));

            Assert.Equal("q2-delay", GetSingleQueueDlxName(exchanges, "mytopic", "q2-delay"));
            Assert.Equal("q2-wait-0", GetSingleQueueDlxName(exchanges, "q2-delay", "q2"));

            for (int i = 1; i < 3; i++)
            {
                Assert.Equal($"q{i}-retry-0", GetSingleQueueDlxName(exchanges, $"q{i}-wait-0"));
                Assert.Equal($"q{i}-wait-1", GetSingleQueueDlxName(exchanges, $"q{i}-retry-0"));
                Assert.Equal($"q{i}-retry-1", GetSingleQueueDlxName(exchanges, $"q{i}-wait-1"));
                Assert.Equal($"q{i}-dead", GetSingleQueueDlxName(exchanges, $"q{i}-retry-1"));
            }

            Assert.Equal(2000, GetSingleQueueTtl(exchanges, "mytopic", "q2-delay"));

            Assert.Equal(4000, GetSingleQueueTtl(exchanges, "q1-wait-0"));
            Assert.Equal(12000, GetSingleQueueTtl(exchanges, "q1-wait-1"));

            Assert.Equal(8000, GetSingleQueueTtl(exchanges, "q2-wait-0"));
            Assert.Equal(16000, GetSingleQueueTtl(exchanges, "q2-wait-1"));
        }

        [Fact]
        public void TwoQueues_OneWithNoRetries_Success()
        {
            var configuration = new RabbitMQConfiguration();

            configuration.AddTopology<MultipleQueuesWithDelayAndRetryTopologyFactory>(
                "mytopic",
                new QueueTopology("q1", 1000),
                new QueueTopology("q2", 2000, new object[] { 8000, 16000 }));

            var exchanges = configuration.Topologies["mytopic"].Exchanges;
            var exchange = exchanges.First();

            Assert.Equal("mytopic", exchange.Name);
            Assert.Equal(2, exchange.Queues.Count);

            Assert.Equal("q1-delay", GetSingleQueueDlxName(exchanges, "mytopic", "q1-delay"));
            Assert.Equal("q1-dead", GetSingleQueueDlxName(exchanges, "q1-delay", "q1"));

            Assert.Equal("q2-delay", GetSingleQueueDlxName(exchanges, "mytopic", "q2-delay"));
            Assert.Equal("q2-wait-0", GetSingleQueueDlxName(exchanges, "q2-delay", "q2"));
            Assert.Equal("q2-retry-0", GetSingleQueueDlxName(exchanges, "q2-wait-0"));
            Assert.Equal("q2-wait-1", GetSingleQueueDlxName(exchanges, "q2-retry-0"));
            Assert.Equal("q2-retry-1", GetSingleQueueDlxName(exchanges, "q2-wait-1"));
            Assert.Equal("q2-dead", GetSingleQueueDlxName(exchanges, "q2-retry-1"));

            Assert.Equal(1000, GetSingleQueueTtl(exchanges, "mytopic", "q1-delay"));
            Assert.Equal(2000, GetSingleQueueTtl(exchanges, "mytopic", "q2-delay"));

            Assert.Equal(8000, GetSingleQueueTtl(exchanges, "q2-wait-0"));
            Assert.Equal(16000, GetSingleQueueTtl(exchanges, "q2-wait-1"));
        }

        [Fact]
        public void TwoQueues_OneWithNoDelayNorRetries_Success()
        {
            var configuration = new RabbitMQConfiguration();

            configuration.AddTopology<MultipleQueuesWithDelayAndRetryTopologyFactory>(
                "mytopic",
                new QueueTopology("q1", 0),
                new QueueTopology("q2", 2000, new object[] { 8000, 16000 }));

            var exchanges = configuration.Topologies["mytopic"].Exchanges;
            var exchange = exchanges.First();

            Assert.Equal("mytopic", exchange.Name);
            Assert.Equal(2, exchange.Queues.Count);

            Assert.Equal("q1-dead", GetSingleQueueDlxName(exchanges, "mytopic", "q1"));

            Assert.Equal("q2-delay", GetSingleQueueDlxName(exchanges, "mytopic", "q2-delay"));
            Assert.Equal("q2-wait-0", GetSingleQueueDlxName(exchanges, "q2-delay", "q2"));
            Assert.Equal("q2-retry-0", GetSingleQueueDlxName(exchanges, "q2-wait-0"));
            Assert.Equal("q2-wait-1", GetSingleQueueDlxName(exchanges, "q2-retry-0"));
            Assert.Equal("q2-retry-1", GetSingleQueueDlxName(exchanges, "q2-wait-1"));
            Assert.Equal("q2-dead", GetSingleQueueDlxName(exchanges, "q2-retry-1"));

            Assert.Null(GetSingleQueueTtl(exchanges, "mytopic", "q1"));
            Assert.Equal(2000, GetSingleQueueTtl(exchanges, "mytopic", "q2-delay"));

            Assert.Equal(8000, GetSingleQueueTtl(exchanges, "q2-wait-0"));
            Assert.Equal(16000, GetSingleQueueTtl(exchanges, "q2-wait-1"));
        }

        [Fact]
        public void TwoQueues_OneWithNoDelay_NoneWithRetries_Success()
        {
            var configuration = new RabbitMQConfiguration();

            configuration.AddTopology<MultipleQueuesWithDelayAndRetryTopologyFactory>(
                "mytopic",
                new QueueTopology("q1", 0),
                new QueueTopology("q2", 2000));

            var exchanges = configuration.Topologies["mytopic"].Exchanges;
            var exchange = exchanges.First();

            Assert.Equal("mytopic", exchange.Name);
            Assert.Equal(2, exchange.Queues.Count);

            Assert.Equal("q1-dead", GetSingleQueueDlxName(exchanges, "mytopic", "q1"));
            Assert.Equal("q2-delay", GetSingleQueueDlxName(exchanges, "mytopic", "q2-delay"));
            Assert.Equal("q2-dead", GetSingleQueueDlxName(exchanges, "q2-delay", "q2"));

            Assert.Null(GetSingleQueueTtl(exchanges, "mytopic", "q1"));
            Assert.Equal(2000, GetSingleQueueTtl(exchanges, "mytopic", "q2-delay"));
        }

        [Fact]
        public void Arguments_Failures()
        {
            var configuration = new RabbitMQConfiguration();

            Exception ex;

            ex = Assert.Throws<ArgumentException>(() =>
            {
                configuration.AddTopology<MultipleQueuesWithDelayAndRetryTopologyFactory>(
                    "mytopic");
            });

            Assert.Contains("A minimum of 2 queues must be provided", ex.Message);

            ex = Assert.Throws<ArgumentException>(() =>
            {
                configuration.AddTopology<MultipleQueuesWithDelayAndRetryTopologyFactory>(
                    "mytopic", "not-QueueTopology");
            });

            Assert.Contains($"must be of type {nameof(QueueTopology)}", ex.Message);

            ex = Assert.Throws<ArgumentException>(() =>
            {
                configuration.AddTopology<MultipleQueuesWithDelayAndRetryTopologyFactory>(
                    "mytopic", new QueueTopology("q", 0));
            });

            Assert.Contains("A minimum of 2 queues must be provided", ex.Message);

            ex = Assert.Throws<ArgumentException>(() =>
            {
                configuration.AddTopology<MultipleQueuesWithDelayAndRetryTopologyFactory>(
                    "mytopic", new QueueTopology("q1", 0), new QueueTopology("q2", 0));
            });

            Assert.Contains("At least one queue must provide an initial delay", ex.Message);

            ex = Assert.Throws<ArgumentException>(() =>
            {
                configuration.AddTopology<MultipleQueuesWithDelayAndRetryTopologyFactory>(
                    "mytopic", new QueueTopology("q1", -1), new QueueTopology("q2", 0));
            });

            Assert.Contains("initial delay must be positive or zero", ex.Message);
        }

        private string GetSingleQueueDlxName(IEnumerable<Exchange> exchanges,
            string exchangeName, string queueName = null)
            => (string)GetSingleQueueArgument(exchanges,
                exchangeName, queueName, "x-dead-letter-exchange");


        private int? GetSingleQueueTtl(IEnumerable<Exchange> exchanges,
            string exchangeName, string queueName = null)
            => (int?)GetSingleQueueArgument(exchanges,
                exchangeName, queueName, "x-message-ttl");

        private object GetSingleQueueArgument(IEnumerable<Exchange> exchanges,
            string exchangeName, string queueName, string argumentName)
        {
            if (queueName == null)
            {
                queueName = exchangeName;
            }

            var singleQueueArguments = exchanges
                .Single(e => e.Name == exchangeName)
                .Queues.Where(q => q.Name == queueName)
                .Select(q => q.Arguments)
                .Single();

            if (singleQueueArguments.ContainsKey(argumentName))
            {
                return singleQueueArguments[argumentName];
            }

            return null;
        }
    }
}
