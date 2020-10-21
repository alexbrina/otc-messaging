using Otc.Messaging.RabbitMQ.Configurations;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Otc.Messaging.RabbitMQ.PredefinedTopologies.Tests
{
    public class SimpleQueueWithDelayAndRetryTopologyTests
    {
        [Fact]
        public void TwoRetries_Success()
        {
            var rabbitMQConfiguration = new RabbitMQConfiguration();

            rabbitMQConfiguration.AddTopology<SimpleQueueWithDelayAndRetryTopologyFactory>(
                "mytopic", 1000, 4000, 8000);

            var exchanges = rabbitMQConfiguration.Topologies["mytopic"].Exchanges;

            Assert.Equal("mytopic-delay", GetSingleQueueDlxName(exchanges, "mytopic"));
            Assert.Equal("mytopic-wait-0", GetSingleQueueDlxName(exchanges, "mytopic-delay"));
            Assert.Equal("mytopic-retry-0", GetSingleQueueDlxName(exchanges, "mytopic-wait-0"));
            Assert.Equal("mytopic-wait-1", GetSingleQueueDlxName(exchanges, "mytopic-retry-0"));
            Assert.Equal("mytopic-retry-1", GetSingleQueueDlxName(exchanges, "mytopic-wait-1"));
            Assert.Equal("mytopic-dead", GetSingleQueueDlxName(exchanges, "mytopic-retry-1"));

            Assert.Equal(1000, GetSingleQueueTtl(exchanges, "mytopic"));
            Assert.Equal(4000, GetSingleQueueTtl(exchanges, "mytopic-wait-0"));
            Assert.Equal(8000, GetSingleQueueTtl(exchanges, "mytopic-wait-1"));
        }

        [Fact]
        public void SingleRetry_Success()
        {
            var rabbitMQConfiguration = new RabbitMQConfiguration();

            rabbitMQConfiguration.AddTopology<SimpleQueueWithDelayAndRetryTopologyFactory>(
                "mytopic", 1000, 4000);

            var exchanges = rabbitMQConfiguration.Topologies["mytopic"].Exchanges;

            Assert.Equal("mytopic-delay", GetSingleQueueDlxName(exchanges, "mytopic"));
            Assert.Equal("mytopic-wait-0", GetSingleQueueDlxName(exchanges, "mytopic-delay"));
            Assert.Equal("mytopic-retry-0", GetSingleQueueDlxName(exchanges, "mytopic-wait-0"));
            Assert.Equal("mytopic-dead", GetSingleQueueDlxName(exchanges, "mytopic-retry-0"));

            Assert.Equal(1000, GetSingleQueueTtl(exchanges, "mytopic"));
            Assert.Equal(4000, GetSingleQueueTtl(exchanges, "mytopic-wait-0"));
        }

        [Fact]
        public void NoRetry_Success()
        {
            var rabbitMQConfiguration = new RabbitMQConfiguration();

            rabbitMQConfiguration.AddTopology<SimpleQueueWithDelayAndRetryTopologyFactory>(
                "mytopic", 1000);

            var exchanges = rabbitMQConfiguration.Topologies["mytopic"].Exchanges;

            Assert.Equal("mytopic-delay", GetSingleQueueDlxName(exchanges, "mytopic"));
            Assert.Equal("mytopic-dead", GetSingleQueueDlxName(exchanges, "mytopic-delay"));

            Assert.Equal(1000, GetSingleQueueTtl(exchanges, "mytopic"));
        }

        [Fact]
        public void Arguments_Failures()
        {
            var rabbitMQConfiguration = new RabbitMQConfiguration();

            Exception ex;

            ex = Assert.Throws<ArgumentException>(() =>
            {
                rabbitMQConfiguration.AddTopology<SimpleQueueWithDelayAndRetryTopologyFactory>(
                    "mytopic");
            });

            Assert.Contains("Must provide delay", ex.Message);

            ex = Assert.Throws<ArgumentException>(() =>
            {
                rabbitMQConfiguration.AddTopology<SimpleQueueWithDelayAndRetryTopologyFactory>(
                    "mytopic", "not-number");
            });

            Assert.Contains("must be of type int", ex.Message);

            ex = Assert.Throws<ArgumentException>(() =>
            {
                rabbitMQConfiguration.AddTopology<SimpleQueueWithDelayAndRetryTopologyFactory>(
                    "mytopic", 0);
            });

            Assert.Contains("must be positive and greater than zero", ex.Message);

            ex = Assert.Throws<ArgumentException>(() =>
            {
                rabbitMQConfiguration.AddTopology<SimpleQueueWithDelayAndRetryTopologyFactory>(
                    "mytopic", -1);
            });

            Assert.Contains("must be positive and greater than zero", ex.Message);
        }

        private string GetSingleQueueDlxName(IEnumerable<Exchange> exchanges, string topicName)
            => (string)GetSingleQueueArgument(exchanges, topicName, "x-dead-letter-exchange");
        

        private int GetSingleQueueTtl(IEnumerable<Exchange> exchanges, string topicName)
            => (int)GetSingleQueueArgument(exchanges, topicName, "x-message-ttl");

        private object GetSingleQueueArgument(IEnumerable<Exchange> exchanges, 
            string topicName, string argumentName)
        {
            var singleQueueArguments = exchanges
                .Single(e => e.Name == topicName)
                .Queues.Select(q => q.Arguments)
                .Single();

            if (singleQueueArguments.ContainsKey(argumentName))
            {
                return singleQueueArguments[argumentName];
            }

            return null;
        }
    }
}
