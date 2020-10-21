using Otc.Messaging.RabbitMQ.Configurations;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;

namespace Otc.Messaging.RabbitMQ.PredefinedTopologies
{
    /// <summary>
    /// Create wait -> retry -> deadletter topology
    /// </summary>
    public class MultipleQueuesWithDelayAndRetryTopologyFactory : ITopologyFactory
    {
        /// <summary>
        /// Create topology
        /// </summary>
        /// <param name="mainExchangeName">
        ///     The main exchange name.
        /// </param>
        /// <param name="args">
        ///     Array of <see cref="QueueTopology"/> to send published messages.
        /// </param>
        /// <returns>The created topology</returns>
        public Topology Create(string mainExchangeName, params object[] args)
        {
            if (mainExchangeName is null)
            {
                throw new ArgumentNullException(nameof(mainExchangeName));
            }

            var queues = ReadArguments(args);

            var exchange = new Exchange()
            {
                Name = mainExchangeName,
                Type = ExchangeType.Fanout
            };

            var exchanges = new List<Exchange>
            {
                exchange
            };

            var queueRetryPackBuilder = new QueueRetryPackBuilder();

            foreach (var queue in queues)
            {
                if (queue.DelayMilliseconds > 0)
                {
                    exchange.Queues.Add(new Queue()
                    {
                        Name = queue.Name + "-delay",
                        Arguments = new Dictionary<string, object>()
                        {
                            { "x-dead-letter-exchange", queue.Name + "-delay" },
                            { "x-message-ttl", queue.DelayMilliseconds }
                        }
                    });

                    var delayExchange = new Exchange()
                    {
                        Name = queue.Name + "-delay"
                    };

                    exchanges.Add(delayExchange);

                    exchanges.AddRange(
                        queueRetryPackBuilder.Create(
                            delayExchange, queue.Name, queue.RetryMilliseconds));
                }
                else
                {
                    exchanges.AddRange(
                        queueRetryPackBuilder.Create(
                            exchange, queue.Name, queue.RetryMilliseconds));
                }
            }

            return new Topology()
            {
                Exchanges = exchanges
            };
        }

        private IEnumerable<QueueTopology> ReadArguments(
            params object[] args)
        {
            if (args is null)
            {
                throw new ArgumentNullException(nameof(args));
            }

            var queues = new List<QueueTopology>();
            var noQueueHasDelay = true;

            foreach (var item in args)
            {
                if (item is QueueTopology queue)
                {
                    queues.Add(queue);

                    if (queue.DelayMilliseconds < 0)
                    {
                        throw new ArgumentException("Queue initial delay must be " +
                            "positive or zero.", nameof(args));
                    }

                    if (noQueueHasDelay && queue.DelayMilliseconds > 0)
                    {
                        noQueueHasDelay = false;
                    }
                }
                else
                {
                    throw new ArgumentException(
                        $"All elements must be of type {nameof(QueueTopology)}.", nameof(args));
                }
            }

            if (queues.Count < 2)
            {
                throw new ArgumentException(
                    "A minimum of 2 queues must be provided " +
                    "to use this predefined topology.", nameof(args));
            }

            if (noQueueHasDelay)
            {
                throw new ArgumentException(
                    "At least one queue must provide an initial delay " +
                    "to use this predefined topology.", nameof(args));
            }

            return queues;
        }
    }
}
