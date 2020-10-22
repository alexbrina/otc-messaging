using Otc.Messaging.RabbitMQ.Configurations;
using System;
using System.Collections.Generic;

namespace Otc.Messaging.RabbitMQ.PredefinedTopologies
{
    /// <inheritdoc cref="Create(string, object[])" />
    public class SimpleQueueWithDelayAndRetryTopologyFactory : ITopologyFactory
    {
        /// <summary>
        /// Create a topology with one exchange and one main queue, both having the same name.
        /// You can optionally set retry queues by defining wait times.
        /// For this topology you must define an initial delay that a message will wait before
        /// being moved to main queue.
        /// </summary>
        /// <remarks>
        /// Argument <c>object[]</c> of method <see cref="Create(string, object[])"/>
        /// must be passed in specific order:
        /// <br />1. First arg must be an int defining the time in milliseconds a message will be
        ///         delayed before moving to the main queue.
        /// <br />2. Next args are optional, defining wait times in milliseconds for each retry
        ///         queue, the number of remaining entries in the array is the number of retry
        ///         queues to be configured.
        /// </remarks>
        /// <param name="mainExchangeName">
        ///     The main exchange name.
        /// </param>
        /// <param name="args">
        ///     See remarks above.
        /// </param>
        /// <returns>The created topology</returns>
        public Topology Create(string mainExchangeName, params object[] args)
        {
            if (mainExchangeName is null)
            {
                throw new ArgumentNullException(nameof(mainExchangeName));
            }

            if (args.Length < 1)
            {
                throw new ArgumentException("Must provide delay in milliseconds " +
                    "as the first element of array.", nameof(args));
            }

            var arguments = new Queue<object>(args);

            int delayMilliseconds;
            try
            {
                delayMilliseconds = (int)arguments.Dequeue();
                
            }
            catch (InvalidCastException e)
            {
                throw new ArgumentException("All elements must be of type int.",
                    nameof(args), e);
            }

            if (delayMilliseconds <= 0)
            {
                throw new ArgumentException("Delay must be positive and greater " +
                    "than zero.", nameof(args));
            }

            var exchange = new Exchange()
            {
                Name = mainExchangeName,
                Queues = new List<Queue>
                {
                    new Queue()
                    {
                        Name = mainExchangeName + "-delay",
                        Arguments = new Dictionary<string, object>()
                        {
                            { "x-dead-letter-exchange", mainExchangeName + "-delay" },
                            { "x-message-ttl", delayMilliseconds }
                        }
                    }
                }
            };

            var delayExchange = new Exchange()
            {
                Name = mainExchangeName + "-delay"
            };

            var exchanges = new List<Exchange>
            {
                exchange,
                delayExchange
            };

            var queueRetryPackBuilder = new QueueRetryPackBuilder();

            exchanges.AddRange(
                queueRetryPackBuilder.Create(delayExchange, mainExchangeName, arguments.ToArray()));

            return new Topology()
            {
                Exchanges = exchanges
            };
        }
    }
}
