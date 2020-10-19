using Otc.Messaging.RabbitMQ.Configurations;
using System;
using System.Collections.Generic;

namespace Otc.Messaging.RabbitMQ.PredefinedTopologies
{
    /// <summary>
    /// Create wait -> retry -> deadletter topology with initial delay
    /// </summary>
    public class SimpleQueueWithDelayAndRetryTopologyFactory : ITopologyFactory
    {
        /// <summary>
        /// Create topology
        /// </summary>
        /// <param name="mainExchangeName">
        ///     The main exchange name.
        /// </param>
        /// <param name="args">
        ///     1. First arg must be an int with time in milliseconds a message will be delayed
        ///         before moving to the main queue.
        ///     2. Next args are wait times in milliseconds for each retry queue, the number
        ///         of remaining entries in the array is the number of retry queues to be
        ///         configured.</param>
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
