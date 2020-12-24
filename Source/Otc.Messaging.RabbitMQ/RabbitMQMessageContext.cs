using Otc.Messaging.Abstractions;
using Otc.Messaging.RabbitMQ.Configurations;
using System;
using System.Threading;

namespace Otc.Messaging.RabbitMQ
{
    /// <summary>
    /// Provides context information about a message received from broker.
    /// It is constructed right after a message arrives and is passed 
    /// to the registered handler. All properties are readonly.
    /// </summary>
    public class RabbitMQMessageContext : IMessageContext
    {
        public RabbitMQMessageContext(string id, DateTimeOffset timestamp, string topic,
            string queue, bool redelivered, string hash, CancellationToken cancellationToken)
        {
            Id = id;
            Timestamp = timestamp;
            Topic = topic;
            Queue = queue;
            Redelivered = redelivered;
            Hash = hash;
            CancellationToken = cancellationToken;
        }

        /// <inheritdoc/>
        public string Id { get; }

        /// <inheritdoc/>
        public DateTimeOffset Timestamp { get; }

        /// <inheritdoc/>
        public string Topic { get; }

        /// <inheritdoc/>
        public string Queue { get; }

        /// <inheritdoc/>
        public bool Redelivered { get; }

        /// <inheritdoc/>
        /// <see cref="RabbitMQConfiguration.CalculateMessageHash"/>
        public string Hash { get; }

        /// <inheritdoc/>
        public CancellationToken CancellationToken { get; }
    }
}