﻿using Otc.Messaging.Abstractions;
using RabbitMQ.Client.Events;
using System;
using System.Threading;

namespace Otc.Messaging.RabbitMQ
{
    /// <summary>
    /// Provides a message received from broker.
    /// It is constructed right after a message arrives and is passed 
    /// to the registered handler. All properties are readonly.
    /// </summary>
    public class RabbitMQMessageContext : IMessageContext
    {
        /// <summary>
        /// Creates a new <see cref="RabbitMQMessageContext"/>.
        /// </summary>
        /// <param name="ea">Message's metadata sent by the broker.</param>
        /// <param name="queue">The queue it came from.</param>
        /// <param name="cancellationToken">The token for async cancellation.</param>
        public RabbitMQMessageContext(
            BasicDeliverEventArgs ea, string queue, CancellationToken cancellationToken)
        {
            Id = ea.BasicProperties.MessageId;
            Timestamp = DateTimeOffset
                .FromUnixTimeMilliseconds(ea.BasicProperties.Timestamp.UnixTime);
            Topic = ea.Exchange;
            Queue = queue;
            Redelivered = ea.Redelivered;
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
        public CancellationToken CancellationToken { get; }
    }
}