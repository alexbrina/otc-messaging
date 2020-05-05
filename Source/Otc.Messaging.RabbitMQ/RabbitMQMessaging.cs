﻿using Microsoft.Extensions.Logging;
using Otc.Messaging.Abstractions;
using Otc.Messaging.RabbitMQ.Configurations;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;

namespace Otc.Messaging.RabbitMQ
{
    /// <summary>
    /// Provides a factory for RabbitMQ <see cref="IPublisher"/> and <see cref="ISubscription"/>.
    /// </summary>
    /// <remarks>
    /// It holds a connection with the broker that is supposed to be long-lived, i.e., this object
    /// instance may usually be registered as a singleton in your application life-cycle, and
    /// provide scoped instances of <see cref="IPublisher"/> and/or <see cref="ISubscription"/>.
    /// </remarks>
    public class RabbitMQMessaging : IMessaging
    {
        private readonly RabbitMQConfiguration configuration;
        private readonly ILoggerFactory loggerFactory;
        private readonly ILogger logger;
        private readonly ICollection<RabbitMQPublisher> publishers;
        private readonly ICollection<RabbitMQSubscription> subscriptions;

        private IConnection connection;
        private IConnection Connection
        {
            get
            {
                if (connection?.IsOpen ?? false)
                {
                    return connection;
                }

                var factory = new ConnectionFactory()
                {
                    HostName = configuration.Host,
                    Port = configuration.Port,
                    UserName = configuration.User,
                    Password = configuration.Password,
                    ClientProvidedName = nameof(RabbitMQ)
                };

                factory.AutomaticRecoveryEnabled = true;
                factory.NetworkRecoveryInterval = TimeSpan.FromSeconds(10);

                logger.LogInformation($"{nameof(Connection)}: Connecting to " +
                    $"{configuration.Host}:{configuration.Port} with user {configuration.User}");

                connection = factory.CreateConnection();
                return connection;
            }
        }

        public RabbitMQMessaging(
            RabbitMQConfiguration configuration,
            ILoggerFactory loggerFactory)
        {
            this.configuration = configuration ??
                throw new ArgumentNullException(nameof(configuration));

            this.loggerFactory = loggerFactory ??
                throw new ArgumentNullException(nameof(loggerFactory));

            logger = loggerFactory.CreateLogger<RabbitMQMessaging>();

            publishers = new List<RabbitMQPublisher>();

            subscriptions = new List<RabbitMQSubscription>();
        }

        /// <inheritdoc/>>
        public IPublisher CreatePublisher()
        {
            if (disposed)
            {
                throw new ObjectDisposedException(nameof(RabbitMQMessaging));
            }

            var (channel, channelEvents) = CreateChannel();

            var publisher = new RabbitMQPublisher(channel, channelEvents,
                configuration.PublishConfirmationTimeout, loggerFactory);

            publishers.Add(publisher);

            logger.LogInformation($"{nameof(CreatePublisher)}: Publisher created");

            return publisher;
        }

        /// <inheritdoc/>
        public ISubscription Subscribe(Action<IMessage> handler, params string[] queues)
        {
            if (disposed)
            {
                throw new ObjectDisposedException(nameof(RabbitMQMessaging));
            }

            var (channel, channelEvents) = CreateChannel();

            var subscription = new RabbitMQSubscription(channel, channelEvents, handler,
                configuration, loggerFactory, queues);

            subscriptions.Add(subscription);

            logger.LogInformation($"{nameof(Subscribe)}: Subscription created");

            return subscription;
        }

        /// <remarks>
        /// Applies a topology that was previously loaded in the configuration
        /// <see cref="RabbitMQConfiguration"/>.
        /// All Exchanges and it's queues and bindings will be declared via 
        /// ExchangeDeclare, QueueDeclare e QueueBind.
        /// It is not necessary to apply theses configurations all the time if
        /// your topology defines exchanges and queues as durables.
        /// </remarks>
        /// <inheritdoc/>
        public void EnsureTopology(string name)
        {
            var channel = Connection.CreateModel();

            configuration.EnsureTopology(name, channel);

            channel.Close();
            channel.Dispose();

            logger.LogInformation($"{nameof(EnsureTopology)}: Topology " +
                "{Topology} applied successfully!", name);
        }

        private (IModel, RabbitMQChannelEventsHandler) CreateChannel()
        {
            var channel = Connection.CreateModel();

            logger.LogInformation($"{nameof(CreateChannel)}: Channel " +
                $"{channel.ChannelNumber} created");

            channel.BasicQos(prefetchSize: 0, prefetchCount: configuration.PerQueuePrefetchCount,
                global: false);

            logger.LogDebug($"{nameof(CreateChannel)}: " +
                $"{nameof(configuration.PerQueuePrefetchCount)} set to " +
                $"{configuration.PerQueuePrefetchCount}");

            return (channel, new RabbitMQChannelEventsHandler(channel, loggerFactory));
        }

        private bool disposed = false;

        /// <summary>
        /// Disposing this object will dispose all subscriptions and publishers created by it.
        /// Connection will be closed too.
        /// </summary>
        public void Dispose()
        {
            if (!disposed)
            {
                logger.LogDebug($"{nameof(RabbitMQMessaging)}: Disposing ...");

                foreach (var publisher in publishers)
                {
                    publisher.Dispose();
                }
                publishers.Clear();

                foreach (var subscription in subscriptions)
                {
                    subscription.Dispose();
                }
                subscriptions.Clear();

                if (connection != null)
                {
                    if (connection.IsOpen)
                    {
                        connection.Close();
                    }

                    connection.Dispose();
                }

                disposed = true;

                logger.LogDebug($"{nameof(RabbitMQMessaging)}: Disposed.");
            }
        }
    }
}