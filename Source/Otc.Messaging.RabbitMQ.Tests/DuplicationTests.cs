using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Otc.Extensions.Configuration;
using Otc.Messaging.Abstractions;
using Otc.Messaging.RabbitMQ.Configurations;
using System;
using System.IO;
using System.Text;
using System.Threading;
using Xunit;
using Xunit.Abstractions;

namespace Otc.Messaging.RabbitMQ.Tests
{
    /// <summary>
    /// This class tests publishing and consumption duplication scenarios.
    /// </summary>
    public class DuplicationTests
    {
        private ITestOutputHelper OutputHelper { get; }
        private readonly IServiceProvider serviceProvider;
        private readonly RabbitMQConfiguration configuration;

        public DuplicationTests(ITestOutputHelper output)
        {
            OutputHelper = output;

            IServiceCollection services = new ServiceCollection();

            var configurationBuilder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false)
                .Build();
            
            configuration = configurationBuilder.SafeGet<RabbitMQConfiguration>();

            serviceProvider = services
                .AddLogging(b => b.AddXUnit(OutputHelper).SetMinimumLevel(LogLevel.Trace))
                .AddRabbitMQ(configuration)
                .BuildServiceProvider();

            var bus = serviceProvider.GetService<IMessaging>();
            bus.EnsureTopology("DuplicationTests");
        }

        /// <summary>
        /// Simulates same message being published multiple times to the main topic
        /// 
        /// This cenario can occur, for instance, when publisher do not receive ack signal
        /// from broker (due to connection problems), even though the message was correctly 
        /// delivered to designated queue, and then publisher eventually publishes same message 
        /// again.
        /// 
        /// <see cref="DuplicationMessageHandler"/>
        /// </summary>
        [Fact]
        public void Test_Publish_Duplicated_Message_Main_Queue()
        {
            using (var bus = serviceProvider.GetService<IMessaging>())
            {
                var pub = bus.CreatePublisher();
                var msg = $"Message Published";
               
                // publish message
                pub.Publish(Encoding.UTF8.GetBytes(msg), "test-dup-1");

                // republishing same message
                pub.Publish(Encoding.UTF8.GetBytes(msg), "test-dup-1");

                var handler = new DuplicationMessageHandler
                { OutputHelper = OutputHelper, Configuration = configuration };

                var sub = bus.Subscribe(handler.Handle, "test-dup-1", "test-dup-1-retry-1");

                sub.Start();
                Thread.Sleep(2000);
                sub.Stop();

                Assert.Single(handler.Duplications);
                Assert.Single(handler.Hashes);
                Assert.Single(handler.CommitedMessages);
            }
        }

        /// <summary>
        /// Simulates a message received from main queue and then rejected by the consumer, it
        /// may occur for whatever reason, like temporary outage of a database or other dependency,
        /// but then it is accepted in the first retry (if retries are configured).
        /// 
        /// <see cref="DuplicationMessageHandler"/>
        /// </summary>
        [Fact]
        public void Test_Publish_Duplicated_Message_Retry_Queue()
        {
            using (var bus = serviceProvider.GetService<IMessaging>())
            {
                var pub = bus.CreatePublisher();

                var msg1 = $"Message For Retry. Will be rejected when arriving from main queue.";
                // publish message for retry
                pub.Publish(Encoding.UTF8.GetBytes(msg1), "test-dup-2");
                // republishing same message 3x
                pub.Publish(Encoding.UTF8.GetBytes(msg1), "test-dup-2");
                pub.Publish(Encoding.UTF8.GetBytes(msg1), "test-dup-2");
                pub.Publish(Encoding.UTF8.GetBytes(msg1), "test-dup-2");

                var msg2 = $"Normal Message. Won't be rejected.";
                // publish message for normal processing
                pub.Publish(Encoding.UTF8.GetBytes(msg2), "test-dup-2");
                // republishing same message once
                pub.Publish(Encoding.UTF8.GetBytes(msg2), "test-dup-2");

                var handler = new DuplicationMessageHandler
                { OutputHelper = OutputHelper, Configuration = configuration };

                var sub = bus.Subscribe(handler.Handle, "test-dup-2", "test-dup-2-retry-1");

                sub.Start();
                Thread.Sleep(15000);
                sub.Stop();

                // 4 * M1 = 3 Duplications
                // 2 * M2 = 1 Duplication
                // Total 4 duplications
                Assert.Equal(4, handler.Duplications.Count);

                // M1 received from 2 queues (main and retry-1)
                // M2 received from 1 queue (main only)
                // Total 3 hashes.
                Assert.Equal(3, handler.Hashes.Count);

                // M1 accepted from retry-1
                // M2 accepted from main
                // Total 2 different messages accepted
                Assert.Equal(2, handler.CommitedMessages.Count);
            }
        }

        /// <summary>
        /// Simulates a connection failure while message is delivered but still unacked, but 
        /// consumer already processed message successfully.
        /// 
        /// Any message delivered and, not acked or nacked, and which consumer channel/connection
        /// is closed, will be put back in the queue to be redelivered.
        /// 
        /// This cenario can occur, for instance, when subscriber receives and process a message
        /// successfully, sends back ack signal, but broker DOES NOT receive it due to network
        /// faillure. Note that, at this point, consumer may have already changed other system state.
        /// 
        /// Another scenario, more susceptible, would be the connection being closed, by broker or
        /// any other reason, while message is being handled. In this case, the library code will
        /// detect that channel is closed and won't send ack or nack signal. At this point, not 
        /// even the consumer may have changed other states, but the message may be already 
        /// redelivered to some other availlable consumer.
        /// 
        /// <see cref="DuplicationMessageHandler"/>
        /// </summary>
        [Fact]
        public void Test_Subscription_Connection_Lost_Successful_Message()
        {
            var config = GetConfiguration();
            config.ClientProvidedName = "otc-messaging-tests-dup-3";

            using (var bus = new RabbitMQMessaging(config,
                new RabbitMQMessageContextFactory(config),
                serviceProvider.GetService<ILoggerFactory>()))
            {
                var pub = bus.CreatePublisher();
                var msg = $"Message Published";

                pub.Publish(Encoding.UTF8.GetBytes(msg), "test-dup-3");

                var handler = new DuplicationMessageHandler
                { OutputHelper = OutputHelper, Configuration = config };

                using (var sub = bus.Subscribe(handler.Handle, "test-dup-3", "test-dup-3-retry-1"))
                {
                    sub.Start();
                    Thread.Sleep(1000);
                    sub.Stop();
                }

                // At this point, message was processed and NO duplication happened
                // BUT ack was not sent back to the broker due to connection failure
                Assert.Empty(handler.Duplications);
                Assert.Single(handler.Hashes);
                Assert.Single(handler.CommitedMessages);

                // some subscriber connects and starts consuming these queues
                using (var sub = bus.Subscribe(handler.Handle, "test-dup-3", "test-dup-3-retry-1"))
                {
                    sub.Start();
                    Thread.Sleep(500);
                    sub.Stop();
                }

                // At this point, message was redelivered, and duplication is detected,
                // hence no hash or message was added
                Assert.Single(handler.Duplications);
                Assert.Single(handler.Hashes);
                Assert.Single(handler.CommitedMessages);
            }
        }

        /// <summary>
        /// Simulates a connection failure while message is delivered but still unacked, but 
        /// then consumer fail to process message and a nack signal is NOT sent back.
        /// 
        /// This scenario, imposes that the overall deduplication logic must take in account some
        /// evidence belonging to the use case (checking some domain state) to inform if given
        /// message have already been processed successfully or not.
        /// 
        /// Otherwise, if deduplication logic consider only message content and queue, it will
        /// falselly take this message as duplicated while it is not.
        /// 
        /// <see cref="DuplicationMessageHandler"/>
        /// </summary>
        [Fact]
        public void Test_Subscription_Connection_Lost_Failed_Message()
        {
            var config = GetConfiguration();
            config.ClientProvidedName = "otc-messaging-tests-dup-4";

            using (var bus = new RabbitMQMessaging(config,
                new RabbitMQMessageContextFactory(config),
                serviceProvider.GetService<ILoggerFactory>()))
            {
                var pub = bus.CreatePublisher();
                var msg = $"Message Published";

                pub.Publish(Encoding.UTF8.GetBytes(msg), "test-dup-4");

                var handler = new DuplicationMessageHandler
                { OutputHelper = OutputHelper, Configuration = config };

                using (var sub = bus.Subscribe(handler.Handle, "test-dup-4", "test-dup-4-retry-1"))
                {
                    sub.Start();
                    Thread.Sleep(1000);
                    sub.Stop();
                }

                // At this point, message was received BUT failled to process and an exception was
                // thown, BUT connection was lost and NO nack was sent. NO duplication happened yet.
                Assert.Empty(handler.Duplications);
                Assert.Single(handler.Hashes);
                Assert.Empty(handler.CommitedMessages);

                // another consumer comes in and receives same message as it was not nacked in first delivery
                using (var sub = bus.Subscribe(handler.Handle, "test-dup-4", "test-dup-4-retry-1"))
                {
                    sub.Start();
                    Thread.Sleep(500);
                    sub.Stop();
                }

                // BUT then duplication is detected and message is WRONGLY discarded, SINCE it was
                // not commited in first attempt
                Assert.Single(handler.Duplications);
                Assert.Single(handler.Hashes);
                Assert.Empty(handler.CommitedMessages);

                // WE LEARN THEN THAT HASH ADDING AND MESSAGE PROCESSING MUST BE AN ATOMIC OPERATION
                // OR MAYBE NO SEPARATE HASH TRACKING, AND USE MESSAGE STORAGE ITSELF TO CHECK DUPLICATION
            }
        }

        private RabbitMQConfiguration GetConfiguration()
        {
            return new RabbitMQConfiguration()
            {
                Hosts = configuration.Hosts,
                Port = configuration.Port,
                VirtualHost = configuration.VirtualHost,
                User = configuration.User,
                Password = configuration.Password,
                PerQueuePrefetchCount = 10,
                MessageHandlerErrorBehavior = MessageHandlerErrorBehavior.RejectOnFistDelivery
            };
        }
    }
}