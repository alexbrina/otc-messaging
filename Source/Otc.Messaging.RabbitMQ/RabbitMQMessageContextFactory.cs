using Otc.Messaging.Abstractions;
using Otc.Messaging.RabbitMQ.Configurations;
using RabbitMQ.Client.Events;
using System;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace Otc.Messaging.RabbitMQ
{
    public class RabbitMQMessageContextFactory : IDisposable
    {
        private readonly RabbitMQConfiguration configuration;
        private readonly Func<byte[], string, byte[]> hashableContent;
        private readonly Encoding encoding;
        private HashAlgorithm hashAlgorithm;

        private HashAlgorithm Hash
        {
            get
            {
                if (hashAlgorithm == null)
                {
                    hashAlgorithm = HashAlgorithm.Create(configuration.MessageHashAlgorithm.Name);
                }

                return hashAlgorithm;
            }
        }

        public RabbitMQMessageContextFactory(RabbitMQConfiguration configuration,
            Func<byte[], string, byte[]> hashableContent = null,
            Encoding encoding = null)
        {
            this.configuration = configuration
                ?? throw new ArgumentNullException(nameof(configuration));

            this.hashableContent = hashableContent ?? GetHashableContent;

            this.encoding = encoding ?? Encoding.UTF8;
        }

        public IMessageContext Create(BasicDeliverEventArgs ea, string queue,
            CancellationToken cancellationToken)
        {
            return new RabbitMQMessageContext(
                ea.BasicProperties.MessageId,
                DateTimeOffset.FromUnixTimeMilliseconds(ea.BasicProperties.Timestamp.UnixTime),
                ea.Exchange,
                queue,
                ea.Redelivered,
                CalculateMessageHash(ea, queue),
                cancellationToken
            );
        }

        private string CalculateMessageHash(BasicDeliverEventArgs ea, string queue)
        {
            if (configuration.CalculateMessageHash)
            {
                var content = hashableContent.Invoke(ea.Body.ToArray(), queue);
                var hashed = Hash.ComputeHash(content);
                return Convert.ToBase64String(hashed);
            }
            else
            {
                return "";
            }
        }

        private byte[] GetHashableContent(byte[] message, string queueName)
        {
            // Default content used for hash calculation is queue name plus message body

            var queue = encoding.GetBytes(queueName);

            var content = new byte[queue.Length + message.Length];
            Buffer.BlockCopy(queue, 0, content, 0, queue.Length);
            Buffer.BlockCopy(message, 0, content, queue.Length, message.Length);
            return content;
        }

        private bool disposed = false;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
            {
                return;
            }

            if (disposing)
            {
                hashAlgorithm.Dispose();
            }

            disposed = true;
        }
    }
}