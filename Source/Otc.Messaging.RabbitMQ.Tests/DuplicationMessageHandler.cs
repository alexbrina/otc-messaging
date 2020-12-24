using Newtonsoft.Json.Linq;
using Otc.Messaging.Abstractions;
using Otc.Messaging.RabbitMQ.Configurations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace Otc.Messaging.RabbitMQ.Tests
{
    public class DuplicationMessageHandler
    {
        public ITestOutputHelper OutputHelper;
        public IList<string> CommitedMessages = new List<string>();
        public IList<string> Duplications = new List<string>();
        public IList<string> Hashes = new List<string>();
        public RabbitMQConfiguration Configuration;

        public void Handle(byte[] message, IMessageContext messageContext)
        {
            var text = Encoding.UTF8.GetString(message);

            // if duplicated, returns without processing
            if (IsDuplicated(text, messageContext)) return;

            /* ----------------------------- Main Service Start ----------------------------- */

            if (text.Contains("For Retry") && !messageContext.Queue.Contains("retry"))
            {
                OutputHelper?.WriteLine($"{nameof(DuplicationMessageHandler)}: " +
                    $"Queue: {messageContext.Queue} - Id: {messageContext.Id} - " +
                    $"MESSAGE FOR RETRY!");

                throw new InvalidOperationException("This message is for retry");
            }

            // simulates a handling failure and connection failure (before nack)
            // second attempt message duplication will be WRONGLY detected
            if (Configuration.ClientProvidedName.Contains("tests-dup-4")
                && !messageContext.Redelivered)
            {
                DropConnection(Configuration.ClientProvidedName);
                throw new InvalidOperationException("This message could be dlxed to retry, but " +
                    "will stay in main queue since it was not nacked.");
            }

            // this correspond to a commit transaction
            CommitedMessages.Add(text);

            /* ----------------------------- Main Service End ----------------------------- */

            // simulates handling succedded but connection failed (before ack)
            // on second attempt (redelivery) the message duplication will be detected
            if (Configuration.ClientProvidedName.Contains("tests-dup-3")
                && !messageContext.Redelivered)
            {
                DropConnection(Configuration.ClientProvidedName);
            }

            OutputHelper?.WriteLine($"{nameof(DuplicationMessageHandler)}: " +
                $"Queue: {messageContext.Queue} - Id: {messageContext.Id} - " +
                $"GOOD MESSAGE!");
        }

        private bool IsDuplicated(string text, IMessageContext messageContext)
        {
            if (Hashes.Contains(messageContext.Hash))
            {
                Duplications.Add(text);

                OutputHelper?.WriteLine($"{nameof(DuplicationMessageHandler)}: " +
                    $"Queue: {messageContext.Queue} - Id: {messageContext.Id} - " +
                    $"DUPLICATION DETECTED - MESSAGE IGNORED!");
                return true;
            }
            Hashes.Add(messageContext.Hash);
            return false;
        }

        private void DropConnection(string connectionId)
        {
            Thread.Sleep(5000);
            var httpClient = GetHttpClient();
            var connectionName = GetConnectionName(connectionId, httpClient).GetAwaiter().GetResult();
            DeleteConnection(connectionName, httpClient).GetAwaiter().GetResult();
        }

        private HttpClient GetHttpClient()
        {
            var credentials = Convert.ToBase64String(
                Encoding.UTF8.GetBytes($"{Configuration.User}:{Configuration.Password}"));

            var uri = $"http://{Configuration.Hosts[0]}:15672/api/";

            var client = new HttpClient();

            client.BaseAddress = new Uri(uri);
            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Basic", credentials);

            return client;
        }

        private async Task<string> GetConnectionName(string connectionId, HttpClient httpClient)
        {
            var content = await httpClient.GetStringAsync(httpClient.BaseAddress + "connections");
            var json = JArray.Parse(content);
            return json.Where(s => s["user_provided_name"].ToString() == connectionId)
                .Select(s => s["name"].ToString()).FirstOrDefault();
        }

        private async Task DeleteConnection(string connectionName, HttpClient httpClient)
        {
            await httpClient.DeleteAsync(httpClient.BaseAddress + "connections/" + connectionName);
        }
    }
}
