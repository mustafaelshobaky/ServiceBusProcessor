using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using System.Text.Json;

namespace ServiceBusProcessor;

class Program
{
    private class Secrets
    {
        public string SourceConnectionString { get; set; } = string.Empty;
        public string DestinationConnectionString { get; set; } = string.Empty;
        public string DestinationQueueName { get; set; } = string.Empty;
    }

    static async Task Main(string[] args)
    {
        Console.WriteLine("Service Bus Processor");
        Console.WriteLine("Usage:");
        Console.WriteLine("  (no args)         - Forward messages from _error queue to destination queue");
        Console.WriteLine("  purge [hours]     - Purge dead letters older than N hours (default: 24)");
        Console.WriteLine();

        if (args.Length > 0 && args[0].Equals("purge", StringComparison.OrdinalIgnoreCase))
        {
            var cutoffHours = 24;
            if (args.Length > 1 && int.TryParse(args[1], out var parsedHours))
                cutoffHours = parsedHours;

            await PurgeDeadLettersAsync(cutoffHours);
        }
        else
        {
            await ForwardErrorMessagesAsync();
        }
    }

    static async Task ForwardErrorMessagesAsync()
    {
        Console.WriteLine("Service Bus Processor starting...");

        var secrets = LoadSecrets();
        if (secrets == null || string.IsNullOrEmpty(secrets.SourceConnectionString) ||
            string.IsNullOrEmpty(secrets.DestinationConnectionString) ||
            string.IsNullOrEmpty(secrets.DestinationQueueName))
        {
            Console.WriteLine("Error: Invalid secrets.json configuration");
            return;
        }

        var destinationQueueName = secrets.DestinationQueueName;
        var sourceQueueName = $"{destinationQueueName}_error";

        var sourceClient = new ServiceBusClient(secrets.SourceConnectionString);
        var destinationClient = new ServiceBusClient(secrets.DestinationConnectionString);
        var sender = destinationClient.CreateSender(destinationQueueName);

        var processorOptions = new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 1,
            AutoCompleteMessages = false,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(5)
        };

        var processor = sourceClient.CreateProcessor(sourceQueueName, processorOptions);

        processor.ProcessMessageAsync += async args =>
        {
            try
            {
                var message = args.Message;
                Console.WriteLine($"Received a message");

                var newMessage = new ServiceBusMessage(message.Body)
                {
                    ContentType = message.ContentType,
                    CorrelationId = message.CorrelationId,
                    MessageId = message.MessageId,
                    Subject = message.Subject
                };

                foreach (var prop in message.ApplicationProperties)
                    newMessage.ApplicationProperties.Add(prop.Key, prop.Value);

                await sender.SendMessageAsync(newMessage);
                Console.WriteLine($"Forwarded message to {destinationQueueName}");
                await args.CompleteMessageAsync(message);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing message: {ex.Message}");
                await args.AbandonMessageAsync(args.Message);
            }
        };

        processor.ProcessErrorAsync += args =>
        {
            Console.WriteLine($"Error occurred: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        try
        {
            await processor.StartProcessingAsync();
            Console.WriteLine("Processing messages. Press any key to stop...");
            Console.ReadKey();
            await processor.StopProcessingAsync();
            Console.WriteLine("Processing stopped");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error running processor: {ex.Message}");
        }
        finally
        {
            await processor.DisposeAsync();
            await sender.DisposeAsync();
            await sourceClient.DisposeAsync();
            await destinationClient.DisposeAsync();
        }
    }

    static async Task PurgeDeadLettersAsync(int cutoffHours)
    {
        var cutoffTime = DateTimeOffset.UtcNow.AddHours(-cutoffHours);
        Console.WriteLine("=== Azure Service Bus Dead Letter Purger ===");
        Console.WriteLine($"Namespace  : curenta-messaging-live");
        Console.WriteLine($"Cutoff     : {cutoffTime:yyyy-MM-dd HH:mm:ss} UTC (older than {cutoffHours}h)");
        Console.WriteLine();

        var secrets = LoadSecrets();
        if (secrets == null || string.IsNullOrEmpty(secrets.SourceConnectionString))
        {
            Console.WriteLine("Error: Invalid secrets.json — missing SourceConnectionString");
            return;
        }

        var adminClient = new ServiceBusAdministrationClient(secrets.SourceConnectionString);
        await using var client = new ServiceBusClient(secrets.SourceConnectionString);

        var totalPurged = 0;
        var totalSkipped = 0;
        var queuesProcessed = 0;

        await foreach (var props in adminClient.GetQueuesRuntimePropertiesAsync())
        {
            if (props.DeadLetterMessageCount == 0)
                continue;

            queuesProcessed++;
            Console.WriteLine($"[{queuesProcessed}] {props.Name}");
            Console.WriteLine($"     DLQ count : {props.DeadLetterMessageCount}");

            var receiver = client.CreateReceiver(props.Name, new ServiceBusReceiverOptions
            {
                SubQueue = SubQueue.DeadLetter,
                ReceiveMode = ServiceBusReceiveMode.PeekLock
            });

            var queuePurged = 0;
            var queueSkipped = 0;

            try
            {
                while (true)
                {
                    var messages = await receiver.ReceiveMessagesAsync(
                        maxMessages: 100,
                        maxWaitTime: TimeSpan.FromSeconds(2));

                    if (messages.Count == 0)
                        break;

                    var oldMessages = messages.Where(m => m.EnqueuedTime < cutoffTime).ToList();
                    var newMessages = messages.Where(m => m.EnqueuedTime >= cutoffTime).ToList();

                    var completeTasks = oldMessages.Select(async m =>
                    {
                        try
                        {
                            await receiver.CompleteMessageAsync(m);
                            Interlocked.Increment(ref queuePurged);
                            Interlocked.Increment(ref totalPurged);
                        }
                        catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessageLockLost)
                        {
                            // lock expired; message will resurface in a future receive batch
                        }
                    });

                    var abandonTasks = newMessages.Select(async m =>
                    {
                        try
                        {
                            await receiver.AbandonMessageAsync(m);
                            Interlocked.Increment(ref queueSkipped);
                            Interlocked.Increment(ref totalSkipped);
                        }
                        catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessageLockLost)
                        {
                            // lock expired; message will resurface in a future receive batch
                        }
                    });

                    await Task.WhenAll(completeTasks.Concat(abandonTasks));

                    if (newMessages.Count > 0)
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"     ERROR : {ex.Message}");
            }
            finally
            {
                await receiver.DisposeAsync();
            }

            Console.WriteLine($"     Purged: {queuePurged}  |  Kept (< {cutoffHours}h): {queueSkipped}");
            Console.WriteLine();
        }

        Console.WriteLine("=== SUMMARY ===");
        Console.WriteLine($"Queues processed : {queuesProcessed}");
        Console.WriteLine($"Total purged     : {totalPurged}");
        Console.WriteLine($"Total kept       : {totalSkipped}");
    }

    static Secrets? LoadSecrets()
    {
        var secretsPath = Path.Combine(AppContext.BaseDirectory, "secrets.json");
        if (!File.Exists(secretsPath))
        {
            Console.WriteLine($"Error: secrets.json not found at {secretsPath}");
            return null;
        }

        var secretsJson = File.ReadAllText(secretsPath);
        return JsonSerializer.Deserialize<Secrets>(secretsJson);
    }
}
