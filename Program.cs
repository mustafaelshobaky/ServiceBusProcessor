using Azure.Messaging.ServiceBus;
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
        Console.WriteLine("Service Bus Processor starting...");

        var secretsPath = Path.Combine(AppContext.BaseDirectory, "secrets.json");
        if (!File.Exists(secretsPath))
        {
            Console.WriteLine($"Error: secrets.json not found at {secretsPath}");
            return;
        }

        var secretsJson = await File.ReadAllTextAsync(secretsPath);
        var secrets = JsonSerializer.Deserialize<Secrets>(secretsJson);
        
        if (secrets == null || string.IsNullOrEmpty(secrets.SourceConnectionString) || 
            string.IsNullOrEmpty(secrets.DestinationConnectionString) || 
            string.IsNullOrEmpty(secrets.DestinationQueueName))
        {
            Console.WriteLine("Error: Invalid secrets.json configuration");
            return;
        }

        var sourceConnectionString = secrets.SourceConnectionString;
        var destinationQueueName = secrets.DestinationQueueName;
        var sourceQueueName = $"{destinationQueueName}_error";
        var destinationConnectionString = secrets.DestinationConnectionString;

        // Create clients for source and destination
        var sourceClient = new ServiceBusClient(sourceConnectionString);
        var destinationClient = new ServiceBusClient(destinationConnectionString);

        // Create a sender for the destination queue
        var sender = destinationClient.CreateSender(destinationQueueName);

        // Create processor options with error handling and concurrency settings
        var processorOptions = new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 1,
            AutoCompleteMessages = false,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(5)
        };

        // Create the processor
        var processor = sourceClient.CreateProcessor(sourceQueueName, processorOptions);

        // Configure the message handler
        processor.ProcessMessageAsync += async args =>
        {
            try
            {
                var message = args.Message;

                Console.WriteLine($"Received a message");

                // Create a new message to send to the destination queue
                var newMessage = new ServiceBusMessage(message.Body)
                {
                    ContentType = message.ContentType,
                    CorrelationId = message.CorrelationId,
                    MessageId = message.MessageId,
                    Subject = message.Subject
                };

                // Copy properties if any
                foreach (var prop in message.ApplicationProperties)
                {
                    newMessage.ApplicationProperties.Add(prop.Key, prop.Value);
                }

                // Send to destination queue
                await sender.SendMessageAsync(newMessage);
                Console.WriteLine($"Forwarded message to {destinationQueueName}");

                // Complete the message
                await args.CompleteMessageAsync(message);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing message: {ex.Message}");
                // Abandon the message to retry later
                await args.AbandonMessageAsync(args.Message);
            }
        };

        // Configure the error handler
        processor.ProcessErrorAsync += args =>
        {
            Console.WriteLine($"Error occurred: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        try
        {
            // Start the processor
            await processor.StartProcessingAsync();
            Console.WriteLine("Processing messages. Press any key to stop...");
            Console.ReadKey();

            // Stop the processor
            await processor.StopProcessingAsync();
            Console.WriteLine("Processing stopped");

            // Dispose of clients
            await processor.DisposeAsync();
            await sender.DisposeAsync();
            await sourceClient.DisposeAsync();
            await destinationClient.DisposeAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error running processor: {ex.Message}");
        }
    }
}