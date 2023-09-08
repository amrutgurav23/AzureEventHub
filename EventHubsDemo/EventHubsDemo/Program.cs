// See https://aka.ms/new-console-template for more information
using Azure.Messaging.EventHubs.Producer;
using Azure.Messaging.EventHubs;
using System.Text;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;

Console.WriteLine("Event Hub Demo!");

#region Sender
//// number of events to be sent to the event hub
//int numOfEvents = 3;

//// The Event Hubs client types are safe to cache and use as a singleton for the lifetime
//// of the application, which is best practice when events are being published or read regularly.
//// TODO: Replace the <CONNECTION_STRING> and <HUB_NAME> placeholder values
//EventHubProducerClient producerClient = new EventHubProducerClient(
//    "Endpoint=sb://myeventhubdemo123.servicebus.windows.net/;SharedAccessKeyName=Policy1;SharedAccessKey=RkSuDIUTLrvhY3AzQ+cOLLF01hWepncFw+AEhILH8aw=",
//    "highwayeventhub");

//// Create a batch of events  
//using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

//for (int i = 1; i <= numOfEvents; i++)
//{
//    if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes($"Event {i}"))))
//    {
//        // if it is too large for the batch
//        throw new Exception($"Event {i} is too large for the batch and cannot be sent.");
//        Console.ReadLine();
//    }
//}

//try
//{
//    // Use the producer client to send the batch of events to the event hub
//    await producerClient.SendAsync(eventBatch);
//    Console.WriteLine($"A batch of {numOfEvents} events has been published.");
//    Console.ReadLine();
//}
//finally
//{
//    await producerClient.DisposeAsync();
//} 
#endregion

#region Receiver
// Create a blob container client that the event processor will use 
BlobContainerClient storageClient = new BlobContainerClient(
    "DefaultEndpointsProtocol=https;AccountName=mystorageaccountdemo1234;AccountKey=pqSRmtMtTrkg0XI1OfooK5GR1fFf5eyI86QEJMRt+iZ74lPARqbcsmEPdFa6v3yv7QwVk0Broprq+AStSMu9tQ==;EndpointSuffix=core.windows.net", "myblob");

// Create an event processor client to process events in the event hub
var processor = new EventProcessorClient(
    storageClient,
    EventHubConsumerClient.DefaultConsumerGroupName,
    "Endpoint=sb://myeventhubdemo123.servicebus.windows.net/;SharedAccessKeyName=Policy1;SharedAccessKey=RkSuDIUTLrvhY3AzQ+cOLLF01hWepncFw+AEhILH8aw=",
    "highwayeventhub");

// Register handlers for processing events and handling errors
processor.ProcessEventAsync += ProcessEventHandler;
processor.ProcessErrorAsync += ProcessErrorHandler;

// Start the processing
await processor.StartProcessingAsync();

// Wait for 30 seconds for the events to be processed
await Task.Delay(TimeSpan.FromSeconds(30));

// Stop the processing
await processor.StopProcessingAsync();

Task ProcessEventHandler(ProcessEventArgs eventArgs)
{
    // Write the body of the event to the console window
    Console.WriteLine("\tReceived event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));
    Console.ReadLine();
    return Task.CompletedTask;
}

Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
{
    // Write details about the error to the console window
    Console.WriteLine($"\tPartition '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
    Console.WriteLine(eventArgs.Exception.Message);
    Console.ReadLine();
    return Task.CompletedTask;
}
#endregion
