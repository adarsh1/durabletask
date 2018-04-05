using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.AzureStorage.Monitoring;
using DurableTask.Core;
using DurableTask.Core.History;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace DurableTask.AzureStorage.Tracking
{
    class AzureTableTrackingStore : ITrackingStore
    {
        static readonly HistoryEvent[] EmptyHistoryEventList = new HistoryEvent[0];

        readonly string storageAccountName;
        readonly string taskHubName;

        readonly CloudTable historyTable;

        readonly CloudTable instancesTable;

        readonly AzureStorageOrchestrationServiceStats stats;

        readonly TableEntityConverter tableEntityConverter;

        readonly IReadOnlyDictionary<EventType, Type> eventTypeMap;

        public AzureTableTrackingStore(string taskHubName, string storageConnectionString, TableRequestOptions storageTableRequestOptions, AzureStorageOrchestrationServiceStats stats)
        {
            this.stats = stats;
            this.tableEntityConverter = new TableEntityConverter();
            this.taskHubName = taskHubName;

            CloudStorageAccount account = CloudStorageAccount.Parse(storageConnectionString);
            this.storageAccountName = account.Credentials.AccountName;

            CloudTableClient tableClient = account.CreateCloudTableClient();

            string historyTableName = $"{taskHubName}History";
            NameValidator.ValidateTableName(historyTableName);

            string instancesTableName = $"{taskHubName}Instances";
            NameValidator.ValidateTableName(instancesTableName);

            this.historyTable = tableClient.GetTableReference(historyTableName);

            this.instancesTable = tableClient.GetTableReference(instancesTableName);

            this.StorageTableRequestOptions = storageTableRequestOptions;

            // Use reflection to learn all the different event types supported by DTFx.
            // This could have been hardcoded, but I generally try to avoid hardcoding of point-in-time DTFx knowledge.
            Type historyEventType = typeof(HistoryEvent);

            IEnumerable<Type> historyEventTypes = historyEventType.Assembly.GetTypes().Where(
                t => !t.IsAbstract && t.IsSubclassOf(historyEventType));

            PropertyInfo eventTypeProperty = historyEventType.GetProperty(nameof(HistoryEvent.EventType));
            this.eventTypeMap = historyEventTypes.ToDictionary(
                type => ((HistoryEvent)FormatterServices.GetUninitializedObject(type)).EventType);
        }

        /// <summary>
        ///  Table Request Options for The History and Instance Tables
        /// </summary>
        public TableRequestOptions StorageTableRequestOptions { get; set; }

        internal CloudTable HistoryTable => this.historyTable;

        internal CloudTable InstancesTable => this.instancesTable;

        /// <inheritdoc />
        public Task CreateAsync()
        {
            return Task.WhenAll(new Task[]
                {
                    this.historyTable.CreateIfNotExistsAsync(),
                    this.instancesTable.CreateIfNotExistsAsync()
                });
        }

        /// <inheritdoc />
        public Task DeleteAsync()
        {
            return Task.WhenAll(new Task[]
                {
                    this.historyTable.DeleteIfExistsAsync(),
                    this.instancesTable.DeleteIfExistsAsync()
                });
        }

        /// <inheritdoc />
        public async  Task<bool> ExistsAsync()
        {
            return this.historyTable != null && this.instancesTable != null && await this.historyTable.ExistsAsync() && await this.instancesTable.ExistsAsync();
        }

        /// <inheritdoc />
        public async Task<IList<HistoryEvent>> GetHistoryEventsAsync(string instanceId, string expectedExecutionId, CancellationToken cancellationToken = default(CancellationToken))
        {
            IList<HistoryEvent> historyEvents;
            string executionId;

            var filterCondition = new StringBuilder(200);

            const char Quote = '\'';

            // e.g. "PartitionKey eq 'c138dd969a1e4a699b0644c7d8279f81'"
            filterCondition.Append("PartitionKey eq ").Append(Quote).Append(instanceId).Append(Quote); // = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, instanceId);
            if (expectedExecutionId != null)
            {
                // Filter down to a specific generation.
                // e.g. "PartitionKey eq 'c138dd969a1e4a699b0644c7d8279f81' and ExecutionId eq '85f05ce1494c4a29989f64d3fe0f9089'"
                filterCondition.Append(" and ExecutionId eq ").Append(Quote).Append(expectedExecutionId).Append(Quote);
            }

            TableQuery query = new TableQuery().Where(filterCondition.ToString());

            // TODO: Write-through caching should ensure that we rarely need to make this call?
            var historyEventEntities = new List<DynamicTableEntity>(100);

            var stopwatch = new Stopwatch();
            int requestCount = 0;

            bool finishedEarly = false;
            TableContinuationToken continuationToken = null;
            while (true)
            {
                requestCount++;
                stopwatch.Start();
                var segment = await this.historyTable.ExecuteQuerySegmentedAsync(
                    query,
                    continuationToken,
                    StorageTableRequestOptions,
                    null,
                    cancellationToken);
                stopwatch.Stop();

                int previousCount = historyEventEntities.Count;
                historyEventEntities.AddRange(segment);
                this.stats.StorageRequests.Increment();
                this.stats.TableEntitiesRead.Increment(historyEventEntities.Count - previousCount);

                continuationToken = segment.ContinuationToken;
                if (finishedEarly || continuationToken == null || cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }

            if (historyEventEntities.Count > 0)
            {
                // The most recent generation will always be in the first history event.
                executionId = historyEventEntities[0].Properties["ExecutionId"].StringValue;

                // Convert the table entities into history events.
                var events = new List<HistoryEvent>(historyEventEntities.Count);
                foreach (DynamicTableEntity entity in historyEventEntities)
                {
                    if (entity.Properties["ExecutionId"].StringValue != executionId)
                    {
                        // The remaining entities are from a previous generation and can be discarded.
                        break;
                    }

                    events.Add((HistoryEvent)this.tableEntityConverter.ConvertFromTableEntity(entity, GetTypeForTableEntity));
                }

                historyEvents = events;
            }
            else
            {
                historyEvents = EmptyHistoryEventList;
                executionId = expectedExecutionId ?? string.Empty;
            }

            AnalyticsEventSource.Log.FetchedInstanceState(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId,
                historyEvents.Count,
                requestCount,
                stopwatch.ElapsedMilliseconds);

            return historyEvents;
        }

        /// <inheritdoc />
        public async Task<IList<OrchestrationState>> GetStateAsync(string instanceId, bool allExecutions)
        {
            return new[] { await this.GetStateAsync(instanceId, executionId: null) };
        }

        /// <inheritdoc />
        public async Task<OrchestrationState> GetStateAsync(string instanceId, string executionId)
        {
            OrchestrationState orchestrationState = new OrchestrationState();

            var stopwatch = new Stopwatch();
            TableResult orchestration = await this.instancesTable.ExecuteAsync(TableOperation.Retrieve<OrchestrationInstanceStatus>(instanceId, ""));
            stopwatch.Stop();
            this.stats.StorageRequests.Increment();
            this.stats.TableEntitiesRead.Increment(1);

            AnalyticsEventSource.Log.FetchedInstanceStatus(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId,
                stopwatch.ElapsedMilliseconds);

            OrchestrationInstanceStatus orchestrationInstanceStatus = (OrchestrationInstanceStatus)orchestration.Result;
            if (orchestrationInstanceStatus != null)
            {
                if (!Enum.TryParse(orchestrationInstanceStatus.RuntimeStatus, out orchestrationState.OrchestrationStatus))
                {
                    throw new ArgumentException($"{orchestrationInstanceStatus.RuntimeStatus} is not a valid OrchestrationStatus value.");
                }

                orchestrationState.OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = instanceId,
                    ExecutionId = orchestrationInstanceStatus.ExecutionId,
                };
                orchestrationState.Name = orchestrationInstanceStatus.Name;
                orchestrationState.Version = orchestrationInstanceStatus.Version;
                orchestrationState.Status = orchestrationInstanceStatus.CustomStatus;
                orchestrationState.CreatedTime = orchestrationInstanceStatus.CreatedTime;
                orchestrationState.LastUpdatedTime = orchestrationInstanceStatus.LastUpdatedTime;
                orchestrationState.Input = orchestrationInstanceStatus.Input;
                orchestrationState.Output = orchestrationInstanceStatus.Output;
            }

            return orchestrationState;
        }

        /// <inheritdoc />
        public Task PurgeHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public async Task SetNewExecutionAsync(ExecutionStartedEvent executionStartedEvent)
        {
            DynamicTableEntity entity = new DynamicTableEntity(executionStartedEvent.OrchestrationInstance.InstanceId, "")
            {
                Properties =
                {
                    ["Input"] = new EntityProperty(executionStartedEvent.Input),
                    ["CreatedTime"] = new EntityProperty(executionStartedEvent.Timestamp),
                    ["Name"] = new EntityProperty(executionStartedEvent.Name),
                    ["Version"] = new EntityProperty(executionStartedEvent.Version),
                    ["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.Pending.ToString()),
                    ["LastUpdatedTime"] = new EntityProperty(executionStartedEvent.Timestamp),
                }
            };

            Stopwatch stopwatch = Stopwatch.StartNew();
            await this.instancesTable.ExecuteAsync(
                TableOperation.Insert(entity));
            this.stats.StorageRequests.Increment();
            this.stats.TableEntitiesWritten.Increment(1);

            AnalyticsEventSource.Log.InstanceStatusUpdate(
                this.storageAccountName,
                this.taskHubName,
                executionStartedEvent.OrchestrationInstance.InstanceId,
                executionStartedEvent.OrchestrationInstance.ExecutionId,
                executionStartedEvent.EventType.ToString(),
                stopwatch.ElapsedMilliseconds);
        }

        /// <inheritdoc />
        public Task StartAsync()
        {
            ServicePointManager.FindServicePoint(this.historyTable.Uri).UseNagleAlgorithm = false;
            ServicePointManager.FindServicePoint(this.instancesTable.Uri).UseNagleAlgorithm = false;
            return Utils.CompletedTask;
        }

        /// <inheritdoc />
        public async Task UpdateStateAsync(OrchestrationRuntimeState runtimeState, string instanceId, string executionId)
        {
            IList<HistoryEvent> newEvents = runtimeState.NewEvents;
            IList<HistoryEvent> allEvents = runtimeState.Events;

            var newEventList = new StringBuilder(newEvents.Count * 40);
            var batchOperation = new TableBatchOperation();
            var tableBatchBatches = new List<TableBatchOperation>();
            tableBatchBatches.Add(batchOperation);

            EventType? orchestratorEventType = null;

            DynamicTableEntity orchestrationInstanceUpdate = new DynamicTableEntity(instanceId, "")
            {
                Properties =
                {
                    ["CustomStatus"] = new EntityProperty(runtimeState.Status),
                    ["ExecutionId"] = new EntityProperty(executionId),
                    ["LastUpdatedTime"] = new EntityProperty(newEvents.Last().Timestamp),
                }
            };

            for (int i = 0; i < newEvents.Count; i++)
            {
                HistoryEvent historyEvent = newEvents[i];
                DynamicTableEntity entity = this.tableEntityConverter.ConvertToTableEntity(historyEvent);

                newEventList.Append(historyEvent.EventType.ToString()).Append(',');

                // The row key is the sequence number, which represents the chronological ordinal of the event.
                long sequenceNumber = i + (allEvents.Count - newEvents.Count);
                entity.RowKey = sequenceNumber.ToString("X16");
                entity.PartitionKey = instanceId;
                entity.Properties["ExecutionId"] = new EntityProperty(executionId);

                // Table storage only supports inserts of up to 100 entities at a time.
                if (batchOperation.Count == 100)
                {
                    tableBatchBatches.Add(batchOperation);
                    batchOperation = new TableBatchOperation();
                }

                // Replacement can happen if the orchestration episode gets replayed due to a commit failure in one of the steps below.
                batchOperation.InsertOrReplace(entity);

                // Monitor for orchestration instance events 
                switch (historyEvent.EventType)
                {
                    case EventType.ExecutionStarted:
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionStartedEvent executionStartedEvent = (ExecutionStartedEvent)historyEvent;
                        orchestrationInstanceUpdate.Properties["Name"] = new EntityProperty(executionStartedEvent.Name);
                        orchestrationInstanceUpdate.Properties["Version"] = new EntityProperty(executionStartedEvent.Version);
                        orchestrationInstanceUpdate.Properties["Input"] = new EntityProperty(executionStartedEvent.Input);
                        orchestrationInstanceUpdate.Properties["CreatedTime"] = new EntityProperty(executionStartedEvent.Timestamp);
                        orchestrationInstanceUpdate.Properties["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.Running.ToString());
                        break;
                    case EventType.ExecutionCompleted:
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionCompletedEvent executionCompleted = (ExecutionCompletedEvent)historyEvent;
                        orchestrationInstanceUpdate.Properties["Output"] = new EntityProperty(executionCompleted.Result);
                        orchestrationInstanceUpdate.Properties["RuntimeStatus"] = new EntityProperty(executionCompleted.OrchestrationStatus.ToString());
                        break;
                    case EventType.ExecutionTerminated:
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionTerminatedEvent executionTerminatedEvent = (ExecutionTerminatedEvent)historyEvent;
                        orchestrationInstanceUpdate.Properties["Output"] = new EntityProperty(executionTerminatedEvent.Input);
                        orchestrationInstanceUpdate.Properties["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.Terminated.ToString());
                        break;
                    case EventType.ContinueAsNew:
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionCompletedEvent executionCompletedEvent = (ExecutionCompletedEvent)historyEvent;
                        orchestrationInstanceUpdate.Properties["Output"] = new EntityProperty(executionCompletedEvent.Result);
                        orchestrationInstanceUpdate.Properties["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.ContinuedAsNew.ToString());
                        break;
                }
            }

            // TODO: Check to see if the orchestration has completed (newOrchestrationRuntimeState == null)
            //       and schedule a time to clean up that state from the history table.

            // First persistence step is to commit history to the history table. Messages must come after.
            // CONSIDER: If there are a large number of history items and messages, we could potentially
            //           improve overall system throughput by incrementally enqueuing batches of messages
            //           as we write to the table rather than waiting for all table batches to complete
            //           before we enqueue messages.
            foreach (TableBatchOperation operation in tableBatchBatches)
            {
                if (operation.Count > 0)
                {
                    Stopwatch stopwatch = Stopwatch.StartNew();
                    await this.historyTable.ExecuteBatchAsync(
                        operation,
                        this.StorageTableRequestOptions,
                        null);
                    this.stats.StorageRequests.Increment();
                    this.stats.TableEntitiesWritten.Increment(operation.Count);

                    AnalyticsEventSource.Log.AppendedInstanceState(
                        this.storageAccountName,
                        this.taskHubName,
                        instanceId,
                        executionId,
                        newEvents.Count,
                        allEvents.Count,
                        newEventList.ToString(0, newEventList.Length - 1),
                        stopwatch.ElapsedMilliseconds);
                }
            }

            Stopwatch orchestrationInstanceUpdateStopwatch = Stopwatch.StartNew();
            await this.instancesTable.ExecuteAsync(TableOperation.InsertOrMerge(orchestrationInstanceUpdate));
            AnalyticsEventSource.Log.InstanceStatusUpdate(
            this.storageAccountName,
            this.taskHubName,
            instanceId,
             executionId,
            orchestratorEventType?.ToString() ?? string.Empty,
            orchestrationInstanceUpdateStopwatch.ElapsedMilliseconds);

            this.stats.StorageRequests.Increment();
            this.stats.TableEntitiesWritten.Increment();
        }

        Type GetTypeForTableEntity(DynamicTableEntity tableEntity)
        {
            string propertyName = nameof(HistoryEvent.EventType);

            EntityProperty eventTypeProperty;
            if (!tableEntity.Properties.TryGetValue(propertyName, out eventTypeProperty))
            {
                throw new ArgumentException($"The DynamicTableEntity did not contain a '{propertyName}' property.");
            }

            if (eventTypeProperty.PropertyType != EdmType.String)
            {
                throw new ArgumentException($"The DynamicTableEntity's {propertyName} property type must a String.");
            }

            EventType eventType;
            if (!Enum.TryParse(eventTypeProperty.StringValue, out eventType))
            {
                throw new ArgumentException($"{eventTypeProperty.StringValue} is not a valid EventType value.");
            }

            return this.eventTypeMap[eventType];
        }
    }
}
