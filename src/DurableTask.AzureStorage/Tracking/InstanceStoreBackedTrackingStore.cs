﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.Common;
using DurableTask.Core.History;
using DurableTask.Core.Tracking;
using Microsoft.WindowsAzure.Storage;

namespace DurableTask.AzureStorage.Tracking
{
    class InstanceStoreBackedTrackingStore : ITrackingStore
    {
        static readonly HistoryEvent[] EmptyHistoryEventList = new HistoryEvent[0];

        readonly IOrchestrationServiceInstanceStore instanceStore;

        /// <inheritdoc />
        public InstanceStoreBackedTrackingStore(IOrchestrationServiceInstanceStore instanceStore)
        {
            this.instanceStore = instanceStore;
        }

        /// <inheritdoc />
        public Task CreateAsync()
        {
            return this.instanceStore.InitializeStoreAsync(false);
        }

        /// <inheritdoc />
        public Task DeleteAsync()
        {
            return this.instanceStore.DeleteStoreAsync();
        }

        /// <summary>
        /// Instance Store Does not Support this currently
        /// </summary>
        /// <returns></returns>
        public Task<bool> ExistsAsync()
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public async Task<IList<HistoryEvent>> GetHistoryEventsAsync(string instanceId, string expectedExecutionId, CancellationToken cancellationToken = default(CancellationToken))
        {
            //If no execution Id is provided get the latest executionId by getting the latest state
            if (expectedExecutionId == null)
            {
                expectedExecutionId = (await instanceStore.GetOrchestrationStateAsync(instanceId, false)).FirstOrDefault()?.State.OrchestrationInstance.ExecutionId;
            }

            var events = await instanceStore.GetOrchestrationHistoryEventsAsync(instanceId, expectedExecutionId);

            if (events == null || !events.Any())
            {
                return EmptyHistoryEventList;
            }
            else
            {
                return events.Select(x => x.HistoryEvent).ToList();
            }
        }

        /// <inheritdoc />
        public async Task<IList<OrchestrationState>> GetStateAsync(string instanceId, bool allExecutions)
        {
            IEnumerable<OrchestrationStateInstanceEntity> states = await instanceStore.GetOrchestrationStateAsync(instanceId, allExecutions);
            return states?.Select(s => s.State).ToList() ?? new List<OrchestrationState>();
        }

        /// <inheritdoc />
        public async Task<OrchestrationState> GetStateAsync(string instanceId, string executionId)
        {
            if (executionId == null)
            {
                return (await GetStateAsync(instanceId, false)).FirstOrDefault();
            }
            else
            {
                return (await instanceStore.GetOrchestrationStateAsync(instanceId, executionId))?.State;
            }
        }

        /// <inheritdoc />
        public Task PurgeHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            return instanceStore.PurgeOrchestrationHistoryEventsAsync(thresholdDateTimeUtc, timeRangeFilterType);
        }

        /// <inheritdoc />
        public async Task SetNewExecutionAsync(ExecutionStartedEvent executionStartedEvent)
        {
            var orchestrationState = new OrchestrationState()
            {
                Name = executionStartedEvent.Name,
                Version = executionStartedEvent.Version,
                OrchestrationInstance = executionStartedEvent.OrchestrationInstance,
                OrchestrationStatus = OrchestrationStatus.Pending,
                Input = executionStartedEvent.Input,
                Tags = executionStartedEvent.Tags,
                CreatedTime = executionStartedEvent.Timestamp,
                LastUpdatedTime = DateTime.UtcNow,
                CompletedTime = DateTimeUtils.MinDateTime
            };

            var orchestrationStateEntity = new OrchestrationStateInstanceEntity()
            {
                State = orchestrationState,
                SequenceNumber = 0
            };

            await this.instanceStore.WriteEntitiesAsync(new[] { orchestrationStateEntity });
        }

        /// <inheritdoc />
        public Task StartAsync()
        {
            //NOP
            return Utils.CompletedTask;
        }

        /// <inheritdoc />
        public async Task UpdateStateAsync(OrchestrationRuntimeState runtimeState, string instanceId, string executionId)
        {
            int oldEventsCount = (runtimeState.Events.Count - runtimeState.NewEvents.Count);
            await instanceStore.WriteEntitiesAsync(runtimeState.NewEvents.Select((x, i) =>
            {
                return new OrchestrationWorkItemInstanceEntity()
                {
                    HistoryEvent = x,
                    ExecutionId = executionId,
                    InstanceId = instanceId,
                    SequenceNumber = i + oldEventsCount,
                    EventTimestamp = x.Timestamp
                };

            }));

            await instanceStore.WriteEntitiesAsync(new InstanceEntityBase[]
            {
                    new OrchestrationStateInstanceEntity()
                    {
                        State = Core.Common.Utils.BuildOrchestrationState(runtimeState),
                        SequenceNumber = runtimeState.Events.Count
                    }
            });
        }
    }
}
