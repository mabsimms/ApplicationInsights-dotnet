// <copyright file="InMemoryTransmitter.cs" company="Microsoft">
// Copyright © Microsoft. All Rights Reserved.
// </copyright>


namespace Microsoft.ApplicationInsights.Channel
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks.Dataflow;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Extensibility.Implementation;
    using Extensibility.Implementation.Tracing;

    /// <summary>
    /// A transmitter that will immediately send telemetry over HTTP. 
    /// Telemetry items are being sent when Flush is called, or when the buffer 
    /// is full (An OnFull "event" is raised) or every 30 seconds. 
    /// </summary>
    internal class PipelinedInMemoryChannel : ITelemetryChannel, IDisposable
    {
        // TPL Dataflow pipeline objects and lifecycle management via CancellationToken
        private BufferBlock<ITelemetry> _buffer;
        private BatchBlock<ITelemetry> _batcher;
        private ActionBlock<IEnumerable<ITelemetry>> _publish;
        private CancellationTokenSource _tokenSource;
        private IDisposable[] _disposables;
        private int _disposeCount = 0;

        // Background timer to periodically flush the batch block
        private System.Threading.Timer _windowTimer;

        // Set the default endpoint address
        private Uri _endpointAddress = new Uri(Constants.TelemetryServiceEndpoint);
        private bool _developerMode = false;

        internal PipelinedInMemoryChannel(ITelemetryBuffer buffer)
        {
            this._tokenSource = new CancellationTokenSource();
           
            // Starting the Runner
            InitializePipeline(buffer.Capacity);
        }

        private void InitializePipeline(int maxBufferedCapacity)
        {
            _buffer = new BufferBlock<ITelemetry>(
                new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = maxBufferedCapacity * 2,
                    CancellationToken = _tokenSource.Token
                });

            _batcher = new BatchBlock<ITelemetry>(maxBufferedCapacity,
                new GroupingDataflowBlockOptions()
                {
                    BoundedCapacity = maxBufferedCapacity,
                    Greedy = true,
                    CancellationToken = _tokenSource.Token
                });

            _publish = new ActionBlock<IEnumerable<ITelemetry>>(
                async (e) => await Send(e),
                   new ExecutionDataflowBlockOptions()
                   {
                       // Maximum of one concurrent batch being published
                       MaxDegreeOfParallelism = 1,

                       // Maximum of three pending batches to be published
                       BoundedCapacity = 3,               
                       CancellationToken = _tokenSource.Token
                   });

            _disposables = new IDisposable[]
            {
                _buffer.LinkTo(_batcher),
                _batcher.LinkTo(_publish)
            };

            _windowTimer = new Timer(Flush, null,
               SendingInterval, SendingInterval);
        }

        public bool? DeveloperMode
        {
            get { return _developerMode; }
            set
            {
                if (value != _developerMode)
                {
                    // Enable developer mode
                    if (value.HasValue && value.Value)
                    {
                        
                    }
                    // Disable developer mode
                    else
                    {
                        
                    }
                }
            }
        }

        public void Send(ITelemetry item)
        {
            try
            {
                if (!_buffer.Post(item))
                {
                    // TODO; immediate flush?
                }                
            }
            catch (Exception e)
            {
                CoreEventSource.Log.LogVerbose("PipelinedInMemoryTransmitter.Enqueue failed: ", e.ToString());
            }
        }

        protected Uri _EndpointAddress
        {
            get { return this._endpointAddress; }
            set { Property.Set(ref this._endpointAddress, value); }
        }

        public string EndpointAddress
        {
            get { return this._EndpointAddress.ToString(); }
            set { this._EndpointAddress = new Uri(value); }
        }

        internal TimeSpan SendingInterval { get; private set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Flushes the in-memory buffer and sends it.
        /// </summary>
        public void Flush(object state = null)
        {
            _batcher?.TriggerBatch();
        }

        public void Flush()
        {
            _batcher?.TriggerBatch();
        }

        /// <summary>
        /// Happens when the in-memory buffer is full. Flushes the in-memory buffer 
        /// and sends the telemetry items.
        /// </summary>
        private void OnBufferFull()
        {
            _batcher?.TriggerBatch();
        }

        /// <summary>
        /// Serializes a list of telemetry items and sends them.
        /// </summary>
        private async Task Send(IEnumerable<ITelemetry> telemetryItems)
        {
            if (telemetryItems == null || !telemetryItems.Any())
            {
                CoreEventSource.Log.LogVerbose("No Telemetry Items passed to Enqueue");
                return;
            }

            byte[] data = JsonSerializer.Serialize(telemetryItems);
            var transmission = new Transmission(this._endpointAddress, 
                data, "application/x-json-stream", 
                JsonSerializer.CompressionType);

            await transmission.SendAsync().ConfigureAwait(false);
        }

        private void Dispose(bool disposing)
        {
            if (Interlocked.Increment(ref _disposeCount) == 1)
            {
                _tokenSource.Cancel();
                _windowTimer?.Dispose();
                foreach (var d in _disposables)
                    d.Dispose(); 
            }
        }
    }
}
