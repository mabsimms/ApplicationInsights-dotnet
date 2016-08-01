using System.Collections.Generic;

namespace Microsoft.ApplicationInsights.Channel
{
    internal interface ITelemetryBuffer
    {
        /// <summary>
        /// Gets or sets the maximum number of telemetry items that can be buffered before transmission.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">The value is zero or less.</exception>
        int Capacity { get; set; }

        void Enqueue(ITelemetry item);
        IEnumerable<ITelemetry> Dequeue();
    }
}