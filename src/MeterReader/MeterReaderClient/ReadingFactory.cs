﻿using System;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using MeterReaderWeb.Services;
using Microsoft.Extensions.Logging;

namespace MeterReaderClient
{
    public class ReadingFactory
    {
        private readonly ILogger<ReadingFactory> _logger;

        public ReadingFactory(ILogger<ReadingFactory> logger) => _logger = logger;

        public Task<ReadingMessage> Generate(int customerId)
        {
            var reading = new ReadingMessage
            {
                CustomerId = customerId,
                ReadingTime = Timestamp.FromDateTime(DateTime.UtcNow),
                ReadingValue = new Random().Next(10000)
            };

            return Task.FromResult(reading);
        }
    }
}