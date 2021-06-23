using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using MeterReaderWeb.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MeterReaderClient
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _config;
        private readonly ReadingFactory _factory;
        private MeterReadingService.MeterReadingServiceClient _client = null;
        private string _token;
        private DateTime _expiration = DateTime.MinValue;
        private Metadata _headers;

        public Worker(ILogger<Worker> logger, IConfiguration config, ReadingFactory factory)
        {
            _logger = logger;
            _config = config;
            _factory = factory;
        }

        protected MeterReadingService.MeterReadingServiceClient Client
        {
            get
            {
                if (_client == null)
                {
                    var channel = GrpcChannel.ForAddress(_config["Service:ServiceUrl"]);
                    _client = new MeterReadingService.MeterReadingServiceClient(channel);
                }

                return _client;
            }
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var counter = 0;
            
            var customerId = _config.GetValue<int>("Service:CustomerId");
            
            while (!stoppingToken.IsCancellationRequested)
            {
                counter++;

                if (counter % 3 == 0)
                {
                    if (await IsAuthorized())
                    {
                        Console.WriteLine("Sending Diagnostics");

                        var stream = Client.SendDiagnostics(_headers);
                        for (var i = 0; i < 5; i++)
                        {
                            var reading = await _factory.Generate(customerId);
                            await stream.RequestStream.WriteAsync(reading);
                        }

                        await stream.RequestStream.CompleteAsync();
                    }
                }
                
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

                var pkt = new ReadingPacket
                {
                    Successful = ReadingStatus.Success,
                    Notes = "This is our test"
                };

                foreach (var _ in Enumerable.Range(0, 5))
                {
                    pkt.Readings.Add(await _factory.Generate(customerId));
                }


                try
                {
                    if (await IsAuthorized())
                    {
                        var result = await Client.AddReadingAsync(pkt, _headers);

                        _logger.LogInformation(result.Success == ReadingStatus.Success
                            ? "Successfully sent"
                            : "Failed to send");
                    }
                }
                catch (RpcException e)
                {
                    if(e.StatusCode == StatusCode.OutOfRange) _logger.LogError($"{e.Trailers}");
                    _logger.LogError($"Exception thrown: {e}");
                }


                await Task.Delay(_config.GetValue<int>("Service:DelayInterval"), stoppingToken);
            }
        }

        private async Task<bool> IsAuthorized()
        {
            if (!string.IsNullOrWhiteSpace(_token) && _expiration > DateTime.UtcNow) return true;
            
            var response = await Client.CreateTokenAsync(new TokenRequest
            {
                Username = _config["Service:Username"],
                Password= _config["Service:Password"]
            });

            if (!response.Success) return false;
            
            _token = response.Token;
            _expiration = response.Expiration.ToDateTime();
            _headers = new Metadata {{"Authorization", $"Bearer {_token}"}};
            return true;
        }
    }
}