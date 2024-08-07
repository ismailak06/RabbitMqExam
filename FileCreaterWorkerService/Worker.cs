using ClosedXML.Excel;
using FileCreaterWorkerService.Models;
using FileCreaterWorkerService.Services;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared;
using System.Data;
using System.Text;
using System.Text.Json;

namespace FileCreaterWorkerService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly RabbitMqClientService _rabbitMqClientService;
        private readonly IServiceProvider _serviceProvider;
        private IModel _channel;

        public Worker(ILogger<Worker> logger, IServiceProvider serviceProvider, RabbitMqClientService rabbitMqClientService)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
            _rabbitMqClientService = rabbitMqClientService;
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            _channel = _rabbitMqClientService.Connect();
            _channel.BasicQos(0, 1, false);

            return base.StartAsync(cancellationToken);
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var consumer = new AsyncEventingBasicConsumer(_channel);
            _channel.BasicConsume(RabbitMqClientService.QueueName, false, consumer);

            consumer.Received += Consumer_Received;

            return Task.CompletedTask;
        }

        private async Task Consumer_Received(object sender, BasicDeliverEventArgs @event)
        {
            try
            {
                await Task.Delay(1000);

                var excel = JsonSerializer.Deserialize<CreateExcelMessage>(Encoding.UTF8.GetString(@event.Body.ToArray()));

                using var memoryStream = new MemoryStream();

                var wb = new XLWorkbook();
                var ds = new DataSet();
                ds.Tables.Add(GetTable("products"));

                wb.Worksheets.Add(ds);
                wb.SaveAs(memoryStream);
                _logger.LogInformation("Excel oluþtu.");

                MultipartFormDataContent multipartFormDataContent = new MultipartFormDataContent
            {
                { new ByteArrayContent(memoryStream.ToArray()), "file", Guid.NewGuid().ToString() + ".xlsx" }
            };

                var baseUrl = "https://localhost:7057/api/File?fileId=" + excel.FileId;

                using (var httpClient = new HttpClient())
                {
                    var response = await httpClient.PostAsync(baseUrl, multipartFormDataContent);
                    if (response.IsSuccessStatusCode)
                    {
                        _channel.BasicAck(@event.DeliveryTag, false);
                    }
                }
            }
            catch (Exception ex)
            {

                throw new Exception(ex.Message);
            }
        }

        private DataTable GetTable(string tableName)
        {
            List<Product> products;

            using (var scope = _serviceProvider.CreateScope())
            {
                var context = scope.ServiceProvider.GetRequiredService<AdventureWorks2019Context>();

                products = context.Products.ToList();
            }

            DataTable table = new DataTable { TableName = tableName };

            table.Columns.Add("ProductId", typeof(int));
            table.Columns.Add("Name", typeof(String));
            table.Columns.Add("ProductNumber", typeof(string));
            table.Columns.Add("Color", typeof(string));

            products.ForEach(x => table.Rows.Add(x.ProductId, x.Name, x.ProductNumber, x.Color));

            return table;
        }
    }
}