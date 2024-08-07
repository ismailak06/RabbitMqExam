using System.Text.Json;
using System.Text;
using Shared;
using RabbitMQ.Client;

namespace UdemyRabbitMQWeb.ExcelCreate.Services
{
    public class RabbitMQPublisher
    {
        private readonly RabbitMqClientService _rabbitMqClientService;

        public RabbitMQPublisher(RabbitMqClientService rabbitMqClientService)
        {
            _rabbitMqClientService = rabbitMqClientService;
        }

        public void Publish(CreateExcelMessage excelMessage)
        {
            var channel = _rabbitMqClientService.Connect();

            var bodyString = JsonSerializer.Serialize(excelMessage);

            var bodyByte = Encoding.UTF8.GetBytes(bodyString);

            var properties = channel.CreateBasicProperties();
            properties.Persistent = true;

            channel.BasicPublish(RabbitMqClientService.ExchangeName, RabbitMqClientService.RoutingExcel, basicProperties: properties, body: bodyByte);
        }
    }
}
