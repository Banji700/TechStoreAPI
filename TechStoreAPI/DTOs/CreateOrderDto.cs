using System.ComponentModel.DataAnnotations;
using TechStoreAPI.OrderAggregate;

namespace TechStoreAPI.DTOs
{
    public class CreateOrderDto
    {
        [Required]
        public string CartId { get; set; } = string.Empty;

        public string OrderNumber { get; set; } = string.Empty;
        public int DeliveryMethodId { get; set; }
        public AddressDto ShippingAddress { get; set; } = null!;
        public PaymentSummary PaymentSummary { get; set; } = null!;

        public decimal Discount { get; set; }
    }
}
