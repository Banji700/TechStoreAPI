using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection.Metadata;
using TechStoreAPI.OrderFunction;

namespace TechStoreAPI.OrderAggregate
{
    public class Order
    {
        public int Id { get; set; }
        public DateTime OrderDate { get; set; } = DateTime.UtcNow;
        public required string BuyerEmail { get; set; }
        public ShippingAddress ShippingAddress { get; set; } = null!;
        public int DeliveryMethodId { get; set; }
        public DeliveryMethod DeliveryMethod { get; set; } = null!;
        public PaymentSummary PaymentSummary { get; set; } = null!;
        public List<OrderItem> OrderItems { get; set; } = [];
        [Column(TypeName="decimal(18,2)")]
        public decimal Subtotal { get; set; }

        [NotMapped]
        [Column(TypeName = "decimal(18,2)")]
        public decimal Total => Subtotal + DeliveryMethod.Price;
        public OrderStatus Status { get; set; } = OrderStatus.Pending;
        public required string PaymentIntentId { get; set; }
    }
}
