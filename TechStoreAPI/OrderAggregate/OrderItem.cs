using System.ComponentModel.DataAnnotations.Schema;

namespace TechStoreAPI.OrderAggregate
{
    public class OrderItem
    {
        public int Id { get; set; }
        public ProductsOrdered ProductsOrdered { get; set; } = null!;

        [Column(TypeName = "decimal(18,2)")]
        public decimal Price { get; set; }

        public int Quantity { get; set; }
    }
}
