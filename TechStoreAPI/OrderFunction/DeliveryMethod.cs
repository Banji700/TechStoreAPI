using System.ComponentModel.DataAnnotations.Schema;

namespace TechStoreAPI.OrderFunction
{
    public class DeliveryMethod
    {
        public int Id { get; set; }
        public required string ShortName { get; set; } 
        public required string DeliveryTime { get; set; }
        public required string Description { get; set; }
        [Column(TypeName = "decimal(18,2)")]
        public  decimal Price { get; set; } 
    }
}
