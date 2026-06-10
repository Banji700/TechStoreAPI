using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using TechStoreAPI.Models;

namespace TechStoreAPI.ShoppingCartFunc
{
    public class CartItem
    {
        public int Id { get; set; }
        public int ProductId { get; set; } 
         public required string ProductName { get; set; }
         public required string Brand { get; set; } = string.Empty;
         public required string Category { get; set; } = string.Empty;
         [Column(TypeName = "decimal(18,2)")]
         public decimal Price { get; set; }
         public required string ImageUrl { get; set; } = string.Empty;
         public int QuantityItems { get; set; }

        
    }   
}
