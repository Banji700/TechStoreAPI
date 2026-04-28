using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using TechStoreAPI.Models;

namespace TechStoreAPI.ShoppingCartFunc
{
    public class CartItem
    {
        // public int Id { get; set; }

        // public int CartId { get; set; }
        // public ShoppingCart Cart { get; set; } = null!;

        //  public int ProductId { get; set; }
        // public Product Product { get; set; } = null!;

        //  public int Quantity { get; set; }

        public int Id { get; set; }
        public int ProductId { get; set; } 
         public required string ProductName { get; set; }
         public required string Brand { get; set; } = string.Empty;
         public required string Category { get; set; } = string.Empty;
         [Column(TypeName = "decimal(18,2)")]
         public decimal Price { get; set; }
         public required string ImageUrl { get; set; } = string.Empty;
         public int QuantityItems { get; set; }

        //public int Id { get; set; }
        //
        //public int CartId { get; set; }
        //public Cart Cart { get; set; } = null!;
        //
        //public int ProductId { get; set; }
        //public Product Product { get; set; } = null!;
        //
        //public int Quantity { get; set; }

        //For Dto
        //public int ProductId { get; set; }
        //public string Name { get; set; } = "";
       // public string ImageUrl { get; set; } = "";
       // public decimal Price { get; set; }
       // public int Quantity { get; set; }
    }   
}
