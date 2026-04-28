using Microsoft.EntityFrameworkCore;
using TechStoreAPI.Models;
using TechStoreAPI.ShoppingCartFunc;

namespace TechStoreAPI.Data
{
    public class ApplicationDBContext: DbContext
    {
        public ApplicationDBContext(DbContextOptions dbContextOptions) : base(dbContextOptions)
        {

        }

        public DbSet<Product> Products { get; set; }
        public DbSet<ShoppingCart>ShoppingCarts { get; set; }
        public DbSet<CartItem> CartItems { get; set; }
    }
}
