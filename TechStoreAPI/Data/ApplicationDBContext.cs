using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using TechStoreAPI.ConfigFolder;
using TechStoreAPI.LoginRegister;
using TechStoreAPI.Models;
using TechStoreAPI.OrderAggregate;
using TechStoreAPI.OrderFunction;
using TechStoreAPI.ShoppingCartFunc;

namespace TechStoreAPI.Data
{
    public class ApplicationDBContext: IdentityDbContext<AppUser>
    {
        public ApplicationDBContext(DbContextOptions dbContextOptions) : base(dbContextOptions)
        {

        }

        public DbSet<Product> Products { get; set; }
        public DbSet<ShoppingCart>ShoppingCarts { get; set; }
        public DbSet<CartItem> CartItems { get; set; }
        public DbSet<Address> Addresses { get; set; }
        public DbSet<DeliveryMethod> DeliveryMethods { get; set; }
        public DbSet<Order> Orders { get; set; }
        public DbSet<OrderItem> OrderItems { get; set; }

        protected override void OnModelCreating(ModelBuilder builder)
        {
            base.OnModelCreating(builder);

            builder.ApplyConfiguration(new RoleConfiguration());

            //List<IdentityRole> roles = new List<IdentityRole>
            // {
            //     new IdentityRole
            //    {
            //        Id = "1",
            //        Name = "Admin",
            //        NormalizedName = "ADMIN",
            //        ConcurrencyStamp = "admin-role-stamp"
            //   },
            //    new IdentityRole
            //    {
            //        Id = "2",
            //        Name = "User",
            //        NormalizedName = "USER",
            //         ConcurrencyStamp = "user-role-stamp"

            //    },
            // };
            // builder.Entity<IdentityRole>().HasData(roles);
        }
    }
}
