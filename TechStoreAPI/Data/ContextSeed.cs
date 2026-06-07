using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.Identity.Client;
using System.Text.Json;
using TechStoreAPI.LoginRegister;
using TechStoreAPI.Models;
using TechStoreAPI.OrderFunction;

namespace TechStoreAPI.Data
{
    public class ContextSeed
    {
        public static async Task SeedAsync(ApplicationDBContext _context, UserManager<AppUser> userManager)
        {
            if(!userManager.Users.Any(x => x.UserName == "admin@test.com"))
            {
                var user = new AppUser
                {
                    UserName = "admin@test.com",
                    Email = "admin@test.com",
                };

                await userManager.CreateAsync(user, "Pa$$w0rd");
                await userManager.AddToRoleAsync(user, "Admin");
            }

            if(!await _context.Products.AnyAsync())
            {
                var productsData = await File.ReadAllTextAsync("Data/SeedData/products.json");

                var products = JsonSerializer.Deserialize<List<Product>>(productsData);

                if (products == null) return;

                _context.Products.AddRange(products);

                await _context.SaveChangesAsync();
            }

            if (!await _context.DeliveryMethods.AnyAsync())
            {
                var deliveryData = await File.ReadAllTextAsync("Data/SeedData/DeliveryMethod.json");

                var deliveryMethods = JsonSerializer.Deserialize<List<DeliveryMethod>>(deliveryData);

                if (deliveryMethods != null)
                {
                    _context.DeliveryMethods.AddRange(deliveryMethods);
                    await _context.SaveChangesAsync();
                }
            }

        }
    }
}
