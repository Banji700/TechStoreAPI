using Microsoft.EntityFrameworkCore;
using Microsoft.Identity.Client;
using System.Text.Json;
using TechStoreAPI.Models;

namespace TechStoreAPI.Data
{
    public class ContextSeed
    {
        public static async Task SeedAsync(ApplicationDBContext _context)
        {
            if(!await _context.Products.AnyAsync())
            {
                var productsData = await File.ReadAllTextAsync("Data/SeedData/products.json");

                var products = JsonSerializer.Deserialize<List<Product>>(productsData);

                if (products == null) return;

                _context.Products.AddRange(products);

                await _context.SaveChangesAsync();
            }

        }
    }
}
