using TechStoreAPI.DTOs;
using TechStoreAPI.Features;
using TechStoreAPI.Models;

namespace TechStoreAPI.Interfaces
{
    public interface ITechRepository
    {
        Task<Pagination<Product>> GetAllProductsAsync(QueryObjects queryObj); //string? brand, string? category, string? sort

        Task<IReadOnlyList<string>> GetByBrandsAsync();

        Task<IReadOnlyList<string>> GetByCategoryAsync();

        Task <Product?> GetProductByIdAsync(int id);

        Task<Product> CreateProductAsync(Product product);

        Task<Product?> UpdateProductAsync(int id, ProductDto productDto);

        Task <Product?> DeleteProductAsync(int id);



    }
}
