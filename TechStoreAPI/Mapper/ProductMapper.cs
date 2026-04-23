using TechStoreAPI.DTOs;
using TechStoreAPI.Models;

namespace TechStoreAPI.Mapper
{
    public static class ProductMapper
    {
        public static ProductDto ToProductDto(this Product product)
        {
            return new ProductDto
            {
                Name = product.Name,
                Brand = product.Brand,
                Category = product.Category,
                Price = product.Price,
                Description = product.Description,
                ImageUrl = product.ImageUrl,
                QuantityItems = product.QuantityItems,
            };
        }

        public static Product ToProductCreateDto(this ProductCreateDto productCreateDto)
        {
            return new Product
            {
                Name = productCreateDto.Name,
                Brand = productCreateDto.Brand,
                Category = productCreateDto.Category,
                Price = productCreateDto.Price,
                Description = productCreateDto.Description,
                ImageUrl = productCreateDto.ImageUrl,
                QuantityItems = productCreateDto.QuantityItems,
            };
        }
    }
}
