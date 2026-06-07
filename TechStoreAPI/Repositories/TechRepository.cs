using Microsoft.EntityFrameworkCore;
using TechStoreAPI.Data;
using TechStoreAPI.DTOs;
using TechStoreAPI.Features;
using TechStoreAPI.Interfaces;
using TechStoreAPI.Mapper;
using TechStoreAPI.Models;

namespace TechStoreAPI.Repositories
{
    public class TechRepository : ITechRepository
    {
        private readonly ApplicationDBContext _context;

        public TechRepository(ApplicationDBContext context)
        {
            _context = context;
        }

        public async Task<Product> CreateProductAsync(Product product)
        {
            object value = await _context.Products.AddAsync(product);
            await _context.SaveChangesAsync();
            return product;
        }

        public async Task<Product?> DeleteProductAsync(int id)
        {
            var deletedproduct = await _context.Products.FindAsync(id);
            if(deletedproduct == null)
            {
                return null;
            }
            _context.Products.Remove(deletedproduct);
            await _context.SaveChangesAsync();
            return deletedproduct;

        }

        public async Task<Pagination<Product>> GetAllProductsAsync(QueryObjects queryObj)
        {
           var query = _context.Products.AsQueryable();
           if (!string.IsNullOrWhiteSpace(queryObj.Brand))
           {
                var brands = queryObj.Brand.Split(',');
                query = query.Where(b => brands.Contains(b.Brand));
               // query = query.Where(b => b.Brand == queryObj.Brand);
            }
           if (!string.IsNullOrEmpty(queryObj.Category))
           {
                var categories = queryObj.Category.Split(',');
                query = query.Where(x => categories.Contains(x.Category));
                //query = query.Where(x => x.Category == queryObj.Category);
            }
           if(!string.IsNullOrWhiteSpace(queryObj.Search))
            {
                query = query.Where(x => x.Name.ToLower().Contains(queryObj.Search) || 
                x.Brand.ToLower().Contains(queryObj.Search.ToLower()));
            }
           //
           query = queryObj.Sort switch
           {
               "priceAsc" => query.OrderBy(x => x.Price),
               "priceDesc" => query.OrderByDescending(x => x.Price),
               _ => query.OrderBy(x => x.Name)
           };

            var skipNum = (queryObj.PageNum -1) * queryObj.PageSize;

            var count = await query.CountAsync();
            
            var allproducts = await query.Skip(skipNum).Take(queryObj.PageSize).ToListAsync();
            return new Pagination<Product>(queryObj.PageNum, queryObj.PageSize, count, allproducts);//await allproducts;
        }

        public async Task<IReadOnlyList<string>> GetByBrandsAsync()
        {
            return await _context.Products.Select(x => x.Brand).Distinct().ToListAsync();
        }

        public async Task<IReadOnlyList<string>> GetByCategoryAsync()
        {
            return await _context.Products.Select(x => x.Category).Distinct().ToListAsync();
        }

        public async Task<Product?> GetProductByIdAsync(int id)
        {

            var selectedproduct = await _context.Products.FindAsync(id);
            return selectedproduct;
        }

        public async Task<Product?> UpdateProductAsync(int id, ProductDto productDto)
        {
            var updatingProduct = await _context.Products.FindAsync(id);
            if(updatingProduct == null)
            {
                return null;
            }
            updatingProduct.Name = productDto.Name;
            updatingProduct.Brand = productDto.Brand;
            updatingProduct.Category = productDto.Category;
            updatingProduct.Price = productDto.Price;
            updatingProduct.Description = productDto.Description;
            updatingProduct.ImageUrl = productDto.ImageUrl;
            updatingProduct.QuantityItems = productDto.QuantityItems;


            await _context.SaveChangesAsync();
            return updatingProduct;
        }

       
    }
}
