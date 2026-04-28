using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using TechStoreAPI.Data;
using TechStoreAPI.DTOs;
using TechStoreAPI.Features;
using TechStoreAPI.Interfaces;
using TechStoreAPI.Mapper;
using TechStoreAPI.Models;

namespace TechStoreAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProductsController : ControllerBase
    {
        private readonly ApplicationDBContext _context;
        private readonly ITechRepository _techRepository;

        public ProductsController(ApplicationDBContext context, ITechRepository techRepository )
        {
            _context = context;
            _techRepository = techRepository;
        }

        [HttpGet]
        public async Task<IActionResult> GetAllProducts([FromQuery]QueryObjects queryObj) //string? brand, string? category, string? sort
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState); 
            }

            var allproducts = await _techRepository.GetAllProductsAsync(queryObj);
            return Ok(allproducts);
        }

        [HttpGet("{id:int}")]
        public async Task<IActionResult> GetProductsById(int id)
        {
            var productselected = await _techRepository.GetProductByIdAsync(id);
            return productselected is null ? NotFound("This Product Does Not Exsist") : Ok(productselected);
        }

        [HttpPut("{id:int}")]
        public async Task<IActionResult> UpdateProducts(int id, ProductDto productDto)
        {
            var updateproduct = await _techRepository.UpdateProductAsync(id, productDto);
            return updateproduct is null ? NotFound("This Product Does Not Exsist") : Ok(updateproduct);
        }

        [HttpPost]
        public async Task<IActionResult> CreateNewProduct(ProductCreateDto productDto)
        {
            var newproduct = productDto.ToProductCreateDto();
            await _techRepository.CreateProductAsync(newproduct);
            return CreatedAtAction(nameof(GetProductsById), new { id = newproduct.Id }, newproduct.ToProductDto());
        }

        [HttpDelete("{id:int}")]
        public async Task<IActionResult> DeleteProducts(int id)
        {
            var deleteProduct = await _techRepository.DeleteProductAsync(id);
            return deleteProduct is null ? NotFound("This Product Does Not Exsist") : Ok(deleteProduct);
        }

        [HttpGet("brands")]
        public async Task<IActionResult> GetByBrands()
        {
            return Ok(await _techRepository.GetByBrandsAsync());
        }

        [HttpGet("category")]
        public async Task<IActionResult> GetByCategoy()
        {
            return Ok(await _techRepository.GetByCategoryAsync());
        }
    }
}
