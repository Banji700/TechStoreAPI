using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using TechStoreAPI.ShoppingCartFunc;

namespace TechStoreAPI.Controllers
{
    [Route("api/[controller]")]
    //[Route("api/shoppingcart")]
    [ApiController]
    public class ShoppingCartController : ControllerBase
    {
        private readonly ICartService _cartService;

        public ShoppingCartController(ICartService cartService)
        {
            _cartService = cartService;
        }

        [HttpGet]
        public async Task<ActionResult<ShoppingCart>?> GetCart()
        {
            var buyerId = Request.Cookies["buyerId"];
            if(string.IsNullOrEmpty(buyerId) )
            {
                return Ok(null);
                //return BadRequest("buyerId cookie missing");
                //return NotFound();
            }

            var cart = await _cartService.GetCartAsync(buyerId);
            if(cart == null )
            {
                return Ok(null);
            }
            return cart;
        }

        [HttpPost]
        public async Task<ActionResult<ShoppingCart>> CreateOrUpdateCart(ShoppingCart cart)
        {
            var buyerId = Request.Cookies["buyerId"];
            if(string.IsNullOrEmpty(buyerId))
            {
                buyerId = Guid.NewGuid().ToString();

                Response.Cookies.Append("buyerId", buyerId, new CookieOptions
                {
                    IsEssential = true,
                    Expires = DateTime.UtcNow.AddDays(30),
                    SameSite = SameSiteMode.None,
                    Secure = true
                });
            }

            cart.BuyerId = buyerId;
            var updatedCart = await _cartService.SetCartAsync(cart);

            if(updatedCart == null )
            {
                return BadRequest("Problem with cart");
            }

            return updatedCart;
                
        }

        [HttpDelete]
        public async Task<ActionResult> DeleteCart()
        {
            var buyerId = Request.Cookies["buyerId"];

            if (string.IsNullOrEmpty(buyerId))
            {
                return NotFound();
            }

            var result = await _cartService.DeleteCartAsync(buyerId);

            if (!result)
            {
                return BadRequest("Problem deleting cart");
            }

            Response.Cookies.Delete("buyerId");

            return Ok();
        }
    }
}
