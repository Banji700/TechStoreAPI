using Microsoft.EntityFrameworkCore;
using TechStoreAPI.Data;
using TechStoreAPI.Interfaces;

namespace TechStoreAPI.ShoppingCartFunc
{
    public class CartService : ICartService
    {
        private readonly ApplicationDBContext _dbContext;
        private readonly ICouponService _couponService;

        // private readonly ICouponService _couponService;


        public CartService(ApplicationDBContext dbContext, ICouponService couponService)
        {
            _dbContext = dbContext;
            _couponService = couponService;
        }

        public async Task<bool> DeleteCartAsync(string key)
        {
            var cart = await _dbContext.ShoppingCarts
                .Include(x => x.Items)
                .FirstOrDefaultAsync(x => x.BuyerId == key);



            if (cart == null) return false;

            _dbContext.ShoppingCarts.Remove(cart);

            return await _dbContext.SaveChangesAsync() > 0;
        }


        public async Task<ShoppingCart?> GetCartAsync(string key)
        {


            var cart = await _dbContext.ShoppingCarts
            .Include(x => x.Items)
            .FirstOrDefaultAsync(x => x.BuyerId == key);

            if (cart != null && !string.IsNullOrEmpty(cart.CouponCode))
            {
                cart.Coupon = await _couponService.GetCouponFromPromoCode(cart.CouponCode);
            }

            return cart;

        }

        public async Task<ShoppingCart?> SetCartAsync(ShoppingCart cart)
        {
            cart.Coupon = null;

            var existingCart = await _dbContext.ShoppingCarts
                .Include(x => x.Items)
                .FirstOrDefaultAsync(x => x.BuyerId == cart.BuyerId);


            if (existingCart == null)
            {
                _dbContext.ShoppingCarts.Add(cart);
            }
            else
            {
                var itemsToRemove = existingCart.Items
               .Where(existingItem => !cart.Items.Any(newItem => newItem.ProductId == existingItem.ProductId))
               .ToList();

                _dbContext.CartItems.RemoveRange(itemsToRemove);

                if (!cart.Items.Any())
                {
                    _dbContext.ShoppingCarts.Remove(existingCart);

                    await _dbContext.SaveChangesAsync();

                    return null;
                }

                foreach (var item in cart.Items)
                {
                    var existingItem = existingCart.Items
                        .FirstOrDefault(x => x.ProductId == item.ProductId);

                    if (existingItem != null)
                    {
                        existingItem.QuantityItems = item.QuantityItems;
                    }
                    else
                    {
                        existingCart.Items.Add(item);
                    }
                }
                existingCart.DeliveryMethodId = cart.DeliveryMethodId;
                existingCart.PaymentIntentId = cart.PaymentIntentId;
                existingCart.ClientSecret = cart.ClientSecret;
                existingCart.CouponCode = cart.CouponCode;

            }
            

            await _dbContext.SaveChangesAsync();

            return await GetCartAsync(cart.BuyerId);
        }
    }
}
