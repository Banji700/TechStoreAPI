using Microsoft.EntityFrameworkCore;
using TechStoreAPI.Data;

namespace TechStoreAPI.ShoppingCartFunc
{
    public class CartService : ICartService
    {
        private readonly ApplicationDBContext _dbContext;

        public CartService(ApplicationDBContext dbContext)
        {
            _dbContext = dbContext;
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


            return await _dbContext.ShoppingCarts
                .Include(x => x.Items)
                .FirstOrDefaultAsync(x => x.BuyerId == key);

        }

        public async Task<ShoppingCart?> SetCartAsync(ShoppingCart cart)
        {
            var existingCart = await _dbContext.ShoppingCarts
            .Include(x => x.Items)
            .FirstOrDefaultAsync(x => x.BuyerId == cart.BuyerId);

            if (existingCart == null)
            {
                _dbContext.ShoppingCarts.Add(cart);
            }
            else
            {
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
            }

            await _dbContext.SaveChangesAsync();

            return await GetCartAsync(cart.BuyerId);
        }
    }
}
