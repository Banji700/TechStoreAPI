using Microsoft.EntityFrameworkCore;
using Stripe;
using System.Diagnostics;
using TechStoreAPI.Data;
using TechStoreAPI.Interfaces;
using TechStoreAPI.OrderFunction;
using TechStoreAPI.ShoppingCartFunc;

namespace TechStoreAPI.Services
{
    public class PaymentService : IPaymentService
    {
        private readonly IConfiguration _config;
        private readonly ICartService _cartService;
        private readonly ApplicationDBContext _dbcontext;

        public PaymentService(ICartService cartService, IConfiguration config, ApplicationDBContext dBContext)
        {
            _config = config;
            _cartService = cartService;
            _dbcontext = dBContext;
        }
        public async Task<ShoppingCart?> CreateOrUpdatePaymentIntent(string cartId)
        {
            StripeConfiguration.ApiKey = _config["StripeSettings:SecretKey"];

            var cart = await _cartService.GetCartAsync(cartId);

            if (cart == null) return null;

            var shippingPrice = 0m;

            if (cart.DeliveryMethodId.HasValue)
            {
                var deliveryMeth = await _dbcontext.DeliveryMethods.FirstOrDefaultAsync(x => x.Id == cart.DeliveryMethodId.Value);

                if (deliveryMeth == null) return null;

                shippingPrice = deliveryMeth.Price;
            }

            foreach (var item in cart.Items)
            {
                var productItem = await _dbcontext.Products.FirstOrDefaultAsync(x => x.Id == item.ProductId);
                if (productItem == null) return null;
                if (item.Price != productItem.Price)
                {
                    item.Price = productItem.Price;
                }
            }
            var service = new PaymentIntentService();
            PaymentIntent? intent = null;

            var subtotal = cart.Items.Sum(x => x.QuantityItems * x.Price);
            var total = subtotal + shippingPrice;
            var amount = (long)(total * 100);

            if (string.IsNullOrEmpty(cart.PaymentIntentId))
            {
                var options = new PaymentIntentCreateOptions
                {
                    // Amount = (long)cart.Items.Sum(x => x.QuantityItems * (x.Price * 100)) + (long)shippingPrice * 100,
                    // Currency = "gbp",
                    // PaymentMethodTypes = ["card"]
                    Amount = amount,
                    Currency = "gbp",
                    PaymentMethodTypes = ["card"]
                };

                intent = await service.CreateAsync(options);
                cart.PaymentIntentId = intent.Id;
                cart.ClientSecret = intent.ClientSecret;
            }
            else
            {
                var existingIntent = await service.GetAsync(cart.PaymentIntentId);

                if (existingIntent.Status == "requires_payment_method" ||
                    existingIntent.Status == "requires_confirmation")
                {
                    var options = new PaymentIntentUpdateOptions
                    {
                        Amount = amount
                    };

                    intent = await service.UpdateAsync(cart.PaymentIntentId, options);
                }
                else
                {
                    var createOptions = new PaymentIntentCreateOptions
                    {
                        Amount = amount,
                        Currency = "gbp",
                        PaymentMethodTypes = ["card"]
                    };

                    intent = await service.CreateAsync(createOptions);

                    cart.PaymentIntentId = intent.Id;
                    cart.ClientSecret = intent.ClientSecret;
                }

               

            }
            await _cartService.SetCartAsync(cart);

            return cart;
        }
    }
}
