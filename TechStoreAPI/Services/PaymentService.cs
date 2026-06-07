using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.EntityFrameworkCore;
using Stripe;
using System.Diagnostics;
using System.Diagnostics.Eventing.Reader;
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
        private readonly ICouponService _couponService;

        public PaymentService(ICartService cartService, IConfiguration config, ApplicationDBContext dBContext, ICouponService couponService)
        {
            _couponService = couponService;
            _config = config;
            _cartService = cartService;
            _dbcontext = dBContext;
            StripeConfiguration.ApiKey = _config["StripeSettings:SecretKey"];
        }
        public async Task<ShoppingCart?> CreateOrUpdatePaymentIntent(string cartId)
        {
            

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
            decimal discount = 0;

            var subtotal = cart.Items.Sum(x => x.QuantityItems * x.Price);

            if (string.IsNullOrEmpty(cart.CouponCode))
            {
                cart.Coupon = null;
                cart.PaymentIntentId = null;
                cart.ClientSecret = null;
            }
            AppCoupon? coupon = null;

            if (!string.IsNullOrEmpty(cart.CouponCode))
            {
                coupon = await _couponService.GetCouponFromPromoCode(cart.CouponCode);
            }
            //var coupon = await _couponService.GetCouponFromPromoCode(cart.CouponCode);

            if (coupon != null) { 

                cart.Coupon = coupon;
          
                if (coupon.PercentOff != null)
                {
                    discount = subtotal * ((decimal)coupon.PercentOff / 100);
                }

                if (coupon.AmountOff != null)
                {
                    discount = (decimal)coupon.AmountOff / 100;
                }

            }
            else
            {
                cart.Coupon = null;
            }

            var total = subtotal + shippingPrice - discount;
            var amount = (long)Math.Round(total * 100);

      

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

            var updatedCart = await _cartService.SetCartAsync(cart);

            if (updatedCart != null)
            {
                updatedCart.Coupon = coupon;
            }

            return updatedCart;
            // await _cartService.SetCartAsync(cart);
            //
            // cart.Coupon = coupon;

            //  return cart;
        }

       
        public async Task<string> RefundPayment(string paymentIntentId)
         {
            var refundOptions = new RefundCreateOptions
            {
                PaymentIntent = paymentIntentId
            };

            var refundService = new RefundService();

            var result = await refundService.CreateAsync(refundOptions);

            return result.Status;
             
         }
    }
}
