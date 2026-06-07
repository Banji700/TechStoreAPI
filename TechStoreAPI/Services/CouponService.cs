using Stripe;
using TechStoreAPI.Interfaces;
using TechStoreAPI.OrderFunction;
using System.Collections.Generic;


namespace TechStoreAPI.Services
{
    public class CouponService : ICouponService
    {
        public CouponService(IConfiguration config)
        {
            StripeConfiguration.ApiKey = config["StripeSettings:SecretKey"];
        }
        public async Task<AppCoupon?> GetCouponFromPromoCode(string code)
        {
            var promotionService = new PromotionCodeService();
            var options = new PromotionCodeListOptions
            {
              
                Code = code,
                Active = true,
                Expand = new List<string>
                {
                  "data.promotion.coupon"
                }
            };
            var promotionCodes = await promotionService.ListAsync(options);

            var promotionCode = promotionCodes.FirstOrDefault();

            if (promotionCode == null || promotionCode.Promotion?.Coupon == null)
            {
                return null;
            }

            var couponService = new Stripe.CouponService();

            var coupon = promotionCode.Promotion.Coupon; 

            return new AppCoupon
            {
                Name = coupon.Name,
                AmountOff = coupon.AmountOff,
                PercentOff = coupon.PercentOff,
                CouponId = coupon.Id,
                PromotionCode = promotionCode.Code
            };
            
        }
    }
}
