using System.ComponentModel.DataAnnotations;
using TechStoreAPI.OrderFunction;

namespace TechStoreAPI.ShoppingCartFunc
{
    public class ShoppingCart
    {
        public int Id { get; set; }
        public string BuyerId { get; set; } = string.Empty;

        public List<CartItem> Items { get; set; } = [];

        public int? DeliveryMethodId { get; set; }

        public string? ClientSecret {  get; set; }

        public string? PaymentIntentId { get; set; }

        public AppCoupon? Coupon { get; set; }

        public string? CouponCode { get; set; }

    }
}
