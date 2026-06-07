using System.ComponentModel.DataAnnotations.Schema;

namespace TechStoreAPI.OrderFunction
{
    public class AppCoupon
    {
        public int Id { get; set; }
        public required string Name { get; set; }
        [Column(TypeName = "decimal(18,2)")]
        public decimal? AmountOff { get; set; }
        [Column(TypeName = "decimal(18,2)")]
        public decimal? PercentOff { get; set; }
        public required string PromotionCode { get; set; }
        public required string CouponId { get; set; }
    }
}
