using TechStoreAPI.OrderFunction;

namespace TechStoreAPI.Interfaces
{
    public interface ICouponService
    {
        Task<AppCoupon?> GetCouponFromPromoCode(string code);
    }
}
