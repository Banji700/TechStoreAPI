using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using TechStoreAPI.Interfaces;
using TechStoreAPI.OrderFunction;

namespace TechStoreAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CouponController : ControllerBase
    {
        private readonly ICouponService _couponService;
        public CouponController(ICouponService couponService)
        {
            _couponService = couponService;
        }

        [HttpGet("{code}")]
        public async Task<ActionResult<AppCoupon>>ValidateCoupon(string code)
        {
            var coupon = await _couponService.GetCouponFromPromoCode(code);

            if (coupon == null) return BadRequest("Invalid voucher code");

            return Ok (coupon);
        }

    }
}
