using Microsoft.EntityFrameworkCore;
using TechStoreAPI.Data;
using TechStoreAPI.DTOs;
using TechStoreAPI.Interfaces;
using TechStoreAPI.LoginRegister;
using TechStoreAPI.OrderAggregate;

namespace TechStoreAPI.Services
{
    public class OrderingService
    {
        private readonly ApplicationDBContext _dbContext;
        private readonly ICouponService _couponService;
        public OrderingService(ApplicationDBContext dbContext, ICouponService couponService)
        {
            _dbContext = dbContext;
            _couponService = couponService;
        }

        public async Task<Order?> CreateOrderAsync(CreateOrderDto orderDto, string buyerEmail)
        {
            var cart = await _dbContext.ShoppingCarts.Include(x => x.Items).FirstOrDefaultAsync(x => x.BuyerId == orderDto.CartId);

            if(cart == null) return null;

            var deliveryMethod = await _dbContext.DeliveryMethods.FindAsync(orderDto.DeliveryMethodId);

            if(deliveryMethod == null) return null;



            var orderItems = cart.Items.Select(item => new OrderItem
            {
                ProductsOrdered = new ProductsOrdered
                { 
                    ProductId = item.ProductId,
                    ProductName = item.ProductName,
                    ImageUrl = item.ImageUrl,
                },
                Price = item.Price,
                Quantity = item.QuantityItems

            }).ToList();

           var subtotal = orderItems.Sum(x => x.Price * x.Quantity);

            if (!string.IsNullOrEmpty(cart.CouponCode))
            {
                cart.Coupon = await _couponService.GetCouponFromPromoCode(cart.CouponCode);
            }

            var discount = cart.Coupon != null
            ? cart.Coupon.PercentOff != null
            ? subtotal * ((decimal)cart.Coupon.PercentOff / 100)
            : (decimal)(cart.Coupon.AmountOff ?? 0) / 100
            : 0;

            var orderCount = await _dbContext.Orders.CountAsync(x => x.BuyerEmail == buyerEmail);

            var order = new Order
            {
                BuyerEmail = buyerEmail,
                PaymentIntentId = cart.PaymentIntentId!,
                Discount = discount,

                ShippingAddress = new ShippingAddress
                {
                    Name = orderDto.ShippingAddress.Name,
                    Line1 = orderDto.ShippingAddress.Line1,
                    Line2 = orderDto.ShippingAddress.Line2,
                    City = orderDto.ShippingAddress.City,
                    State = orderDto.ShippingAddress.State,
                    PostalCode = orderDto.ShippingAddress.PostalCode,
                    Country = orderDto.ShippingAddress.Country
                },
                DeliveryMethodId = deliveryMethod.Id,
                DeliveryMethod = deliveryMethod,
                PaymentSummary = orderDto.PaymentSummary,
                OrderItems = orderItems,
                Subtotal = subtotal,
                OrderNumber = $"{(orderCount + 1):D2}"

            };

            _dbContext.Orders.Add(order);

            var result = await _dbContext.SaveChangesAsync();

            return result > 0 ? order : null;
        }

        public async Task<IReadOnlyList<Order>> GetOrdersAsync(string buyerEmail)
        {
            return await _dbContext.Orders.Include(x => x.DeliveryMethod).Include(x => x.OrderItems).ThenInclude(x => x.ProductsOrdered).Where(x => x.BuyerEmail == buyerEmail).ToListAsync();
            // return await _dbContext.Orders.Include(x => x.DeliveryMethod).Where(x => x.BuyerEmail == buyerEmail).ToListAsync();
        }

        public async Task<Order?> GetOrderByIdAsync(int id, string buyerEmail)
        {
            return await _dbContext.Orders.
                Include(x => x.DeliveryMethod)
                .Include(x => x.ShippingAddress)
                .Include(x => x.PaymentSummary)
                .Include(x => x.OrderItems).
                ThenInclude(x => x.ProductsOrdered)
                .FirstOrDefaultAsync(x => x.Id == id && x.BuyerEmail == buyerEmail);
            //return await _dbContext.Orders.Include(x => x.DeliveryMethod).Include(x => x.OrderItems).FirstOrDefaultAsync(x => x.Id == id && x.BuyerEmail==buyerEmail);
        }


    }
}
