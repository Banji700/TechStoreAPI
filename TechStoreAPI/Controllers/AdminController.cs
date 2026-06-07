using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TechStoreAPI.Data;
using TechStoreAPI.Features;
using TechStoreAPI.Interfaces;
using TechStoreAPI.OrderAggregate;
using TechStoreAPI.Services;

namespace TechStoreAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AdminController : ControllerBase
    {
        private readonly ApplicationDBContext _context;
        private readonly IPaymentService _paymentService;

        public AdminController(ApplicationDBContext context, IPaymentService paymentService)
        {
            _context = context;
            _paymentService = paymentService;
        }

        [HttpGet("orders")]
        public async Task<ActionResult<Pagination<Order>>>GetOrders([FromQuery]OrderSpecParams specParams)
        {

            var query = _context.Orders
                .Include(x => x.OrderItems)
                .Include(x => x.DeliveryMethod)
                .OrderByDescending(x => x.OrderDate)
                .AsQueryable();

            if(!string.IsNullOrEmpty(specParams.Filter) && specParams.Filter != "All")
            {
                if(Enum.TryParse<OrderStatus>(specParams.Filter, true, out var status))
                {
                    query = query.Where(x => x.Status == status);
                }
            }

            var count = await query.CountAsync();

            var orders = await query
                .Skip((specParams.PageIndex - 1) * specParams.PageSize)
                .Take(specParams.PageSize)
                .ToListAsync();

            return Ok(new Pagination<Order>(
                specParams.PageIndex,
                specParams.PageSize,
                count,
                orders
                ));


        }

        [HttpGet("orders/{id:int}")]
        public async Task<ActionResult<Order>> GetOrder(int id)
        {
            var order = await _context.Orders.
             Include(x => x.DeliveryMethod)
            .Include(x => x.ShippingAddress)
            .Include(x => x.PaymentSummary)
            .Include(x => x.OrderItems).
            ThenInclude(x => x.ProductsOrdered)
           .FirstOrDefaultAsync(x => x.Id == id);

            if (order == null) return NotFound();

            return Ok(order);
        }

        [HttpPut("orders/{id:int}/status")]
        public async Task<ActionResult> UpdateOrderStatus(int id,[FromBody] OrderStatus status)
        {
            var order = await _context.Orders.FindAsync(id);

            if (order == null) return NotFound();

            order.Status = status;

            await _context .SaveChangesAsync();

            return NoContent();
        }

        
        [HttpPost("orders/refund/{id:int}")]

        public async Task<ActionResult<Order>> RefundOrder(int id)
        {
            var order = await _context.Orders.FindAsync(id);  

            if (order == null) return NotFound();

            if (order.Status == OrderStatus.Pending) return BadRequest("Payment Not Received");

            var result = await _paymentService.RefundPayment(order.PaymentIntentId);

            if(result == "succeeded")
            {
                order.Status = OrderStatus.Refunded;

                await _context.SaveChangesAsync();

                return Ok(order);
            }



            return BadRequest();
        }

    }
}
