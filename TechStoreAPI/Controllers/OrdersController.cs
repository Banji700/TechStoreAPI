using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Security.Claims;
using TechStoreAPI.DTOs;
using TechStoreAPI.OrderAggregate;
using TechStoreAPI.Services;

namespace TechStoreAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]

    public class OrdersController : ControllerBase
    {
        private readonly OrderingService _orderingService;

        public OrdersController(OrderingService orderingService)
        {
            _orderingService = orderingService;
        }

        [Authorize]
        [HttpPost]
        public async Task<ActionResult<Order>> CreateOrder(CreateOrderDto orderDto)
        {
            var email = User.FindFirstValue(ClaimTypes.Email);

            if (string.IsNullOrEmpty(email))
                return Unauthorized();

            var createOrder = await _orderingService.CreateOrderAsync(orderDto, email);

            if (createOrder == null)
            {
                return BadRequest();
            }

            return Ok(createOrder);
        }

        [HttpGet]
        public async Task<ActionResult<IReadOnlyList<Order>>> GetOrdersUser()
        {
            var email = User.FindFirstValue(ClaimTypes.Email);

            if (string.IsNullOrEmpty(email))
            {
                return Unauthorized();
            }
            var orders = await _orderingService.GetOrdersAsync(email);

            return Ok(orders);

        }

        [HttpGet("{id:int}")]
        public async Task<ActionResult<Order?>> GetOrderbyId(int id)
        {
            var email = User.FindFirstValue(ClaimTypes.Email);

            if (string.IsNullOrEmpty(email))
                return Unauthorized();
            var orders = await _orderingService.GetOrderByIdAsync(id,email);

            if (orders == null) return NotFound();
            return Ok(orders);
        }
    }
}
