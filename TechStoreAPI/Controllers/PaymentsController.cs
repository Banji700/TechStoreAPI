using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;
using Stripe;
using TechStoreAPI.Data;
using TechStoreAPI.Interfaces;
using TechStoreAPI.OrderAggregate;
using TechStoreAPI.OrderFunction;
using TechStoreAPI.ShoppingCartFunc;
using TechStoreAPI.SignalR;

namespace TechStoreAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PaymentsController : ControllerBase
    {
        private readonly IPaymentService _paymentService;
        private readonly ApplicationDBContext _dbContext;
        private readonly IHubContext<NotificationHub> _hubContext;
        private readonly string _whSecret;

        public PaymentsController(IPaymentService paymentService, ApplicationDBContext dbContext, IConfiguration config, IHubContext<NotificationHub> hubContext)
        {
            _dbContext = dbContext;
            _paymentService = paymentService;
            _whSecret = config["StripeSettings:WhSecret"]!;
            _hubContext = hubContext;

        }



        //[Authorize]
        //[HttpPost("{cartId}")]
        [HttpPost]
        public async Task<ActionResult<ShoppingCart>> CreateUpdatePaymentIntent()//string cartId
        {
            // var cart = await _paymentService.CreateOrUpdatePaymentIntent(cartId);
            // if (cart == null) return BadRequest("Problem with your cart");
            // return Ok(cart);
            var buyerId = Request.Cookies["buyerId"];

            if (string.IsNullOrEmpty(buyerId))
                return BadRequest("buyerId cookie missing");

            var cart = await _paymentService.CreateOrUpdatePaymentIntent(buyerId);

            if (cart == null)
                return BadRequest("Problem with your cart");

            return Ok(cart);
        }

        [HttpGet("delivery-methods")]
        public async Task<ActionResult<IReadOnlyList<DeliveryMethod>>> GerDeliveryMethods()
        {
            return Ok(await _dbContext.DeliveryMethods.ToListAsync());
        }

        [HttpPost("webhook")]
        public async Task<IActionResult> StripeWebhook()
        {
            var json = await new StreamReader(Request.Body).ReadToEndAsync();

            try
            {
                var stripeEvent = ConstructStripeEvent(json);

                if (stripeEvent.Type == "payment_intent.succeeded")
                {
                    if (stripeEvent.Data.Object is not PaymentIntent intent)
                    {
                        return BadRequest("Invalid Event Data");
                    }

                    await HanldePaymentIntentSucceeded(intent);
                }

                return Ok();
            }
            catch (StripeException ex)
            {
               return BadRequest(ex.Message);
            }
        }

        private async Task HanldePaymentIntentSucceeded(PaymentIntent intent)
        {
            if (intent.Status == "succeeded")
            {
                var order = await _dbContext.Orders.Include(x => x.OrderItems).Include(x => x.DeliveryMethod).FirstOrDefaultAsync(x => x.PaymentIntentId == intent.Id);

                if (order == null) return;

                if((long) (order.Total * 100) != intent.Amount)
                {
                    order.Status = OrderStatus.PaymentMismatch;
                }
                else
                {
                    order.Status = OrderStatus.PaymentReceived;
                }
                
                await _dbContext.SaveChangesAsync();

                var connectionId = NotificationHub.GetConnectionIdByEEmail(order.BuyerEmail);

                if (!string.IsNullOrEmpty(connectionId))
                {
                    await _hubContext.Clients.Client(connectionId).SendAsync("OrderCompleteNotification", order);
                }

            }
        }

        private Event ConstructStripeEvent(string json)
        {
            try
            {
                return EventUtility.ConstructEvent(json, Request.Headers["Stripe-Signature"], _whSecret);
            }
            catch (Exception ex)
            {
                throw new StripeException("Invalid Signature");
            }
        }
    }
}
