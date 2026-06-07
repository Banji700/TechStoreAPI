using TechStoreAPI.ShoppingCartFunc;

namespace TechStoreAPI.Interfaces
{
    public interface IPaymentService
    {
        Task<ShoppingCart?> CreateOrUpdatePaymentIntent(string cartId);

        Task<string> RefundPayment(string paymentIntentId);
    }
}
