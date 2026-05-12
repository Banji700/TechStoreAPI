namespace TechStoreAPI.OrderAggregate
{
    public enum OrderStatus
    {
        Pending,

        PaymentReceived,

        PaymentFailed,

        PaymentMismatch
    }
}
