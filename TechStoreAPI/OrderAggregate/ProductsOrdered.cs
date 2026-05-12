namespace TechStoreAPI.OrderAggregate
{
    public class ProductsOrdered
    {
        public int Id { get; set; }
        public int ProductId { get; set; }

        public required string ProductName { get; set; }
        public required string ImageUrl { get; set; }
    }
}
