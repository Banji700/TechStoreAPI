using System.ComponentModel.DataAnnotations;

namespace TechStoreAPI.ShoppingCartFunc
{
    public class ShoppingCart
    {
        public int Id { get; set; }
        public string BuyerId { get; set; } = string.Empty;

        public List<CartItem> Items { get; set; } = [];
    }
}
