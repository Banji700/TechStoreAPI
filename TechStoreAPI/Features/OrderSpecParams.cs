using TechStoreAPI.OrderAggregate;

namespace TechStoreAPI.Features
{
    public class OrderSpecParams 
    {
        public int PageIndex { get; set; } = 1;

        private int _pageSize = 10;

        public int PageSize
        {
            get => _pageSize;
            set => _pageSize = value > 50 ? 50 : value;
        }

        public string? Filter { get; set; }
    }
}
