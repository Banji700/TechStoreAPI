using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace TechStoreAPI.OrderAggregate
{
    public class OrderItemConfiguration : IEntityTypeConfiguration<OrderItem>
    {
        public void Configure(EntityTypeBuilder<OrderItem> builder)
        {
            builder.OwnsOne(x => x.ProductsOrdered, o => o.WithOwner());
            builder.Property(x => x.Price).HasColumnType("decimal(18,2)");
        }
    }
}

