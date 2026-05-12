using TechStoreAPI.DTOs;
using TechStoreAPI.LoginRegister;

namespace TechStoreAPI.Mapper
{
    public static class AddressMapper
    {
        public static AddressDto? ToDto(this Address address)
        {
            if (address == null) return null;
            return new AddressDto
            {
                Line1 = address.Line1,
                Line2 = address.Line2,
                City = address.City,
                State = address.State,
                Country = address.Country,
                PostalCode = address.PostalCode
                

            };
        }

        public static Address ToEntity(this AddressDto dto)
        {
            return new Address
            {
                Line1 = dto.Line1,
                Line2 = dto.Line2,
                City = dto.City,
                State = dto.State,
                Country = dto.Country,
                PostalCode = dto.PostalCode


            };
        }
    }
}
