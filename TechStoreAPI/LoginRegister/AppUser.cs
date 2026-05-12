using Microsoft.AspNetCore.Identity;
using System.ComponentModel.DataAnnotations;

namespace TechStoreAPI.LoginRegister
{
    public class AppUser : IdentityUser
    {
        public string? FirstName { get; set; }
        
        public string? LastName { get; set; }

        public Address? Address { get; set; }
    }
}
