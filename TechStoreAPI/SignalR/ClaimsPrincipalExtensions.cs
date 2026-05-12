using System.Security.Claims;


namespace TechStoreAPI.SignalR

{
    public static class ClaimsPrincipalExtensions
    {
        public static string? GetEmail(this ClaimsPrincipal user)
        {
            return user.FindFirstValue(ClaimTypes.Email);
        }
    }
}
