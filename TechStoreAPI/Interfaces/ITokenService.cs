using TechStoreAPI.LoginRegister;

namespace TechStoreAPI.Interfaces
{
    namespace TechStoreAPI.Interfaces 
    {
      public interface ITokenService
      {
            string CreateToken(AppUser user);
      }
    }
}
