using TechStoreAPI.LoginRegister;

namespace TechStoreAPI.Interfaces
{
    namespace TechStoreAPI.Interfaces 
    {
      public interface ITokenService
      {
            Task <string> CreateToken(AppUser user);
      }
    }
}
