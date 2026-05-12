using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TechStoreAPI.Data;
using TechStoreAPI.OrderFunction;

namespace TechStoreAPI.Controllers
{
    //[Route("api/[controller]")]
    [Route("api/deliverymethods")]
    [ApiController]
    public class DeliveryMethodsController : ControllerBase
    {
        private readonly ApplicationDBContext _dbcontext;

        public DeliveryMethodsController(ApplicationDBContext dbcontext)
        {
            _dbcontext = dbcontext;
        }

        [HttpGet]
        public async Task<ActionResult<IReadOnlyList<DeliveryMethod>>>GetDeliveryMethods()
        {
            return await _dbcontext.DeliveryMethods.ToListAsync();
        }
    }
}
