using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using TechStoreAPI.DTOs;
using TechStoreAPI.Interfaces.TechStoreAPI.Interfaces;
using TechStoreAPI.LoginRegister;
using TechStoreAPI.Mapper;

namespace TechStoreAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AccountController : ControllerBase
    {
        private readonly UserManager<AppUser> _userManager;
        private readonly ITokenService _tokenService;
        private readonly SignInManager<AppUser> _signInManager;

        public AccountController(UserManager<AppUser> userManager, ITokenService tokenService, SignInManager<AppUser> signInManager)
        {
            _userManager = userManager;
            _tokenService = tokenService;
            _signInManager = signInManager;
        }

        [HttpPost("login")]
        public async Task <IActionResult> Login(LoginDto loginDto)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            var user = await _userManager.Users.FirstOrDefaultAsync(x => x.Email == loginDto.Email.ToLower());

            if (user == null) return Unauthorized("Invaild Username!");

            var result = await _signInManager.CheckPasswordSignInAsync(user,loginDto.Password, false);

            if (!result.Succeeded) return Unauthorized("Username not found or password incorrect");
            var roles = await _userManager.GetRolesAsync(user);


            return Ok(
                new NewUserDto
                {
                    FirstName = user.FirstName,
                    LastName = user.LastName,
                    UserName = user.Email,
                    Email = user.Email,
                    Token = await _tokenService.CreateToken(user),
                    Roles = roles.ToList()
                }
                );
                
        }

        [HttpPost("register")]
        public async Task <IActionResult> Register([FromBody] RegisterDto registerDto)
        {
            try
            {
                if (!ModelState.IsValid)
                {
                    return BadRequest(ModelState);
                }

                var appUser = new AppUser
                {
                    FirstName = registerDto.FirstName,
                    LastName = registerDto.LastName,
                    Email = registerDto.Email,
                    UserName=registerDto.Email
                };

                var createdUser = await _userManager.CreateAsync(appUser, registerDto.Password);
                

                if (createdUser.Succeeded)
                {
                    var roleResult = await _userManager.AddToRoleAsync(appUser, "Customer");
                    
                    if (roleResult.Succeeded)
                    {
                        var roles = await _userManager.GetRolesAsync(appUser);

                        return Ok
                            (
                               new NewUserDto
                               {
                                   UserName = appUser.Email,
                                   Email = appUser.Email,
                                   FirstName = appUser.FirstName,
                                   LastName = appUser.LastName,
                                   Token = await _tokenService.CreateToken(appUser),
                                   Roles = roles.ToList()
                               }
                            );

                    }
                    else
                    {
                        return StatusCode(500, roleResult.Errors);
                    }
                }
                else
                {
                    return StatusCode(500, createdUser.Errors);
                }

            }catch (Exception e)
            {
                return StatusCode(500, e);
            }

        }

        [HttpPost("address")]
       
        public async Task<IActionResult> CreateOrUpdateAddress(AddressDto addressDto)
        {
            var email = User.FindFirstValue(ClaimTypes.Email);

            if (string.IsNullOrEmpty(email)) return Unauthorized();

            var user = await _userManager.Users
                .Include(x => x.Address)
                .SingleOrDefaultAsync(x => x.Email == email);

            if (user == null) return Unauthorized();

            user.Address = new Address
            {
                Line1 = addressDto.Line1,
                Line2 = addressDto.Line2,
                City = addressDto.City,
                State = addressDto.State,
                PostalCode = addressDto.PostalCode,
                Country = addressDto.Country
            };

            var result = await _userManager.UpdateAsync(user);

            if (!result.Succeeded) return BadRequest("Problem updating user address");

            return Ok(user.Address.ToDto());
        }

        [HttpGet("user-info")]
        public async Task<IActionResult> UserInfo()
        {
            var email = User.FindFirstValue(ClaimTypes.Email);

           // if (User.Identity?.IsAuthenticated == false) return NoContent();

            var user = await _signInManager.UserManager.Users.Include(x => x.Address).FirstOrDefaultAsync(x => x.Email == User.FindFirstValue(ClaimTypes.Email));

            if (user == null) return Unauthorized();

            return Ok(new
            {
                user.FirstName,
                user.LastName,
                user.Email,
                Address = user.Address?.ToDto(),
                Roles = User.FindFirstValue(ClaimTypes.Role)
            });
        }
    }
}
