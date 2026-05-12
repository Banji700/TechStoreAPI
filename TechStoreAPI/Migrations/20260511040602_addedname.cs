using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace TechStoreAPI.Migrations
{
    /// <inheritdoc />
    public partial class addedname : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "Name",
                table: "ShippingAddress",
                type: "nvarchar(max)",
                nullable: false,
                defaultValue: "");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "Name",
                table: "ShippingAddress");
        }
    }
}
