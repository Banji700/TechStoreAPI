using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace TechStoreAPI.Migrations
{
    /// <inheritdoc />
    public partial class AddCoupon : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "CouponCode",
                table: "ShoppingCarts",
                type: "nvarchar(max)",
                nullable: true);

            migrationBuilder.AddColumn<int>(
                name: "CouponId",
                table: "ShoppingCarts",
                type: "int",
                nullable: true);

            migrationBuilder.AddColumn<decimal>(
                name: "Discount",
                table: "Orders",
                type: "decimal(18,2)",
                nullable: false,
                defaultValue: 0m);

            migrationBuilder.CreateTable(
                name: "AppCoupon",
                columns: table => new
                {
                    Id = table.Column<int>(type: "int", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    Name = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    AmountOff = table.Column<decimal>(type: "decimal(18,2)", nullable: true),
                    PercentOff = table.Column<decimal>(type: "decimal(18,2)", nullable: true),
                    PromotionCode = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    CouponId = table.Column<string>(type: "nvarchar(max)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AppCoupon", x => x.Id);
                });

            migrationBuilder.CreateIndex(
                name: "IX_ShoppingCarts_CouponId",
                table: "ShoppingCarts",
                column: "CouponId");

            migrationBuilder.AddForeignKey(
                name: "FK_ShoppingCarts_AppCoupon_CouponId",
                table: "ShoppingCarts",
                column: "CouponId",
                principalTable: "AppCoupon",
                principalColumn: "Id");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_ShoppingCarts_AppCoupon_CouponId",
                table: "ShoppingCarts");

            migrationBuilder.DropTable(
                name: "AppCoupon");

            migrationBuilder.DropIndex(
                name: "IX_ShoppingCarts_CouponId",
                table: "ShoppingCarts");

            migrationBuilder.DropColumn(
                name: "CouponCode",
                table: "ShoppingCarts");

            migrationBuilder.DropColumn(
                name: "CouponId",
                table: "ShoppingCarts");

            migrationBuilder.DropColumn(
                name: "Discount",
                table: "Orders");
        }
    }
}
