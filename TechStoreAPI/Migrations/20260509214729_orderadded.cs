using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace TechStoreAPI.Migrations
{
    /// <inheritdoc />
    public partial class orderadded : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_OrderItems_ProductsOrdered_productsOrderedId",
                table: "OrderItems");

            migrationBuilder.RenameColumn(
                name: "Year",
                table: "PaymentSummary",
                newName: "ExpYear");

            migrationBuilder.RenameColumn(
                name: "productsOrderedId",
                table: "OrderItems",
                newName: "ProductsOrderedId");

            migrationBuilder.RenameIndex(
                name: "IX_OrderItems_productsOrderedId",
                table: "OrderItems",
                newName: "IX_OrderItems_ProductsOrderedId");

            migrationBuilder.AddForeignKey(
                name: "FK_OrderItems_ProductsOrdered_ProductsOrderedId",
                table: "OrderItems",
                column: "ProductsOrderedId",
                principalTable: "ProductsOrdered",
                principalColumn: "Id",
                onDelete: ReferentialAction.Cascade);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_OrderItems_ProductsOrdered_ProductsOrderedId",
                table: "OrderItems");

            migrationBuilder.RenameColumn(
                name: "ExpYear",
                table: "PaymentSummary",
                newName: "Year");

            migrationBuilder.RenameColumn(
                name: "ProductsOrderedId",
                table: "OrderItems",
                newName: "productsOrderedId");

            migrationBuilder.RenameIndex(
                name: "IX_OrderItems_ProductsOrderedId",
                table: "OrderItems",
                newName: "IX_OrderItems_productsOrderedId");

            migrationBuilder.AddForeignKey(
                name: "FK_OrderItems_ProductsOrdered_productsOrderedId",
                table: "OrderItems",
                column: "productsOrderedId",
                principalTable: "ProductsOrdered",
                principalColumn: "Id",
                onDelete: ReferentialAction.Cascade);
        }
    }
}
