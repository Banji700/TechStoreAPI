namespace TechStoreAPI.Features
{
    public class QueryObjects
    {
      public string Brand { get; set; } = string.Empty;
      public string Category { get; set; } = string.Empty;
      public string Sort { get; set; } = string.Empty;
      
      public int PageNum { get; set; } = 1;
      
      public int PageSize { get; set; } = 10;
      
      public string Search {  get; set; } = string.Empty;
       
    }
}
