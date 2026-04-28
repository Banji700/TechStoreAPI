namespace TechStoreAPI.Features
{
    public class Pagination<T>
    {
        public Pagination(int pagenum, int pagesize, int count, IReadOnlyList<T> data)
        {
            PageNum= pagenum;
            PageSize=pagesize;
            Count=count;
            Data = data;
        }

        public int PageNum { get; set; } 

        public int PageSize { get; set; } 

        public int Count { get; set; }

        public IReadOnlyList<T> Data { get; set; }
    }
}
