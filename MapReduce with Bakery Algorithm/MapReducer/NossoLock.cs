
namespace MapReduce.WordCount
{
    public class NossoLock
    {
        public bool[] flags;
        public int[] tickets;
        public int n = 0;

        public void load(int n)
        {
            this.n = n;
            this.flags = new bool[n];
            this.tickets = new int[n];
        }
    }
}
