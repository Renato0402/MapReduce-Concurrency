using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MapReduce.Bakery
{
    public class Bakery
    {
        public bool[] flags;
        public int[] tickets;
        public int n = 10;

        public Bakery()
        {
            this.flags = new bool[n];
            this.tickets = new int[n];
        }
        public void load(int n)
        {
            this.n = n;
            this.flags = new bool[n];
            this.tickets = new int[n];
        }

        public void prepare(int id)
        {
            if (id >= flags.Length)
            {
                updateSizes(id);
            }

            flags[id] = true;

            tickets[id] = tickets.Max() + 1;

            flags[id] = false;

            for (int i = 0; i < n; i++)
            {
                while (flags[i]) ;

                while (tickets[i] != 0 && (tickets[i] < tickets[id]
                        || tickets[id] == tickets[i] && i < id)) ;
            }
        }

        public void discardTicket(int id)
        {
            if (id >= flags.Length)
            {
                updateSizes(id);
            }

            tickets[id] = 0;
        }

        public void updateSizes(int n)
        {
            this.n = n;
            bool[] tempFlags = new bool[n];
            int[] tempTickets = new int[n];

            int cont = 0;

            for (int i = 0; i < flags.Length; i++, cont++)
            {
                if (cont == flags.Length)
                {
                    break;
                }

                tempFlags[cont] = flags[i];
                tempTickets[cont] = tickets[i];
            }

            flags = tempFlags;
            tickets = tempTickets;
        }
    }
}
