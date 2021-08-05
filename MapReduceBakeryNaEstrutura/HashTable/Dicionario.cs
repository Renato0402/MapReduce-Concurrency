using System;
using System.Collections;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using MapReduce.Bakery;


namespace MapReduce
{
    public class Dicionario<K, V>
    {
        private int MAX;
        private NO<K, V>[] nodes = null;
        public Bakery.Bakery bakery = BakeryInstance.bakery;

        public Dicionario()
        {
            init(8);
        }
        public Dicionario(int n)
        {
            init(n);
        }

        private void init(int n)
        {
            MAX = n;
            nodes = new NO<K, V>[MAX];
            for (int i = 0; i < MAX; i++)
                nodes[i] = null;
        }
        private int hash(K key)
        {
            if (key.GetHashCode() < 0)
            {
                int aux = (key.GetHashCode() * -1) % MAX;

                if (aux < 0)
                {

                    return aux * -1;
                }
                else
                {
                    return aux;
                }
            }
            else
            {
                if (key.GetHashCode() % MAX < 0)
                {
                    return (key.GetHashCode() % MAX) * -1;
                }
                else
                {
                    return key.GetHashCode() % MAX;
                }
            }
        }
        public void add(K key, V value)
        {
            bakery.prepare(Convert.ToInt32(Thread.CurrentThread.Name));

            int k = hash(key);
            nodes[k] = addNO(nodes[k], key, value);

            bakery.discardTicket(Convert.ToInt32(Thread.CurrentThread.Name));
        }
        private NO<K, V> addNO(NO<K, V> head, K key, V value)
        {
            if (head == null)
            {
                head = new NO<K, V>(key, value);
                return head;
            }

            NO<K, V> h = head;

            while (h.next != null)
            {
                if (h.key.Equals(key)) return head;
                h = h.next;
            }

            h.next = new NO<K, V>(key, value);

            return head;
        }
        public void del(K key)
        {
            int k = hash(key);
            nodes[k] = deleteGetHeadNO(nodes[k], key);
        }
        private NO<K, V> deleteGetHeadNO(NO<K, V> head, K key)
        {
            if (head == null) return null;
            if (head.key.Equals(key))
            {
                NO<K, V> n = head.next;
                //free(head);
                return n;
            }
            NO<K, V> del = head;//pode ser head.next
            NO<K, V> ant = head;
            while (del != null)
            {
                if (del.key.Equals(key))
                    break;
                ant = del;
                del = del.next;
            }
            if (del != null)
            {
                ant.next = del.next;
                //free(del);
            }
            return head;
        }
        public void toStringHash()
        {
            for (int k = 0; k < MAX; k++)
            {
                Console.WriteLine("index(%d): ", k);
                if (nodes[k] != null)
                    toStringNO(nodes[k]);
                Console.WriteLine("\n");
            }
        }
        public void toStringNO(NO<K, V> head)
        {
            NO<K, V> end = head;
            while (end != null)
            {
                Console.WriteLine("[%s, %s] ", end.key.GetHashCode(), end.value.GetHashCode());
                end = end.next;
            }
        }
        public V valor(K key)
        {
            int k = hash(key);
            return valueNO(nodes[k], key);

        }
        private V valueNO(NO<K, V> head, K key)
        {
            NO<K, V> end = head;
            while (end != null)
            {
                if (end.key.Equals(key)) return end.value;
                end = end.next;
            }
            return default(V);
        }

        public List<NO<K, V>> getTable()
        {
            bakery.prepare(Convert.ToInt32(Thread.CurrentThread.Name));

            List<NO<K, V>> table = new List<NO<K, V>>();

            for (int i = 0; i < nodes.Length; i++)
            {
                if (nodes[i] != null)
                {
                    NO<K, V> h = nodes[i];

                    table.Add(h);

                    while (h.next != null)
                    {
                        table.Add(h);

                        h = h.next;
                    }
                }
            }

            bakery.discardTicket(Convert.ToInt32(Thread.CurrentThread.Name));

            return table;

        }

        public bool TryGetValue(K key, out V value)
        {
            bakery.prepare(Convert.ToInt32(Thread.CurrentThread.Name));

            value = valor(key);

            if (value == null)
            {
                bakery.discardTicket(Convert.ToInt32(Thread.CurrentThread.Name));

                return false;
            }

            else
            {
                bakery.discardTicket(Convert.ToInt32(Thread.CurrentThread.Name));

                return true;
            }
        }

        public class NO<TKey, TValue>
        {
            public TKey key;
            public TValue value;
            public NO<TKey, TValue> next = null;
            public NO(TKey key, TValue value)
            {
                this.key = key;
                this.value = value;
            }
        }
    }
}
