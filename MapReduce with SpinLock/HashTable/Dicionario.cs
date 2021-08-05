using System;
using System.Collections;
using System.Collections.Generic;

using System.Text;
using System.Threading;


namespace MapReducer
{
    public sealed class Dicionario<K, V>
    {

        private List<Tuple<K, V>> tabela;
        static SpinLock _spinlock = new SpinLock();

        public Dicionario()
        {
            tabela = new List<Tuple<K, V>>();
        }

        public void add(K key, V value)

        {
            bool lockTaken = false;
            try
            {

                _spinlock.Enter(ref lockTaken);
                tabela.Add(new Tuple<K, V>(key, value));


            }
            finally
            {
                if (lockTaken) _spinlock.Exit(false);
            }


        }

        public V get(K key)
        {



            if (tabela.Count == 0)
            {
                return default(V);
            }


            for (int i = 0; i < tabela.Count; i++)
            {

                if (tabela[i].Item1.Equals(key))
                {


                    return tabela[i].Item2;
                }
            }


            return default(V);
        }

        public List<Tuple<K, V>> getTable()
        {
            bool lockTaken = false;
            try
            {

                _spinlock.Enter(ref lockTaken);
                return tabela;
            }
            finally
            {
                if (lockTaken) _spinlock.Exit(false);
            }
        }

        public bool TryGetValue(K key, out V value)
        {
            bool lockTaken = false;
            try
            {

                _spinlock.Enter(ref lockTaken);

                value = get(key);


                if (value == null)
                {


                    return false;
                }


                else
                {


                    return true;
                }
            }
            finally
            {
                if (lockTaken) _spinlock.Exit(false);
            }
        }
    }

}

