using System;
using System.Collections.Generic;
using System.Text;

namespace MapReduce.HashTable
{
    public class No<K, V>
    {
        private K key;
        private V value;

        public No(K key, V value)
        {
            this.key = key;
            this.value = value;
        }

        public K getKey()
        {
            return key;
        }

        public void setKey(K key)
        {
            this.key = key;
        }

        public V getValue()
        {
            return value;
        }

        public void setValue(V value)
        {
            this.value = value;
        }
    }
}
