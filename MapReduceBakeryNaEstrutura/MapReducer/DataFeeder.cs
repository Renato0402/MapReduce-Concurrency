using MapReduce;
using MapReduce.HashTable;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace MapReducer {
    public class DataFeeder<OMK, OMV> {
        //private static readonly ConcurrentDictionary<OMK, List<OMV>> DATA = new ConcurrentDictionary<OMK, List<OMV>>();
        //public static ConcurrentDictionary<OMK, List<OMV>> DataFeed { get { return DATA; } }

        private static readonly Dicionario<OMK, List<OMV>> DATA = new Dicionario<OMK, List<OMV>>();
        public static Dicionario<OMK, List<OMV>> DataFeed { get { return DATA; } }

        public void PrintDataFeed() {
            foreach (var kv in DataFeed.getTable()) {
                Console.Write("KEY: " + kv.key + "[ ");

                foreach (OMV val in kv.value) {
                    Console.Write(val + " ");
                }

                Console.WriteLine("]");
            }
        }
    }
}

