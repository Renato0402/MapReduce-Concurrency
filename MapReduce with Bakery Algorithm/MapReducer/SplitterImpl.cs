using MapReduce.Bakery;
using MapReduce.WordCount;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace MapReducer {
    public class SplitterImpl<IMK, IMV, OMK, OMV, MF> : ISplitter<IMK, IMV, OMK, OMV, MF> where MF : IMapFunction<IMK, IMV, OMK, OMV> {
        public static Bakery bakery = new Bakery();
        
        public DataFeeder<OMK, OMV> Splitting(int n, MF mf, IEnumerable<Pair<IMK, IMV>> enumerable) {
            var threads = new List<Thread>();
            var mappers = new ConcurrentDictionary<int, IMapper<IMK, IMV, OMK, OMV, MF>>();
            IEnumerator<Pair<IMK, IMV>> dados = enumerable.GetEnumerator();

            int i = 0;
            while (dados.MoveNext()) {
                IMapper<IMK, IMV, OMK, OMV, MF> mapper;

                int partition = i++ % n;
                if (!mappers.TryGetValue(partition, out mapper)) {
                    mapper = new MapperImpl<IMK, IMV, OMK, OMV, MF>(mf);
                    //mappers.Add(partition, mapper);
                    mappers.TryAdd(partition, mapper);
                }
                mapper.ReceiveInputPair(new Pair<IMK, IMV>(dados.Current.Key, dados.Current.Value));
            }

            foreach (var kv in mappers) {
                lock (DataFeeder<OMK, OMV>.DataFeed)
                {
                    Thread t = new Thread(kv.Value.Compute);

                    threads.Add(t);
                }
            }

            int cont = 0;

            foreach (Thread t in threads)
            {
                t.Start();
                t.Name = cont.ToString();

                cont++;
            }

            foreach (Thread t in threads)
                t.Join();

            return new DataFeeder<OMK, OMV>();
        }
    }
}
