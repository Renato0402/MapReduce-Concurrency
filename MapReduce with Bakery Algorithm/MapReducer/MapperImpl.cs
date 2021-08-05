using System.Collections.Generic;
using System;
using System.Threading;
using System.Linq;
using System.Collections.Concurrent;

namespace MapReducer
{
    class MapperImpl<IMK, IMV, OMK, OMV, MF> : IMapper<IMK, IMV, OMK, OMV, MF> where MF : IMapFunction<IMK, IMV, OMK, OMV>
    {

        private readonly IDictionary<IMK, List<IMV>> _input = new ConcurrentDictionary<IMK, List<IMV>>();
        private IDictionary<IMK, List<IMV>> Input => _input;
        private MF mapfunction = default;

        public MF Function { get { return mapfunction; } set { mapfunction = value; } }

        public MapperImpl(MF mf)
        {
            this.Function = mf;
        }

        public void ReceiveInputPair(Pair<IMK, IMV> pair)
        {// 1|"verde branco azul", 5|"rosa branco preto"
            if (!this.Input.TryGetValue(pair.Key, out List<IMV> ivs))
            {
                ivs = new List<IMV>();
                Input.Add(pair.Key, ivs);
            }
            ivs.Add(pair.Value);
        }

        public void Compute()
        {
            var localBakeryAccess = SplitterImpl<IMK, IMV, OMK, OMV, MF>.bakery;
            var Output = DataFeeder<OMK, OMV>.DataFeed;

            foreach (var kv in Input)
            {
                IMK chave = kv.Key;
                List<IMV> valores = kv.Value;
                foreach (IMV valor in valores)
                {
                    List<Pair<OMK, OMV>> pares = Function.Run(chave, valor);

                    foreach (Pair<OMK, OMV> par in pares)
                    {
                        localBakeryAccess.prepare(Convert.ToInt32(Thread.CurrentThread.Name));

                        if (!Output.TryGetValue(par.Key, out List<OMV> omvs))
                        {
                            omvs = new List<OMV>();

                            Output.Add(par.Key, omvs);
                        }

                        omvs.Add(par.Value);

                        if (Function is IMapFunctionCombiner<IMK, IMV, OMK, OMV>)
                        {
                            IMapFunctionCombiner<IMK, IMV, OMK, OMV> mf = (IMapFunctionCombiner<IMK, IMV, OMK, OMV>)Function;
                            OMV omv = mf.Combiner(omvs);
                            omvs.Clear();
                            omvs.Add(omv);
                        }

                        localBakeryAccess.discardTicket(Convert.ToInt32(Thread.CurrentThread.Name));
                    }
                }
            }
        }


        /*public void Compute(object o)
        {
            var Output = DataFeeder<OMK, OMV>.DataFeed;

            foreach (var kv in Input)
            {
                IMK chave = kv.Key;
                List<IMV> valores = kv.Value;
                foreach (IMV valor in valores)
                {
                    List<Pair<OMK, OMV>> pares = Function.Run(chave, valor);


                    foreach (Pair<OMK, OMV> par in pares)
                    {


                        if (!Output.TryGetValue(par.Key, out List<OMV> omvs))
                        {
                            omvs = new List<OMV>();

                            Output.Add(par.Key, omvs);
                        }

                        omvs.Add(par.Value);

                        if (Function is IMapFunctionCombiner<IMK, IMV, OMK, OMV>)
                        {
                            IMapFunctionCombiner<IMK, IMV, OMK, OMV> mf = (IMapFunctionCombiner<IMK, IMV, OMK, OMV>)Function;
                            OMV omv = mf.Combiner(omvs);
                            omvs.Clear();
                            omvs.Add(omv);
                        }
                    }
                }
            }
        }
    }*/

        /*public void Compute()
        {

            var Output = DataFeeder<OMK, OMV>.DataFeed;

            foreach (var kv in Input)
            {
                IMK chave = kv.Key;
                List<IMV> valores = kv.Value;
                foreach (IMV valor in valores)
                {
                    List<Pair<OMK, OMV>> pares = Function.Run(chave, valor);


                    foreach (Pair<OMK, OMV> par in pares)
                    {
                        lock (Output)
                        {

                            if (!Output.TryGetValue(par.Key, out List<OMV> omvs))
                            {
                                omvs = new List<OMV>();

                                Output.Add(par.Key, omvs);
                            }

                            omvs.Add(par.Value);

                            if (Function is IMapFunctionCombiner<IMK, IMV, OMK, OMV>)
                            {
                                IMapFunctionCombiner<IMK, IMV, OMK, OMV> mf = (IMapFunctionCombiner<IMK, IMV, OMK, OMV>)Function;
                                OMV omv = mf.Combiner(omvs);
                                omvs.Clear();
                                omvs.Add(omv);
                            }
                        }
                    }
                }
            }
        }*/
    }
}
