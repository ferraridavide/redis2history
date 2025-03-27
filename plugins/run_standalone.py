def init_argparser():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--args', '-a', type=str, nargs='*',
                        help="Key pattern to be scanned on Redis")
    parser.add_argument('--config', '-c', type=str, required=True,
                        help="Path to the JSON configuration file")
    parser.add_argument('--input', '-i', type=str,
                        help="Pickle file to use as worker input")
    parser.add_argument('--iterate_list', '-iter', type=bool,
                        help="Specifies whether the input is to be interpreted as a list or as an item")
    return parser


def run_standalone(WorkerClass, iterate_list = False):
    parser = init_argparser()
    args = parser.parse_args()
    import sys
    sys.path.append('./')
    from load_config import load_config
    from clients import RedisClient
    config = load_config(args.config)
    worker_args = {key: value for key, value in [kv_pair.split('=') for kv_pair in args.args]} if args.args else {}
    redis = RedisClient(config.redisSettings)
    worker = WorkerClass(**{
        'redis_connection': redis,
        'args': worker_args})
    results = []
    worker.putResult = lambda x: results.append(x) if x != None else None
    import pickle, os, datetime
    if args.input:
        with open(args.input, 'rb') as handle:
            data = pickle.load(handle)
            if iterate_list or args.iterate_list:
                for item in data:
                    worker.doTask(item)
            else:
                worker.doTask(data)
    else:
        worker.doTask(None)
    out_name = WorkerClass.__name__ + "_" +  datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + ".pickle"
    with open(out_name, 'wb') as handle:
         pickle.dump(results, handle, protocol=pickle.HIGHEST_PROTOCOL)
    return results
