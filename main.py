"""
Executes the pipelines defined in the configuration file.
"""

from multiprocessing import freeze_support
from multiprocessing.managers import BaseManager
import multiprocessing
import os
import pickle
import sys
import time
from typing import Any
import argparse
import re
import datetime
from rich.console import Console
from mpipe import Pipeline
from redis import Redis
import loader
import factory

from load_config import load_config
from models import Config
from clients import MongoClientProxy, RedisClient, MongoClient, RedisClientProxy, ReacquiringLock

from analyzer import analize_pipeline, discard_retired_pipelines


def format_string(string: str, global_args: dict[str, str]) -> str:
    """
    Formats a string with date, environment variables and global variables.

    Args:
        string (str): The input string with placeholders.
        globalArgs (dict[str, str]): A dictionary of global arguments.

    Returns:
        str: The formatted string with replaced placeholders.
    """

    def replace_date(match):
        format_str = match.group(1)
        today = datetime.datetime.now()
        formatted_date = today.strftime(format_str) # Example: {{date:%Y-%m-%d}}
        return formatted_date

    def replace_env(match):
        env_var = match.group(1)
        return os.environ.get(env_var, "")

    def replace_global(match):
        global_var = match.group(1)
        return global_args.get(global_var, "")

    string = re.sub(r"{{date:(.*?)}}", replace_date, string)
    string = re.sub(r"{{env:(.*?)}}", replace_env, string)
    string = re.sub(r"{{global:(.*?)}}", replace_global, string)

    return string

def redis_lock(config: Config) -> ReacquiringLock:
    """
    Acquires a Redis lock based on the provided configuration.

    Args:
        config (Config): The configuration object containing Redis settings.

    Returns:
        ReacquiringLock: The acquired Redis lock.
    """
    if config.redisSettings.lock is None:
        return None
    redis_instance = Redis(host=config.redisSettings.host,
                       port=config.redisSettings.port,
                       ssl=config.redisSettings.tls,
                       decode_responses=True)
    lock_str = format_string(config.redisSettings.lock, config.globalArgs)
    print(lock_str)
    lock = redis_instance.lock(lock_str, timeout=config.redisSettings.lockTimeout, lock_class=ReacquiringLock, thread_local=False)
    if lock.locked():
        print("Redis is locked, waiting...")
    lock.acquire()
    print("Redis lock acquired, proceeding...")
    return lock

def load_plugin(config, shared_redis, shared_mongo, pipeline_results, stage):
    """
    Load a plugin with the given configuration and shared objects.

    Args:
        config (dict): Configuration settings.
        shared_redis (RedisConnection): Redis connection object.
        shared_mongo (MongoConnection): MongoDB connection object.
        pipeline_results (dict): Results of the pipeline.
        stage (Stage): Stage object.

    Returns:
        None
    """
    loader.load_plugin(
        stageId=stage.id,
        workers=stage.workers or 1,
        pluginName=stage.script,
        plugin_args={
            'pipeline_results': pipeline_results,
            'redis_connection': shared_redis,
            'mongo_connection': shared_mongo,
            'args': config.globalArgs | (stage.args or {})
        }
    )

def main(config_file: str, manual_order: bool, export_run: bool, dry_run: bool):

    # Ensures that the script can be run as a frozen executable.
    freeze_support()

    # This line sets the start method for creating new processes in the multiprocessing module.
    # It uses the 'spawn' method if it is available, otherwise it does nothing.
    if 'spawn' in multiprocessing.get_all_start_methods():
        multiprocessing.set_start_method('spawn')


    config = load_config(config_file)
    lock = redis_lock(config)


    ordered_pipelines = config.pipelines if manual_order else analize_pipeline(config.pipelines)
    print("Plan of execution:")
    print(" -> ".join([p.id for p in ordered_pipelines]))

    export_dir = "run_" + time.strftime("%Y%m%d-%H%M%S")
    if export_run:
        os.mkdir(export_dir)

    # Registering the shared objects
    BaseManager.register('RedisClient', RedisClient, RedisClientProxy)
    BaseManager.register('MongoClient', MongoClient, MongoClientProxy)

    manager = BaseManager()
    manager.start()

    shared_redis = manager.RedisClient(config.redisSettings)
    shared_mongo = manager.MongoClient(config.mongoSettings)

    console = Console(highlight=False)

    pipeline_results: dict[str, Any] = {}
    pipeline_ids = {p.id for p in ordered_pipelines}

    if dry_run:
        for pipeline in ordered_pipelines:
            print(f"Registering plugins in {pipeline.id}...")
            for stage in pipeline.stages:
                load_plugin(config, shared_redis, shared_mongo, pipeline_results, stage)
                print(f"==> {stage.id}")
        print("Dry run completed successfully.")
        sys.exit()


    for pipeline in ordered_pipelines:
        # process = psutil.Process(os.getpid())
        # console.print(f"=> Process is using {humanize.naturalsize(process.memory_info().rss)}")
        # console.line()

        console.print(f"=> Registering plugins for stages in [bold]{pipeline.id}[/bold] pipeline")
        for stage in pipeline.stages:
            load_plugin(config, shared_redis, shared_mongo, pipeline_results, stage)

        console.print(f"=> Running stages in [bold]{pipeline.id}[/bold] pipeline")
        start = time.time()

        if pipeline.parallel:
            for source_stage, next_stage in zip(pipeline.stages, pipeline.stages[1:]):
                factory.get(source_stage.id).link(factory.get(next_stage.id))

            first_stage = pipeline.stages[0]
            pipe = Pipeline(factory.get(first_stage.id))


            pipe_input = pipeline_results[pipeline.source] if pipeline.source is not None and pipeline.source in pipeline_ids else [""]
            if pipeline.bulkSource:
                pipe.put(pipe_input)
            else:
                for chunk in pipe_input:
                    pipe.put(chunk)
            pipe.put(None)

            res = list(pipe.results())
            pipeline_results[pipeline.id] = res
            if export_run:
                with open(f"{export_dir}/{pipeline.id}.pickle", 'wb') as handle:
                    pickle.dump(res, handle, protocol=pickle.HIGHEST_PROTOCOL)

        else:
            prev_stage_result = pipeline_results[pipeline.source] if pipeline.source is not None and pipeline.source in pipeline_ids else [None]
            for stage in pipeline.stages:
                stage_result = []
                worker_stage = factory.get(stage.id)
                worker = worker_stage._worker_class(**worker_stage._worker_args)
                worker.putResult = lambda x: stage_result.append(x) if x is not None else None
                if stage == pipeline.stages[0] and pipeline.bulkSource:
                    worker.doTask(prev_stage_result)
                else:
                    for value in prev_stage_result:
                        worker.doTask(value)
                prev_stage_result = stage_result

                if export_run:
                    with open(f"{export_dir}/{pipeline.id}_{stage.id}.pickle", 'wb') as handle:
                        pickle.dump(stage_result, handle, protocol=pickle.HIGHEST_PROTOCOL)

            pipeline_results[pipeline.id] = prev_stage_result
        end = time.time()
        console.print(f"=> {pipeline.id} pipeline finished in [bold]{end-start:.2f}s[/bold]")



        #discard_retired_pipelines(ordered_pipelines, pipeline_results, pipeline)

    if lock is not None:
        lock.release()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='redis2history')
    parser.add_argument('-c', '--config', type=str, help='Config file path', required=True)
    parser.add_argument('-m', '--manual_order', action='store_true', help='Runs pipelines in the order they are defined in the config file, instead of the order determined by the analyzer.')
    parser.add_argument('-e', '--export_run', action='store_true', help='Save results of each pipeline stage to a file')
    parser.add_argument('-dr', '--dry_run', action='store_true', help='Checks the config file and pipeline structure without running the pipelines')
    args = parser.parse_args()

    main(config_file=args.config, manual_order=args.manual_order, export_run=args.export_run, dry_run=args.dry_run)
