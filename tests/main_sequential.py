from multiprocessing.managers import BaseManager
import os
import time
from typing import Any
import loader
import factory
import pickle
import psutil, humanize
from rich.console import Console
import numpy as np
import pickle

from load_config import load_config
from mpipe import Pipeline
from models import PipelineModel
from clients import RedisClient, MongoClient


def sortExecution(pipelines: PipelineModel) -> list[PipelineModel]:
    '''Questa funzione ordina le pipelines in ordine di esecuzione, in base alle dipendenze'''
    return pipelines


if __name__ == '__main__':
    #freeze_support() # --> to ensure that your script runs correctly when compiled to a standalone executable.
    #multiprocessing.set_start_method('spawn') if 'spawn' in multiprocessing.get_all_start_methods() else None
    
    config = load_config('./config.json')



    shared_redis = RedisClient(config.redisSettings)
    shared_mongo = MongoClient(config.mongoSettings)

    console = Console(highlight=False)

    pipeline_results: dict[str, Any] = {}
    pipeline_ids = set([p.id for p in config.pipelines]) # --> ottiene tutti gli ID delle pipeline eg. [pipe1, pipe2, pipe3]
    previous_stage_result = [None]
    for pipeline in sortExecution(config.pipelines):
        with console.status(f"[bold green]Running {pipeline.id} pipeline...") as status:
            start = time.time()
            console.print(f"=> Registering plugins for stages in [bold]{pipeline.id}[/bold] pipeline")
            for stage in pipeline.stages:
                loader.load_plugin(
                    stageId=stage['id'],
                    workers=stage['workers'] if 'workers' in stage else 1,
                    pluginName=stage['script'], 
                    plugin_args={
                        'pipeline_results': pipeline_results,
                        'redis_connection':  shared_redis,
                        'mongo_connection':  shared_mongo,
                        'args': config.globalArgs | (stage['args'] if 'args' in stage else {})
                        })
                    
            console.print(f"=> Running stages in [bold]{pipeline.id}[/bold] pipeline")

            for stage_index, stage in enumerate(pipeline.stages):
                stage_result = []
                worker_stage = factory.get(stage['id'])
                if (stage['id'] == "getVehicleDrivers"):
                    worker_stage._worker_args['driverCache'] = {}
                worker = worker_stage._worker_class(**worker_stage._worker_args)
                worker.putResult = lambda x: stage_result.append(x) if x != None else None
                if stage_index == 0:
                    worker.doTask(previous_stage_result)
                else:
                    for prev in previous_stage_result:
                        worker.doTask(prev)
                
                
                print("done")
                previous_stage_result = stage_result

            end = time.time()
            console.print(f"=> {pipeline.id} pipeline finished in [bold]{humanize.naturaldelta(end-start)}[/bold]")


            process = psutil.Process(os.getpid())
            console.print(f"=> Process is using [bold]{humanize.naturalsize(process.memory_info().rss)}[/bold]")
            console.line()



