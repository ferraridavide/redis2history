import ctypes
import importlib
import multiprocessing
from typing import Any
import factory

from redis import Redis
from models import PipelineModel, RedisSettings, Transforms
from mpipe import OrderedWorker, Stage
from mpipe.TubeQ import TubeQ

class ModuleInterface:

    @staticmethod
    def register() -> OrderedWorker:
        """Register plugin"""

def import_module(name: str) -> ModuleInterface:
    return importlib.import_module(name)

def load_transforms(transforms: list[Transforms], redisConn: Redis, globalArgs: dict) -> None:
    for transform in transforms:
        plugin = import_module(transform.script)
        transform_plugin = plugin.register()

        plugin_args = {'args': globalArgs | ({} if transform.args is None else transform.args), 'redisConn': redisConn}
        tube: TubeQ = TubeQ(maxsize=3) # Back-pressure

        transform_instance = Stage(
            transform_plugin, 
            do_stop_task=True,
            # size=4 if transform.id == 'printer' else 1,
            # input_tube=tube if transform.id == 'printer' else None,
            **(plugin_args))

            
        factory.register(transform.id, transform_instance)


def load_plugin(stageId: str, pluginName: str, workers: int, plugin_args: dict[str, Any]) -> None:
    plugin_wrapper = import_module(pluginName)
    plugin = plugin_wrapper.register()

    plugin_stage = Stage(plugin, do_stop_task=False, size=workers, **(plugin_args))
    factory.register(stageId, plugin_stage)
