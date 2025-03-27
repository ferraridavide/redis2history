import json
from models import Config, MongoSettings, PipelineModel, RedisSettings, Transforms



def load_config(config_file_path: str) -> Config:
    with open(config_file_path) as config_file:
        data = json.load(config_file)
        
        redisDict: dict = data['redis']
       
        redisSettings = RedisSettings(**redisDict)

        mongoDict: dict = data['mongo']
        mongoSettings = MongoSettings(**mongoDict)
        
        # pipelines = [PipelineModel(**x) for x in data['pipelines']]
        pipelines = [PipelineModel(id=x['id'], 
                                   source=x.get('source', None),
                                   stages=[Transforms(**y) for y in x['stages']], 
                                   bulkSource=x.get('bulkSource', False), 
                                   parallel=x.get('parallel', True)) 
             for x in data['pipelines']]


        globalArgs:dict = data['global'] 

        return Config(redisSettings, mongoSettings, globalArgs, pipelines)