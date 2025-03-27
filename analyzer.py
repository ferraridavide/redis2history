from collections import defaultdict
import networkx as nx
from rich.tree import Tree
from rich import print as richprint
from load_config import load_config
from models import PipelineModel, Transforms
from typing import Any
import gc

#TEMP
import os
import psutil
from time import sleep

def analize_config(transforms: list[Transforms]) -> None:
    
    dep_graph = nx.DiGraph()

    # TODO: Check for id collisions
    ids = [t.id for t in transforms]
    dups = set([x for x in ids if ids.count(x) > 1])
    if dups:
        print('Duplicate IDs found: {d}'.format(d=', '.join(['{d}(x{n})'.format(d=d, n=ids.count(d)) for d in dups])))
        quit()

    # TODO: Check for non-existing sources
    no_source = [x for x in transforms if x.source not in ids and x.source is not None]
    if no_source:
        print('Transforms with non-existing sources found: {d}'.format(d=', '.join([x.id for x in no_source])))



    for t in transforms:
        if t.source:
            dep_graph.add_edge(t.source, t.id)

    # Check for graph cycles
    cycles:list[str] = list(nx.simple_cycles(dep_graph))
    if len(cycles):
        print("Recursive cycles have been detected:")
        for i, cycle in enumerate(cycles, start=1):
            print('\t{i}: {cycle}'.format(i=i, cycle= ' -> '.join(cycle + [cycle[0]])))
        quit()

    roots = set([t.id for t in transforms if t.source == None])
    for s in roots:
        depth = {}
        for prereq, target in nx.dfs_edges(dep_graph, s):
            prereq_tree = depth.setdefault(prereq, Tree(prereq))
            prereq_tree.add(depth.setdefault(target, Tree(target)))
        richprint(depth[s])

def analize_pipeline(pipelines: list[PipelineModel]):
    """
    Analyzes the given list of pipelines and returns the ordered pipelines based on their dependencies.

    Args:
        pipelines (list[PipelineModel]): A list of PipelineModel objects representing the pipelines.

    Returns:
        list[PipelineModel]: The ordered list of pipelines based on their dependencies.

    Raises:
        ValueError: If a duplicate pipeline id is found.
        ValueError: If a pipeline source is not found.
        ValueError: If a duplicate stage id is found in a pipeline.
        ValueError: If the pipelines are not connected, i.e., there are disconnected subgraphs.
        ValueError: If a circular dependency is found.
    """
    G = nx.DiGraph()

    pipeline_ids = set()
    stage_ids = defaultdict(set)

    for pipeline in pipelines:
        if pipeline.id in pipeline_ids:
            raise ValueError(f"Duplicate pipeline id found: {pipeline.id}")
        pipeline_ids.add(pipeline.id)
        G.add_node(pipeline.id)
        if pipeline.source is not None:
            G.add_edge(pipeline.source, pipeline.id) # Since this is a directed graph, the order of the arguments is important

    for pipeline in pipelines:
        if pipeline.source is not None and pipeline.source not in pipeline_ids:
            raise ValueError(f"Pipeline {pipeline.id} source {pipeline.source} not found")
        for stage in pipeline.stages:
            if stage.id in stage_ids[pipeline.id]:
                raise ValueError(f"Duplicate stage id found in pipeline {pipeline.id}: {stage.id}")
            stage_ids[pipeline.id].add(stage.id)


    if not nx.is_weakly_connected(G):
        raise ValueError("Pipelines are not connected, there are disconnected subgraphs")

 
    if cycles := list(nx.simple_cycles(G)):
        raise ValueError(f"Circular dependency found: {cycles}")

    root_node = next(n for n, d in G.in_degree() if d==0)
    ordered_nodes = list(nx.dfs_preorder_nodes(G, root_node))
    ordered_pipelines = [pipeline for node in ordered_nodes for pipeline in pipelines if pipeline.id == node]
    return ordered_pipelines

def discard_retired_pipelines(pipelines: list[PipelineModel],pipeline_data: dict[str, Any], currentPipeline: PipelineModel):
    """
    After each pipeline execution, some previously executed pipelines may not be needed anymore, the data they contain can be discarded
    This is a sort of garbage collector for pipelines
    """
    current_index = next((index for index, obj in enumerate(pipelines) if obj.id == currentPipeline.id), None)
    for active_pipeline in [item for item in pipeline_data.keys() if item != currentPipeline.id]: # For any pipeline available that is not the current one...
        if not any(pipeline.source == active_pipeline for pipeline in pipelines[current_index+1:]): # Check if no other subsequent pipeline depends on it...

            pipeline_data[active_pipeline].clear()
            del pipeline_data[active_pipeline]

            gc.collect()




config = load_config('./configs/memory_demo.json')
pipelines = config.pipelines
analize_pipeline(pipelines)
