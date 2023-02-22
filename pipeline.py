#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan  9 23:15:11 2023

@author: andara
"""

from streamsets.sdk import ControlHub
import re

credential_id = "xxxx
token = "xxxx"

def get_sch():
   sch = ControlHub(credential_id=credential_id, token=token)
   return sch

def get_pipelines():
    sch = get_sch()
    pipeline_data = sch.pipelines.get_all()
    return pipeline_data

def select_pipeline_name(name):
    for pipeline in get_pipelines():
        if pipeline.name == name:
            return pipeline

def select_pipeline(pipeline_identifier):
    for pipeline in get_pipelines():
        if pipeline.pipeline_id == pipeline_identifier or pipeline.name == pipeline_identifier:
            return pipeline

def get_pipeline_names():
    return [pipeline.name for pipeline in get_pipelines()]

def get_pipeline_stages(pipeline):
    return pipeline.stages

def get_stage_configuration(stage):
    return dict(stage.configuration.items())

def check_for_parameter(stage_config, param):
    for value in stage_config.values():
        if param in str(value):
            return True
    return False

#pipeline = select_pipeline("sdk_example2")

def map_params_to_stages(pipeline):
    stages = get_pipeline_stages(pipeline)
    params = list(pipeline.parameters.keys())
    
    out_dict = dict()
    
    for stage in stages:
        out_dict[stage.instance_name] = list()
        stage_config = get_stage_configuration(stage)
        for param in params:
            if check_for_parameter(stage_config, param):
                out_dict[stage.instance_name] += [param]
    return out_dict
        
def map_stages_to_params(pipeline):
    stages = get_pipeline_stages(pipeline)
    params = list(pipeline.parameters.keys())
    
    out_dict = dict()
    
    for param in params:
        out_dict[param] = list()
        for stage in stages:
            stage_config = get_stage_configuration(stage)
            if check_for_parameter(stage_config, param):
                out_dict[param] += [stage.instance_name]
    return out_dict     

#returns all pipelines whose names match the pattern
def grep_pipelines(pat):
    pipe_list = list()
    for pipeline in get_pipelines():
        if pat in pipeline.name:
            pipe_list += [pipeline]
    return pipe_list

#return all pipelines and their constituent stages and the parameters used in each stages
def pipe_stages(pipelines):
    stage_list = dict()
    for pipeline in pipelines:
        stage_list[pipeline.name] = map_params_to_stages(pipeline)
    return stage_list

##connections

def get_connections():
    return get_sch().connections

def select_connection(name):
    for connection in get_connections():
        if connection.name == name:
            return connection

#gets all pipeline versions - probably need an arg to select only latest versions    
def get_connection_commits(con):
    con_commits = {con.name: list()}
    for commit in con.pipeline_commits:
        con_commits[con.name] += [{'name':commit.pipeline.name, 'version':commit.version}]
    return con_commits
    
    
#configs
#stage_config = get_stage_configuration(stage).keys()
#{ k:a[k] for k in b.keys()}

#returns configuration name and value from stage where the config parameter name matchest the pattern
def grep_config(stage, pat):
    config_list = list()
    stage_configuration = get_stage_configuration(stage)
    for conf in stage_configuration.keys():
        if pat in conf:
            config_list += [conf]
    return {k:stage_configuration[k] for k in config_list}

#parses configs looking for 'FROM schema.table' statements to discover hot tables        
def find_hot_tables(config_dict):
    hot_tables = list()
    for val in config_dict.values():
        hot_tables += re.findall('from\\s*([\\w]*\\.[\\w]*)', re.sub('\\n', '', val[0]), flags=re.I)
    return hot_tables


#finds all stages of a certain type (based on pattern matching) within a pipeline
def find_stages(stage_type, pipelines):
    match_dict = dict()
    for pipeline in pipelines:
        match_list = [stage for stage in pipeline.stages if stage_type in stage.stage_name]   
        if len(match_list) > 0:
            match_dict[pipeline.pipeline_id] = match_list
    return match_dict

#used for finding all stages of a certain type in all pipelines that have a specific config setting
# e.g. find all http stages within the list of pipelines that are not using OAauth2
# find_config_in_pipelines(pipelines, stage_type = 'Http', config_pat = 'useOAuth2', config_val = False)
def find_config_in_pipelines(pipelines, stage_type, config_pat, config_val):
    stages = find_stages(stage_type, pipelines)
    matches = dict()
    for pipe, stage_list in stages.items():
        #account for multiple stage matches
        for stage in stage_list:
            z = list(grep_config(stage, config_pat).values())[0]
            if z == config_val:
                if matches.get(pipe) is None:
                    matches[pipe] = [stage]
                else:
                    matches[pipe] = matches[pipe] + [stage]
    return matches
    
# config_replacement = {'conf.client.oauth2.credentialsGrantType':'CLIENT_CREDENTIALS',
#                       'conf.client.oauth2.tokenUrl': 'https://www.test.test',
#                       'conf.client.oauth2.additionalValues':[{'header':'value'}],
#                       'conf.client.useOAuth2': True}

def update_stage_config(stage, config_replacement):
    stage.configuration.update(config_replacement)