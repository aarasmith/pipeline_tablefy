#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 15 09:42:07 2022

@author: andara
"""

import os
from dotenv import load_dotenv
import db

load_dotenv()

def get_cred_id():
    return os.environ['CREDENTIAL_ID']

def get_token_id():
    return os.environ['TOKEN_ID']

from streamsets.sdk import ControlHub
############### pipeline attributes

def get_pipelines(sch, pipeline_data):
    atr_list = ['pipelineId', 'name', 'description', 'pipelineLabels', 'version', 'executorType']
    
    values_to_insert = []
    
    for pipeline in pipeline_data:
        
        pipe_dict = pipeline.__dict__.get('_data')
        
        value_list = []
        for key in atr_list:
            value = pipe_dict.get(key)
            value_list.append(value)
        
        value_list[3] = [label.get('label') for label in value_list[3]]
        
        values_to_insert.append(tuple(value_list))
        
    return values_to_insert

############## pipeline stages

def get_stages(sch, pipeline_data):    
    
    values_to_insert = []
    
    for pipeline in pipeline_data:
    
        pipeline_id = pipeline.pipeline_id
        for stage in pipeline.stages:
            
            try:
                stage_description = stage.description
            except:
                stage_description = ''
            
            stage_type = stage.__dict__.get('stage_type')
            
            stage_dict = stage.__dict__.get('_data')
            
            atr_list = ['stageName', 'library', 'instanceName']
            value_list = []
            for key in atr_list:
                value = stage_dict.get(key)
                value_list.append(value)
                
            value_list_out = [pipeline_id, value_list[0], stage_type, value_list[1], value_list[2], stage_description]
            
            values_to_insert.append(tuple(value_list_out))
    
    return values_to_insert

def get_pipeline_info():
   sch = ControlHub(credential_id=get_cred_id(), token=get_token_id())
   pipeline_data = sch.pipelines.get_all()
   
   db.insert_pipelines(get_pipelines(sch, pipeline_data))
   db.insert_stages(get_stages(sch, pipeline_data))


