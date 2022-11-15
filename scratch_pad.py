#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 15 09:42:07 2022

@author: andara
"""

import psycopg2
import os

def get_cred_id():
    return credential_id

def get_token_id():
    return token_id

from streamsets.sdk import ControlHub
############### pipeline attributes
sch = ControlHub(credential_id=get_cred_id(), token=get_token_id())

pipeline_data = sch.pipelines.get_all()

#stage2 = pipeline_data.get_all()

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

pipeline_data = sch.pipelines
stage2 = pipeline_data.get_all()

def get_stages(sch, pipeline_data):    
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
   
   insert_pipelines(get_pipelines(sch, pipeline_data))
   insert_stages(get_stages(sch, pipeline_data))

#value_dict = dict(zip(atr_list, value_list))

master_dict_keys = list(range(0, len(sch.pipelines)))

master_dict = dict(zip(master_dict_keys, dict_list))


with psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        ) as conn:
            cur = conn.cursor()
            cur.execute('select * from PIPELINES;')

cursor = db_connection.cursor()
    cursor.executemany(
            'INSERT INTO posts VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING',
            posts
            )
    db_connection.commit()


{k: v for d in test_entry[3] for k, v in d.items()}.get('label')
{k: v for d in test for k, v in d.items()}

[label.get('label') for label in test]
