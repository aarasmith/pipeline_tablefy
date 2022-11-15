#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 15 10:53:59 2022

@author: andara
"""

def get_connection():
    return psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port)

def get_schema():
    with open('schema.sql') as f:
        return f.read()
    
def create_tables():
    with psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port
            ) as conn:
                cur = conn.cursor()
                cur.execute(get_schema())


def insert_pipelines(my_values):
    #my_values = list(zip(my_values))
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.executemany(
            'INSERT INTO PIPELINES VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (pipelineId) DO NOTHING;',
            my_values
            )
        conn.commit()

def insert_stages(my_values):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.executemany(
            'INSERT INTO pipeline_stages VALUES (%s,%s,%s,%s,%s,%s)',
            my_values
            )
        conn.commit()