CREATE TABLE IF NOT EXISTS PIPELINES (
pipelineId text primary key,
name text,
description text,
pipelineLabels text,
version text,
executorType text
);

CREATE TABLE IF NOT EXISTS pipeline_stages (
pipelineId text,
stage_name text,
stage_type text,
library text,
instance_name text,
description text,
FOREIGN KEY (pipelineId) REFERENCES pipelines(pipelineId)
);