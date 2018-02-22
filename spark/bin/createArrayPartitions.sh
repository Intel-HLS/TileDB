#!/bin/bash

/home/sparkuser/VariantDB/repos/store/python_api/db/query_db.py -d
sqlite:////home/sparkuser/VariantDB/meta_data/samples_and_fields.sqlite -j '{
"query":"ArrayIdx2SampleIdx","input":{"array_idx":2},"output":"/tmp/t.csv"}'
