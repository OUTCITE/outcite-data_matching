#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from common import *
from pathlib import Path
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index = sys.argv[1]; #'geocite' #'ssoar'

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

_chunk_size       = _configs['chunk_size_research_data'];
_request_timeout  = _configs['requestimeout_research_data'];

_great_score  = _configs['great_score_research_data'];
_ok_score     = _configs['ok_score_research_data'];
_max_rel_diff = _configs['max_rel_diff_research_data'];
_threshold    = _configs['threshold_research_data'];
_thr_prec     = _configs['thr_prec_research_data'];

_recheck = _configs['recheck_research_data'];

#====================================================================================
_index_m    = 'research_data';
_from_field = 'id';
_to_field   = 'research_data_ids';

_transformap = [ ('reference',     "source['refstr']"),
                 ('year',          "int(source['publication_year'])"),
                 ('authors',       "[{'author_string': source['person'][i]} for i in range(len(source['person']))]"),
                 ('publishers',    "[{'publisher_string': source['publisher']}]"),
                 ('za_number',     "source['study_number']"),
                 ('title',         "source['title']"),
                 ('doi',           "source['doi'][0] if isinstance(source['doi'],list) else None"),
                 ('type',          "source['type']") ];

_query_fields = _configs['research_data_query_fields'];#['title','person','source','publisher','doi'];
#====================================================================================
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client = ES(['http://localhost:9200'],timeout=60);#_client = ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,search(_to_field,_from_field,_query_fields,_index,_index_m,_great_score,_ok_score,_thr_prec,_max_rel_diff,_threshold,_transformap,_recheck),chunk_size=_chunk_size, request_timeout=_request_timeout):
    i += 1;
    if not success:
        print('\n[!]-----> A document failed:', info['index']['_id'], info['index']['error'],'\n');
    print(i,info)
    if i % _chunk_size == 0:
        print(i,'refreshing...');
        _client.indices.refresh(index=_index);
print(i,'refreshing...');
_client.indices.refresh(index=_index);
#-------------------------------------------------------------------------------------------------------------------------------------------------
