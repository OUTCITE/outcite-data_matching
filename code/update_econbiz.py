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

_chunk_size       = _configs['chunk_size_econbiz'];
_request_timeout  = _configs['requestimeout_econbiz'];

_great_score  = _configs['great_score_econbiz'];
_ok_score     = _configs['ok_score_econbiz'];
_max_rel_diff = _configs['max_rel_diff_econbiz'];
_threshold    = _configs['threshold_econbiz'];
_thr_prec     = _configs['thr_prec_econbiz'];

_recheck = _configs['recheck_econbiz'];

#====================================================================================
_index_m    = 'econbiz';
_from_field = 'id';
_to_field   = 'econbiz_ids';

_transformap = { 'title':                       ['title',                       True,  None], #name in matchobj, name in refobj, pick first from list, default value
                 'year':                        ['year',                        True,  None],
                 'publishers.name':             ['publishers.publisher_string', False, []  ],
                 'authors.name':                ['authors.author_string',       False, []  ],
                 'doi':                         ['doi',                         False, None],};

_query_fields = ['title','authors','publishers','doi'];
#====================================================================================
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client = ES(['localhost'],scheme='http',port=9200,timeout=60);

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
