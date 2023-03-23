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

_chunk_size       = _configs['chunk_size_arxiv'];
_request_timeout  = _configs['requestimeout_arxiv'];

_great_score  = _configs['great_score_arxiv'];
_ok_score     = _configs['ok_score_arxiv'];
_max_rel_diff = _configs['max_rel_diff_arxiv'];
_threshold    = _configs['threshold_arxiv'];
_thr_prec     = _configs['thr_prec_arxiv'];

_recheck = _configs['recheck_arxiv'];

#====================================================================================
_index_m    = 'arxiv';
_from_field = 'id';
_to_field   = 'arxiv_ids';

_transformap = { 'title':                           ['title',                       True,  None], #name in matchobj, path in refobj, name in refobj, pick first from list, default value
                 #'update_date':                     ['year',                        False, None],
                 'authors_parsed.author_string':    ['authors.author_string',       False, []  ],
                 #'authors_parsed.surnames':         ['authors.surname',             False, []  ],
                 #'authors_parsed.initials':         ['authors.initials',            False, []  ],
                 #'authors_parsed.firstnames':       ['authors.firstnames',          False, []  ],
                 'doi':                             ['doi',                         True, None],};

_query_fields = ['title','authors_parsed.author_string','doi'];
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