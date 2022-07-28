#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from common import *
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index            = sys.argv[1]; #'geocite' #'ssoar'
_chunk_size       = 200;
_requestimeout    =  60;

_great_score  = [100,50]; # [full reference, title]
_ok_score     = [36,18];
_max_rel_diff = [0.5,0.5];
_threshold    = 0.6; #relative difference to largest string, 0 if identical, 1 if no overlap at all
_thr_prec     = 0.6;

_recheck = False;

#====================================================================================
_index_m    = 'crossref';
_from_field = 'DOI';
_to_field   = 'crossref_ids';

_transformap = { 'title':                       ['title',                       True,  None], #name in matchobj, name in refobj, pick first from list, default value
                 'published-print.date-parts':  ['year',                        True,  None],
                 'publisher':                   ['publishers.publisher_string', False, []  ],
                 'author.given':                ['authors.firstnames',          False, []  ],
                 'author.family':               ['authors.surname',             True,  []  ] };

_query_fields = ['title','author','publisher'];
#====================================================================================
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client = ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,search(_to_field,_from_field,_query_fields,_index,_index_m,_great_score,_ok_score,_thr_prec,_max_rel_diff,_threshold,_transformap,_recheck),chunk_size=_chunk_size, request_timeout=_requestimeout):
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
