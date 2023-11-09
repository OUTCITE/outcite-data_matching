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
_index            = sys.argv[1]; #'geocite' #'ssoar'

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

_chunk_size       = _configs['chunk_size_crossref'];
_request_timeout  = _configs['requestimeout_crossref'];

_great_score  = _configs['great_score_crossref'];
_ok_score     = _configs['ok_score_crossref'];
_max_rel_diff = _configs['max_rel_diff_crossref'];
_threshold    = _configs['threshold_crossref'];
_thr_prec     = _configs['thr_prec_crossref'];

_recheck = _configs['recheck_crossref'];

#====================================================================================
_index_m    = 'crossref';
_from_field = 'DOI';
_to_field   = 'crossref_ids';


_transformap =  [ ('reference',    "source['refstr']"),
                  ('year',         "int(source['published-print']['date-parts'][0][0])"),
                  ('authors',      "[{'author_string':source['author'][i]['given']+' '+source['author'][i]['family'],'firstnames': [part for part in source['author'][i]['given'].split() if len(part)>1 and not part.endswith('.')],'initials':[part[0] for part in source['author'][i]['given'].split()],'surname': source['author'][i]['family']} for i in range(len(source['author']))]"),
                  ('publishers',   "[{'publisher_string': source['publisher']}]"),
                  ('title',        "source['title'][0]"),
                  ('issue',        "int(source['issue'])"),
                  ('volume',       "int(source['volume'])"),
                  ('doi',          "source['DOI']"),
                  ('type',         "source['type']") ];

_query_fields = ['title','author','publisher','doi'];
#====================================================================================
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);

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
