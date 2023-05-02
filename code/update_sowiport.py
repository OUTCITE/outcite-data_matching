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
_index            = sys.argv[1]; #'geocite' #'outcite_ssoar' #'ssoar_gold'

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

_chunk_size       = _configs['chunk_size_sowiport'];
_request_timeout  = _configs['requestimeout_sowiport'];

_great_score  = _configs['great_score_sowiport'];
_ok_score     = _configs['ok_score_sowiport'];
_max_rel_diff = _configs['max_rel_diff_sowiport'];
_threshold    = _configs['threshold_sowiport'];
_thr_prec     = _configs['thr_prec_sowiport'];

_recheck = _configs['recheck_sowiport'];

#====================================================================================
_index_m    = 'sowiport';
_from_field = 'id';
_to_field   = 'sowiport_ids';

_transformap = [ ('reference',     "source['refstr']"),
                 ('year',          "int(source['date'])"),
                 ('authors',       "[{'author_string': source['coreAuthor'][i]} for i in range(len(source['coreAuthor']))]"),
                 ('publishers',    "[{'publisher_string': source['corePublisher']}]"),
                 ('editors',       "[{'editor_string': source['coreEditor']}]"),
                 ('title',         "source['title']"),
                 ('issue',         "int(source['coreZsnummer'])"),
                 ('volume',        "int(source['coreZsband'])"),
                 ('type',          "source['subtype']"),
                 ('doi',           "source['doi'][0]"),
                 ('source',        "source['coreJournalTitle']") ];

_query_fields = ['title','coreAuthor','coreJournalTitle','institutions','corePublisher'];
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
