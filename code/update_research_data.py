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

# THE INDEX TO UPDATE THE REFERENCES IN
_index = sys.argv[1];

# LOADING THE CONFIGS CUSTOM IF AVAILABLE OTHERWISE THE DEFAULT CONFIGS FILE
IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

# PARAMETERS FOR THE BULK UPDATING ELASTICSEARCH PROCESS
_chunk_size       = _configs['chunk_size_research_data'];
_request_timeout  = _configs['requestimeout_research_data'];

# IF MATCHING SCORE IS ABOVE THIS THEN DIRECTLY A MATCH OF THE METAOBJ AND THE REFOBJ
_great_score  = _configs['great_score_research_data'];
# IF MATCHING SCORE IS ABOVE THIS THEN MAY BE A MATCH CONDITIONED ON FURTHER CHECK
_ok_score     = _configs['ok_score_research_data'];
# VARIOUS THRESHOLDS USED TO MATCH THE METAOBJ AND THE REFOBJ
_max_rel_diff = _configs['max_rel_diff_research_data'];
_threshold    = _configs['threshold_research_data'];
_thr_prec     = _configs['thr_prec_research_data'];

# WETHER TO REDO THE MATCHING FOR DOCUMENTS THAT HAVE ALREADY BEEN LABELLED AS PROCESSED FOR THIS STEP BEFORE
_recheck = _configs['recheck_research_data'];

#====================================================================================
# NAME OF THE ELASTICSEARCH INDEX TO MATCH AGAINST
_index_m    = 'research_data';
# FIELD NAME IN THE TARGET INDEX WHICH STORES THE ID TO KEEP AS REALIZATION OF THE MATCHING
_from_field = 'id';
# WHERE TO ADD THE ID FROM THE ABOVE TARGET INDEX FIELD
_to_field   = 'research_data_ids';

# TO SEE IF THE REFOBJ AND THE TARGET METAOBJ MATCH EVALUATE THE RHS IN THE METAOBJ AND COMPARE TO THE LHS FIELD IN THE REFOBJ
_transformap = [ ('reference',     "source['refstr']"),
                 ('year',          "int(source['publication_year'])"),
                 ('authors',       "[{'author_string': source['person'][i]} for i in range(len(source['person']))]"),
                 ('publishers',    "[{'publisher_string': source['publisher']}]"),
                 ('za_number',     "source['study_number']"),
                 ('title',         "source['title']"),
                 ('doi',           "source['doi'][0] if isinstance(source['doi'],list) else None"),
                 ('type',          "source['type']") ];

# WHICH FIELDS TO CONSIDER FOR MULTI-MATCH QUERY (NOT CURRENTLY USED)
_query_fields = _configs['research_data_query_fields'];#['title','person','source','publisher','doi'];
#====================================================================================
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

# CONNECTION TO THE LOCAL ELASTICSEARCH INSTANCE WHERE THE INDEX IS
_client = ES(['http://localhost:9200'],timeout=60);

# BATCH UPDATING THE LOCAL DOCUMENTS INDEX WITH THE MATCHES
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
