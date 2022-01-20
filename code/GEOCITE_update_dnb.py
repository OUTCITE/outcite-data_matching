#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from difflib import SequenceMatcher as SM
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index            = 'geocite';#sys.argv[1];
_chunk_size       = 200;
_scroll_size      = 100;
_max_extract_time = 0.1; #minutes
_max_scroll_tries = 2;

_great_score  = [100,50]; # [full reference, title]
_ok_score     = [36,18];
_max_rel_diff = [0.5,0.5];

_recheck = True;

_ids = None;#['GaS_2000_0001'];#["gesis-ssoar-29359","gesis-ssoar-55603","gesis-ssoar-37157","gesis-ssoar-5917","gesis-ssoar-21970"];#None

_body = { '_op_type': 'update',
          '_index': _index,
          '_id': None,
          '_source': { 'doc':  {
                                'has_dnb_ids': True,
                                'dnb_ids':     None,
                                'refobjects':        None
                                }
                     }
        }

_refobjs = [    'anystyle_references_from_cermine_fulltext',
                'anystyle_references_from_cermine_refstrings',
                'anystyle_references_from_grobid_fulltext',
                'anystyle_references_from_grobid_refstrings',
                'anystyle_references_from_gold_fulltext',
                'anystyle_references_from_gold_refstrings',
                'cermine_references_from_cermine_xml',
                'cermine_references_from_grobid_refstrings',
                'cermine_references_from_gold_refstrings',
                'grobid_references_from_grobid_xml' ];

_search_body_title     = { 'query':     { 'match_phrase': { 'title': None } }, '_source':['id','title','publishers','editors','authors','ids'] };
_search_body_refstring = { 'query':     { 'multi_match':  { 'query': None, 'fields': ['title','authors','titles','publishers','editors'] } }, '_source':['id','title','publishers','editors','authors','ids'] };

#_scr_body = { "query": { 'match_all': {} } };
_scr_body = { "query": { "ids": { "values": _ids } } } if _ids else {'query':{'bool':{'must':{'term':{'has_dnb_ids': False}}}}} if not _recheck else {'query':{'match_all':{}}};
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def distance(a,b):
    s          = SM(None,a,b);
    overlap    = sum([block.size for block in s.get_matching_blocks()]);
    return 1-(overlap / max([len(a),len(b)]));

def get_best_match(query,TITLE,results): #TODO: We probably need to do more sophisticated matching checks based on authors, etc.
    if len(results) > 0:
        print('-------------------------------------------\n'+query+'\n-------------------------------------------\n',results[0][0]['id'],'\n',results[0][0]['title'],'\n',results[0][1],'\n-------------------------------------------');
    else:
        return None;
    title = results[0][0]['title'][0] if isinstance(results[0][0]['title'],list) else results[0][0]['title'];
    if results[0][1] > _great_score[TITLE]:
        print('PASSED: score',results[0][1],'>',_great_score[TITLE]);
        return results[0][0]['id'] if 'id' in results[0][0] else None;
    if results[0][1] > _ok_score[TITLE] and distance(query,title) < _max_rel_diff[TITLE]:
        print('PASSED: score',results[0][1],'>',_ok_score[TITLE],'and distance',distance(query,title),'<',_max_rel_diff[TITLE]);
        return results[0][0]['id'] if 'id' in results[0][0] else None;
    print('FAILED: score',results[0][1],'<=',_ok_score[TITLE],'and/or distance',distance(query,title),'>=',_max_rel_diff[TITLE]);
    return None;

def find(refobjects,client):
    ids = [];
    for i in range(len(refobjects)):
        doi  = None;
        body = None;
        if 'title' in refobjects[i]:
            body                                   = copy(_search_body_title);
            body['query']['match_phrase']['title'] = refobjects[i]['title'];
        elif 'reference' in refobjects[i]:
            body                                  = copy(_search_body_refstring);
            body['query']['multi_match']['query'] = refobjects[i]['reference'];
        else:
            continue;
        results = client.search(index='dnb',body=body,size=10)['hits']['hits'];
        results = [(result['_source'],result['_score'],) for result in results];
        doi     = get_best_match(refobjects[i]['title'] if 'title' in refobjects[i] else refobjects[i]['reference'],'title' in refobjects[i],results);
        if doi != None:
            refobjects[i]['dnb_id'] = doi;
            ids.append(doi);
    return set(ids), refobjects;

def search_dnb():
    client   = ES(['localhost'],scheme='http',port=9200,timeout=60);
    client_m = ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=_index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,body=_scr_body);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            print('---------------------------------------------------------------------------------------------\n',doc['_id'],'---------------------------------------------------------------------------------------------\n');
            body        = copy(_body);
            body['_id'] = doc['_id'];
            ids        = set(doc['_source']['dnb_ids']) if doc['_source']['dnb_ids'] != None else set([]);
            for refobj in _refobjs:
                previous_refobjects                         = doc['_source'][refobj] if refobj in doc['_source'] else None;
                new_ids, new_refobjects                     = find(previous_refobjects,client_m) if isinstance(previous_refobjects,list) else (set([]),previous_refobjects);
                ids                                        |= new_ids;
                body['_source']['doc'][refobj]              = new_refobjects; # The updated ones
                print('-->',refobj,'gave',['','no '][len(new_ids)==0]+'ids',', '.join(new_ids),'\n');
            print('------------------------------------------------\n-- overall ids --------------------------------\n'+', '.join(ids)+'\n------------------------------------------------');
            body['_source']['doc']['dnb_ids']     = list(ids) if len(ids) > 0 else None;
            body['_source']['doc']['has_dnb_ids'] = True       if len(ids) > 0 else False;
            yield body;
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time*_scroll_size))+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e);
                print('\n[!]-----> Some problem occured while scrolling. Sleeping for 3s and retrying...\n');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client = ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,search_dnb(),chunk_size=_chunk_size, request_timeout=60):
    i += 1;
    if not success:
        print('\n[!]-----> A document failed:', info['index']['_id'], info['index']['error'],'\n');
    print(i,info)
    if i % _chunk_size == 0:
        print(i,'refreshing...');
        _client.indices.refresh(index=_index);
print(i,'refreshing...');
_client.indices.refresh(index=_index);
#'''
#-------------------------------------------------------------------------------------------------------------------------------------------------
