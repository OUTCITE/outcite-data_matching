import numpy as np
from scipy.optimize import linear_sum_assignment as LSA
from difflib import SequenceMatcher as SM
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
import re
import time
import sys
import json
import sqlite3
from pathlib import Path

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

_logfile  = 'matching.log';
_bufferdb = 'queries.db';

_max_extract_time = _configs["max_extract_time"];
_max_scroll_tries = _configs["max_scroll_tries"];
_scroll_size      = _configs["scroll_size"];
_max_val_len      = _configs["max_val_len"];
#'''
_refobjs = _configs["refobjs"];

_ids     = _configs["ids"];#['GaS_2000_0001'];#["gesis-ssoar-29359","gesis-ssoar-55603","gesis-ssoar-37157","gesis-ssoar-5917","gesis-ssoar-21970"];#None
#'''
#_refobjs = [ 'anystyle_references_from_gold_refstrings' ];

YEAR = re.compile(r'1[5-9][0-9]{2}|20(0[0-9]|1[0-9]|2[0-3])'); #1500--2023

def log(strings,OUT):
    OUT.write(' '.join([str(string) for string in strings])+'\n');

def lookup(query,table,cur):
    rows = cur.execute("SELECT result FROM "+table+" WHERE query=?",(json.dumps(query),)).fetchall();
    if rows:
        return json.loads(rows[0][0]);
    return None;

def store(query,result,table,cur):
    cur.execute("INSERT OR REPLACE INTO "+table+" VALUES(?,?)",(json.dumps(query),json.dumps(result),));

def walk_down(pointer,match_keys):
    if len(match_keys)==0:
        yield pointer;
    elif isinstance(pointer,dict):
        if match_keys[0] in pointer:
            for el in walk_down(pointer[match_keys[0]],match_keys[1:]):
                yield el;
    elif isinstance(pointer,list):
        for el_p in pointer:
            for el in walk_down(el_p,match_keys):
                yield el;

def extract(L):
    L_ = L[0] if len(L)>0 else None;
    if isinstance(L_,list):
        return extract(L_);
    return L_;

def merge(d, u):
    for k, v in u.items():
        if v == None:                                   # discard None values
            continue;
        elif (not k in d) or d[k] == None:              # new key or old value was None
            d[k] = v;
        elif isinstance(v,dict) and v != {}:            # non-Counter dicts are merged
            d[k] = merge(d.get(k,{}),v);
        elif isinstance(v,set):                         # set are joined
            d[k] = d[k] | v;
        elif isinstance(v,list):                        # list are concatenated
            d[k] = d[k] + v;
        elif isinstance(v,int) or isinstance(v,float):  # int and float are added
            d[k] = d[k] + v;
        elif v != dict():                               # anything else is replaced
            d[k] = v;
    return d;

def remove_empty(data):
    new_data = {}
    for k, v in data.items():
        if isinstance(v, dict):
            v = remove_empty(v)
        if v:#not v in (u'', None, {}, []):
            new_data[k] = v
    return new_data


def transform(result,transformap):
    matchobj = dict();
    for match_keystr in transformap:
        #--------------------------------------------------------------------------------------------------------------------------------------------------------
        match_keys     = match_keystr.split('.');                                                               # The path in the matchobj
        match_pointers = list(walk_down(result,match_keys));                                                          # The information stored at the end of the path
        if len(match_pointers)==0:                                                          # If the path does not exist
            continue;
        ref_keystr,get_1st,default = transformap[match_keystr];                                                    # The information from the transformation mapping
        match_values               = [extract(match_pointers)] if get_1st and len(match_pointers)>=1 else match_pointers;  # The extracted information #TODO: this did not work
        ref_keys                   = ref_keystr.split('.');                                                        # The path in the refobj
        #--------------------------------------------------------------------------------------------------------------------------------------------------------
        if len(ref_keys) == 1:
            matchobj_ = {ref_keys[0]: match_values};
        else:
            if default == []:
                matchobj_ = {ref_keys[0]: []};
                values    = match_values if isinstance(match_values,list) else [match_values];
                for value in values:
                    matchobj_[ref_keys[0]].append(dict());
                    ref_pointer = matchobj_[ref_keys[0]][-1];
                    for i in range(1,len(ref_keys)):
                        ref_pointer[ref_keys[i]] = dict();
                        if i+1 < len(ref_keys):
                            ref_pointer = ref_pointer[ref_keys[i]];
                        elif value and len(value) > 0:
                            ref_pointer[ref_keys[i]] = value;
            else:
                matchobj_[ref_keys[0]] = dict();
                ref_pointer           = matchobj_[ref_keys[0]];
                for i in range(1,len(ref_keys)):
                    ref_pointer[ref_keys[i]] = dict();
                    if i+1 < len(ref_keys):
                        ref_pointer = ref_pointer[ref_keys[i]];
                    elif match_values and len(match_values) > 0:
                        ref_pointer[ref_keys[i]] = match_values;
        matchobj = merge(matchobj,matchobj_);
        #--------------------------------------------------------------------------------------------------------------------------------------------------------
        #keys = list(matchobj.keys());
        #for key in keys:
        #    if not matchobj[key] or len(matchobj[key]) == 0:
        #        del matchobj[key];
    matchobj = remove_empty(matchobj);
    return matchobj;

def distance(a,b):
    a,b        = a.lower(), b.lower();
    s          = SM(None,a,b);
    overlap    = sum([block.size for block in s.get_matching_blocks()]);
    return 1-(overlap / max([len(a),len(b)]));

def distance_2(a,b):
    a,b      = a.lower(), b.lower();
    s        = SM(None,a,b);
    overlap  = sum([block.size for block in s.get_matching_blocks()]);
    dist     = max([len(a),len(b)]) - overlap;
    return dist;

def flatten(d, parent_key='', sep='_'):
    items = [];
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k;
        if isinstance(v, dict):
            items.extend(flatten(v, new_key, sep=sep).items());
        else:
            items.append((new_key, v));
    return dict(items);

def pairfy(d, parent_key='', sep='_'): # To be applied after flatten!
    for key in d:
        if isinstance(d[key],list):
            for el in d[key]:
                if isinstance(el,dict):
                    for a,b in pairfy(el,key,sep):
                        yield (a,str(b),);
                else:
                    yield (parent_key+sep+key,str(el),);
        else:
            yield (parent_key+sep+key,str(d[key]),);

def dictfy(pairs):
    d = dict();
    for attr,val in pairs:
        if not attr in d:
            d[attr] = [];
        d[attr].append(val);
    return d;

def assign(A,B): # Two lists of strings
    #print(A); print(B); print('---------------------------------------------------------');
    M          = np.array([[distance_2(a,b) if isinstance(a,str) and isinstance(b,str) else a!=b for b in B] for a in A]);
    rows, cols = LSA(M);
    mapping    = [pair for pair in zip(rows,cols)];
    costs      = [M[assignment] for assignment in mapping];
    return mapping,costs;

def similar_enough(a,b,cost,threshold):
    if isinstance(a,str) and isinstance(b,str):
        if YEAR.fullmatch(a) and YEAR.fullmatch(b):
            y1, y2 = int(a), int(b);
            return abs(y1-y2) <= 1; # A one year difference between years is accepted
        return cost / max([len(a),len(b)]) < threshold;
    return a == b;

def compare_refstrings(P_strings,T_strings,threshold): # Two lists of strings
    mapping,costs = assign(P_strings,T_strings);
    pairs         = [(P_strings[i],T_strings[j],) for i,j in mapping];
    matches       = [(P_strings[mapping[i][0]],T_strings[mapping[i][1]],) for i in range(len(mapping)) if     similar_enough(P_strings[mapping[i][0]],T_strings[mapping[i][1]],costs[i],threshold)];
    mismatches    = [(P_strings[mapping[i][0]],T_strings[mapping[i][1]],) for i in range(len(mapping)) if not similar_enough(P_strings[mapping[i][0]],T_strings[mapping[i][1]],costs[i],threshold)];
    precision     = len(matches) / len(P_strings);
    recall        = len(matches) / len(T_strings);
    return precision, recall, len(matches), len(P_strings), len(T_strings), matches, mismatches, mapping, costs;

def compare_refobject(P_dict,T_dict,threshold):                       # Two dicts that have already been matched based on refstring attribute
    P_pairs     = pairfy(flatten(P_dict));                            # All attribute-value pairs from the output dict
    T_pairs     = pairfy(flatten(T_dict));                            # All attribute-value pairs from the gold   dict
    P_pair_dict = dictfy(P_pairs);                                    # Output values grouped by attributes in a dict
    T_pair_dict = dictfy(T_pairs);                                    # Gold   values grouped by attributes in a dict
    P_keys      = set(P_pair_dict.keys());                            # Output attributes
    T_keys      = set(T_pair_dict.keys());                            # Gold attributes
    TP_keys     = P_keys & T_keys;                                    # Attributes present in output and gold
    P           = sum([len(P_pair_dict[P_key]) for P_key in P_keys]); # Number of attribute-value pairs in output
    T           = sum([len(T_pair_dict[T_key]) for T_key in T_keys]); # Number of attribute-value pairs in gold object
    TP          = 0;                                                  # Number of attribute-value pairs in output and gold
    matches     = [];
    mismatches  = [];
    mapping     = [];
    costs       = [];
    for TP_key in TP_keys:
        prec, rec, TP_, P_, T_, matches_, mismatches_, mapping_, costs_ = compare_refstrings(P_pair_dict[TP_key],T_pair_dict[TP_key],threshold);
        TP                                                             += TP_;
        matches                                                        += [(TP_key,str(match_0),str(match_1),) for match_0,      match_1      in matches_    ];
        mismatches                                                     += [(TP_key,str(match_0),str(match_1),) for match_0,      match_1      in mismatches_ ];
        mapping                                                        += [(TP_key,assignment_0,assignment_1,) for assignment_0, assignment_1 in mapping_    ];
        costs                                                          += [(TP_key,cost_,)                     for cost_                      in costs_      ];
    return TP/P, TP/T, TP, P, T, matches, mismatches, mapping, costs;

def get_best_match(refobj,results,query_field,great_score,ok_score,thr_prec,max_rel_diff,threshold,transformap,id_field,OUT):
    #TITLE = True if 'title' in refobj and refobj['title'] else False;
    query_val = refobj[query_field];
    if len(results) > 0:
            log(['____________________________________________________________________________________________________________________'],OUT);#,results[0][0]['id'],'\n',results[0][0]['title'],'\n',results[0][1],'\n-------------------------------------------');
    else:
        return (None,None);
    matchobj = transform(results[0][1],transformap);
    log(['=QUERY===============================================================================\n'+query_field.upper()+': '+query_val+'\n====================================================================================='],OUT);
    log(['=MATCH===============================================================================\n'+'\n'.join([key+':    '+str(matchobj[key]) for key in matchobj])],OUT);
    #print('=REFER===============================================================================\n'+'\n'.join([key+':    '+str(refobj[key])   for key in refobj  ])+'\n================================================================================');
    prec, rec, tp, p, t, matches, mismatches, mapping, costs = compare_refobject(matchobj,refobj,threshold);
    matchprec                                                = len(matches)/(len(matches)+len(mismatches)) if len(matches)+len(mismatches) > 0 else 0;
    if len(matches)+len(mismatches) > 0:
        log(['Matchprec:',matchprec,'Precision:',prec,'Recall:',rec,'\n___________________________________'],OUT);
        log(['Matches:   ',matches],OUT);
        log(['Mismatches:',mismatches,'\n___________________________________'],OUT);
    title = results[0][1]['title'][0] if isinstance(results[0][1]['title'],list) and len(results[0][1]['title'])>0 else '' if isinstance(results[0][1]['title'],list) else results[0][1]['title'];
    if results[0][0] > great_score[query_field=='title']:
        log(['PASSED '+query_field.upper()+'-query: score',results[0][0],'>',great_score[query_field=='title']],OUT);
        if matchprec >= thr_prec and id_field in results[0][1]:
            log(['DID MATCH:',matchprec,'>=',thr_prec],OUT);
            return (results[0][1][id_field],matchobj,);
        log(['DID NOT MATCH.'],OUT);
    elif results[0][0] > ok_score[query_field=='title'] and distance(query_val,title) < max_rel_diff[query_field=='title']:
        log(['PASSED '+query_field.upper()+'-query: score',results[0][0],'>',ok_score[query_field=='title'],'and distance',distance(query_val,title),'<',max_rel_diff[query_field=='title']],OUT);
        if matchprec >= thr_prec and id_field in results[0][1]:
            log(['DID MATCH:',matchprec,'>=',thr_prec],OUT);
            return (results[0][1][id_field],matchobj,);
        log(['DID NOT MATCH.'],OUT);
        #matchID = results[0][1][id_field] if matchprec >= thr_prec and id_field in results[0][1] else None;
        #return (matchID,matchobj) if matchID else (None,None);
    if (not query_val) or not title:
        log(['FAILED:',query_val,title],OUT);
    else:
        log(['FAILED: score',results[0][0],'<=',ok_score[query_field=='title'],'and/or distance',distance(query_val,title),'>=',max_rel_diff[query_field=='title'],'and/or did not match'],OUT);
    return (None,None);

def make_refs(matched_refs,index_m):
    refobjects = [];
    for match_id in matched_refs:
        new_ref = {index_m+'_id':match_id};
        for field in matched_refs[match_id]:
            if field not in ['authors','publishers','editors'] and isinstance(matched_refs[match_id][field],list) and len(matched_refs[match_id][field])>0:
                new_ref[field] = matched_refs[match_id][field][0];
            elif field in ['authors','publishers','editors']:
                new_objs = [];
                if not isinstance(matched_refs[match_id][field],list):
                    matched_refs[match_id][field] = [matched_refs[match_id][field]];
                for obj in matched_refs[match_id][field]:
                    new_obj = dict();
                    for obj_field in obj:
                        if isinstance(obj[obj_field],list) and len(obj[obj_field])>0:
                            new_obj[obj_field] = obj[obj_field][0];
                        elif obj[obj_field]:
                            new_obj[obj_field] = obj[obj_field];
                    new_objs.append(new_obj);
                if new_objs:
                    new_ref[field] = new_objs;
            elif matched_refs[match_id][field]:
                new_ref[field] = matched_refs[match_id][field];
        refobjects.append(new_ref);
    return refobjects;

def find(refobjects,client,index,field,query_doi,query_title,query_refstring,fields,great_score,ok_score,thr_prec,max_rel_diff,threshold,transformap,id_field,OUT,cur):
    ids          = [];
    matchobjects = [];
    for i in range(len(refobjects)):
        results_doi    = [];
        results_title  = [];
        results_refstr = [];
        if query_doi and 'doi' in refobjects[i] and refobjects[i]['doi']:
            query                 = copy(query_doi);
            query['match']['doi'] = refobjects[i]['doi'][:_max_val_len];
            results_doi          = lookup(query,index,cur);
            if not results_doi:
                results_doi = client.search(index=index,query=query,_source=fields)['hits']['hits'];
                store(query,results_doi,index,cur);
        if 'title' in refobjects[i] and refobjects[i]['title']:
            query                          = copy(query_title);
            query['match_phrase']['title'] = refobjects[i]['title'][:_max_val_len];
            results_title                  = lookup(query,index,cur);
            if not results_title:
                results_title = client.search(index=index,query=query,_source=fields)['hits']['hits'];
                store(query,results_title,index,cur);
        if 'reference' in refobjects[i] and refobjects[i]['reference']:
            query                         = copy(query_refstring);
            query['multi_match']['query'] = refobjects[i]['reference'][:_max_val_len];
            results_refstr                = lookup(query,index,cur);
            if not results_refstr:
                results_refstr = client.search(index=index,query=query,_source=fields)['hits']['hits'];
                store(query,results_refstr,index,cur);
        if (not (query_doi and 'doi' in refobjects[i] and refobjects[i]['doi'])) and (not ('title' in refobjects[i] and refobjects[i]['title'])) and (not ('reference' in refobjects[i] and refobjects[i]['reference'])):
            print('Neither title nor refstr in refobject!');
            continue;
        #results       = sorted([(result['_score'],result['_source'],) for result in results],key=lambda x: x[0],reverse=True); #print(query,'\n',results)
        best_results_doi    = [(results_doi[   0]['_score'],results_doi[   0]['_source'],)] if results_doi  else [];
        best_results_title  = [(results_title[ 0]['_score'],results_title[ 0]['_source'],)] if results_title  else [];
        best_results_refstr = [(results_refstr[0]['_score'],results_refstr[0]['_source'],)] if results_refstr else [];
        #results            = sorted(best_results_title+best_results_refstr,key=lambda x: x[0],reverse=True);
        #TITLE              = best_results_title[0][0]>best_results_refstr[0][0] if results_title and results_refstr else False if results_refstr else True if results_title else False;
        #ID, match_obj      = get_best_match(refobjects[i],results,TITLE,great_score,ok_score,thr_prec,max_rel_diff,threshold,transformap,id_field);
        ID, match_obj      = get_best_match(refobjects[i],best_results_doi   ,'doi'      ,great_score,ok_score,thr_prec,max_rel_diff,threshold,transformap,id_field,OUT) if best_results_doi               else [None,None];
        ID, match_obj      = get_best_match(refobjects[i],best_results_title ,'title'    ,great_score,ok_score,thr_prec,max_rel_diff,threshold,transformap,id_field,OUT) if best_results_title  and not ID else [ID,match_obj];
        ID, match_obj      = get_best_match(refobjects[i],best_results_refstr,'reference',great_score,ok_score,thr_prec,max_rel_diff,threshold,transformap,id_field,OUT) if best_results_refstr and not ID else [ID,match_obj];
        if ID != None:
            refobjects[i][field[:-1]] = ID;
            ids.append(ID);
            matchobjects.append(match_obj);
            #print(refobjects[i]);
        else:
            if field[:-1] in refobjects[i]:
                del refobjects[i][field[:-1]]; #TODO: New, test!
    return ids, refobjects, matchobjects;

def search(field,id_field,query_fields,index,index_m,great_score,ok_score,thr_prec,max_rel_diff,threshold,transformap,recheck):
    #----------------------------------------------------------------------------------------------------------------------------------
    OUT = open(_logfile,'w');
    con = sqlite3.connect(_bufferdb);
    cur = con.cursor();
    cur.execute("CREATE TABLE IF NOT EXISTS "+index_m+"(query TEXT PRIMARY KEY, result TEXT)");
    #----------------------------------------------------------------------------------------------------------------------------------
    ref_fields      = ['id'] + [transformap[key][0].split('.')[0] for key in transformap] + [field[:-1]];
    match_fields    = [id_field] + list(transformap.keys());
    body            = { '_op_type': 'update', '_index': index, '_id': None, '_source': { 'doc': { 'processed_'+field: True, field: None } } };
    query_doi       = { 'match':        { 'doi'  : None } } if 'doi' in query_fields else None;
    query_title     = { 'match_phrase': { 'title': None } };
    query_refstring = { 'multi_match':  { 'query': None, 'fields': query_fields } };
    #scr_query       = { "ids": { "values": _ids } } if _ids else {'bool':{'must_not':{'term':{'processed_'+field: True}}}} if not recheck else {'match_all':{}};
    scr_query       = { "ids": { "values": _ids } } if _ids else { 'bool':{'must_not': [{'term':{'processed_'+field: True}}], 'should': [{'term':{'has_'+refobj:True}} for refobj in _refobjs] } } if not recheck else {'bool': {'should': [{'term':{'has_'+refobj:True}} for refobj in _refobjs]}};
    #----------------------------------------------------------------------------------------------------------------------------------
    print('------------------->',scr_query);
    client   = ES(['localhost'],scheme='http',port=9200,timeout=60);
    client_m = ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,query=scr_query,_source=[field]+_refobjs);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    print('------------------->',page['hits']['total']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            print('---------------------------------------------------------------------------------------------\n',doc['_id'],'---------------------------------------------------------------------------------------------\n');
            body         = copy(body);
            body['_id']  = doc['_id'];
            ids          = set([]); #set(doc['_source'][field]) if field in doc['_source'] and doc['_source'][field] != None else set([]); #TODO: New, test!
            matched_refs = dict();
            for refobj in _refobjs:
                #previous_refobjects            = [{ ref_field: ref[ref_field] for ref_field in ref_fields if ref_field in ref } for ref in doc['_source'][refobj]] if refobj in doc['_source'] and doc['_source'][refobj] else None;
                previous_refobjects                    = doc['_source'][refobj] if refobj in doc['_source'] and doc['_source'][refobj] else None;
                #print(previous_refobjects)
                new_ids, new_refobjects, matchobjects    = find(previous_refobjects,client_m,index_m,field,query_doi,query_title,query_refstring,match_fields,great_score,ok_score,thr_prec,max_rel_diff,threshold,transformap,id_field,OUT,cur) if isinstance(previous_refobjects,list) else (set([]),previous_refobjects,[]);
                new_ids_set                              = set(new_ids);
                ids                                     |= new_ids_set;
                body['_source']['doc'][refobj]           = new_refobjects; # The updated ones
                body['_source']['doc'][field+'_'+refobj] = list(new_ids);
                silver_refobjects                        = body['_source']['doc']['silver_refobjects'] if 'silver_refobjects' in body['_source']['doc'] else dict();
                if refobj in silver_refobjects:         #TODO: Not sure what is the point of the above commented out line and this if clause. Can't we just write the new matchobjects there?
                    silver_refobjects[refobj][index_m] = matchobjects;
                else:
                    silver_refobjects[refobj] = {index_m: matchobjects};
                for i in range(len(new_ids)):
                    if not new_ids[i] in matched_refs:
                        matched_refs[new_ids[i]] = matchobjects[i];
                #body['_source']['doc']['silver_refobjects'] = silver_refobjects;#{refobj: {index_m: new_refobjects}}; # The updated ones #TODO: Use again, just could not overwrite index
                print('-->',refobj,'gave',['','no '][len(new_ids_set)==0]+'ids',', '.join(new_ids_set),'\n');
            body['_source']['doc']['matched_references_from_'+index_m] = make_refs(matched_refs,index_m);
            print('------------------------------------------------\n-- overall ids --------------------------------\n'+', '.join(ids)+'\n------------------------------------------------');
            body['_source']['doc'][field]              = list(ids) #if len(ids) > 0 else None;
            body['_source']['doc']['processed_'+field] = True      #if len(ids) > 0 else False;
            body['_source']['doc']['has_'+field]       = len(ids) > 0;
            body['_source']['doc']['num_'+field]       = len(ids);
            yield body;
            con.commit();
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time*_scroll_size))+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e, file=sys.stderr);
                print('\n[!]-----> Some problem occured while scrolling. Sleeping for 3s and retrying...\n');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;
    client.clear_scroll(scroll_id=sid);
    OUT.close();
    con.close();
