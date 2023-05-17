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
_use_buffered     = _configs["use_buffered"];
_dateweight       = _configs["date_weight"];
#'''
_refobjs = _configs["refobjs"];

_ids     = _configs["ids"];#['GaS_2000_0001'];#["gesis-ssoar-29359","gesis-ssoar-55603","gesis-ssoar-37157","gesis-ssoar-5917","gesis-ssoar-21970"];#None
#'''
#_refobjs = [ 'anystyle_references_from_gold_refstrings' ];

YEAR      = re.compile(r'1[5-9][0-9]{2}|20(0[0-9]|1[0-9]|2[0-3])'); #1500--2023
NAMESEP   = re.compile(r'\W');
GARBAGE   = re.compile(r'\W')#re.compile(r'[\x00-\x1f\x7f-\x9f]|(-\s+)');
SOURCEKEY = re.compile(r"source\['[A-Za-z|_]+[1-9|A-Za-z|_]*'\]");

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

def transform(source,transformap):
    target = dict();
    for target_key, source_str in transformap:
        source_val = None;
        try:
            #print(source_str)
            source_val = eval(source_str,{'source':source},{'source':source});
        except Exception as e:
            #traceback.print_exc();
            pass;#print(e);
        if source_val:
            target[target_key] = source_val;
    return target;

def transform_(result,transformap):
    matchobj = dict();
    for match_keystr in transformap:
        #--------------------------------------------------------------------------------------------------------------------------------------------------------
        match_keys     = match_keystr.split('.');                                                               # The path in the matchobj
        match_pointers = list(walk_down(result,match_keys));                                                          # The information stored at the end of the path
        if len(match_pointers)==0:                                                          # If the path does not exist
            continue;
        ref_keystr,get_1st,default = transformap[match_keystr];                                                    # The information from the transformation mapping
        match_values               = extract(match_pointers) if get_1st and len(match_pointers)>=1 else match_pointers;  # The extracted information #TODO: this did not work
        ref_keys                   = ref_keystr.split('.');                                                        # The path in the refobj
        #--------------------------------------------------------------------------------------------------------------------------------------------------------
        if len(ref_keys) == 1:
            matchobj_ = {ref_keys[0]: match_values};
        else:
            if default == []:
                matchobj_ = dict();#{ref_keys[0]: []};
                values    = match_values if isinstance(match_values,list) else [match_values];
                matchobj_[ref_keys[0]] = [dict() for j in range(len(values))] if not ref_keys[0] in matchobj_ else matchobj_[ref_keys[0]];
                for j in range(len(values)):
                    ref_pointer = matchobj_[ref_keys[0]][j];
                    for i in range(1,len(ref_keys)):
                        ref_pointer[ref_keys[i]] = dict();
                        if i+1 < len(ref_keys):
                            ref_pointer = ref_pointer[ref_keys[i]];
                        elif values[j] and len(values[j]) > 0:
                            ref_pointer[ref_keys[i]] = values[j];
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
        print(match_keystr,ref_keystr,get_1st);
        print(default,matchobj,matchobj_);
        #--------------------------------------------------------------------------------------------------------------------------------------------------------
        #keys = list(matchobj.keys());
        #for key in keys:
        #    if not matchobj[key] or len(matchobj[key]) == 0:
        #        del matchobj[key];
    matchobj = remove_empty(matchobj);
    print('----------------------------------------------------------')
    print(matchobj);
    print('----------------------------------------------------------')
    return matchobj;


def transform_(result,transformap): #TODO: This whole stupid approach still does not work
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

def distance_3(a,b):
    a,b        = '_'+re.sub(GARBAGE,'',a.lower()),'_'+re.sub(GARBAGE,'',b.lower());#a.lower(), b.lower();
    s          = SM(None,a,b);
    overlap    = sum([block.size**1 for block in s.get_matching_blocks() if block.size>=2]);
    dist       = min([len(a),len(b)])**1-overlap;
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
    M          = np.array([[distance_3(a,b) if isinstance(a,str) and isinstance(b,str) else a!=b for b in B] for a in A]);
    rows, cols = LSA(M);
    mapping    = [pair for pair in zip(rows,cols)];
    costs      = [M[assignment] for assignment in mapping];
    return mapping,costs;

def similar_enough(a,b,cost,threshold):
    if isinstance(a,str) and isinstance(b,str):
        if YEAR.fullmatch(a) and YEAR.fullmatch(b):
            y1, y2 = int(a), int(b);
            return abs(y1-y2) <= 1; # A one year difference between years is accepted
        return cost / min([len(a),len(b)])**1 < threshold;#max and not **1
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

def get_best_match(refobj,results,query_field,query_val,great_score,ok_score,thr_prec,max_rel_diff,threshold,transformap,id_field,OUT):
    #TITLE = True if 'title' in refobj and refobj['title'] else False;
    #query_val = refobj[query_field];
    if len(results) > 0:
            log(['____________________________________________________________________________________________________________________'],OUT);
    else:
        return (None,None);
    log(['=REFER===============================================================================\n'+'\n'.join([key+':    '+str(refobj[key])   for key in refobj  ])+'\n================================================================================'],OUT);
    log(['=QUERY===============================================================================\n'+query_field.upper()+': '+query_val+'\n====================================================================================='],OUT);
    for score,source in results: # This will still return the first matching result, but if the ranking order is not so good, then a later one also has a chance
        matchobj = transform(source,transformap);
        log(['=MATCH===============================================================================\n'+'\n'.join([key+':    '+str(matchobj[key]) for key in matchobj])],OUT);
        #matchobj_ = {key:matchobj[key] if key!='authors' else [name_part for author in matchobj['authors'] for name_part in [[],NAMESEP.split(author['author_string'])]['author_string' in author and author['author_string']]] for key in matchobj};
        #refobj_   = {key:refobj  [key] if key!='authors' else [name_part for author in refobj  ['authors'] for name_part in [[],NAMESEP.split(author['author_string'])]['author_string' in author and author['author_string']]] for key in refobj  };
        matchobj_ = {key:matchobj[key] if key!='authors' else [{'author_string':[part for part in NAMESEP.split(author['author_string']) if part]} for author in matchobj['authors'] if 'author_string' in author and author['author_string']] for key in matchobj if matchobj[key] not in [None,'None',' ',''] };
        refobj_   = {key:refobj  [key] if key!='authors' else [{'author_string':[part for part in NAMESEP.split(author['author_string']) if part]} for author in refobj  ['authors'] if 'author_string' in author and author['author_string']] for key in refobj   if refobj  [key] not in [None,'None',' ',''] };
        prec, rec, tp, p, t, matches, mismatches, mapping, costs = compare_refobject(matchobj_,refobj_,threshold);
        #matchprec                                                = len(matches)/(len(matches)+len(mismatches)) if len(matches)+len(mismatches) > 0 else 0;
        matchprec                                                = sum([min(len(a),len(b)) if key!='_year' else _dateweight for key,a,b in matches])/(sum([min(len(a),len(b)) if key!='_year' else _dateweight for key,a,b in matches])+sum([min(len(a),len(b)) if key!='_year' else _dateweight for key,a,b in mismatches])) if sum([min(len(a),len(b)) if key!='_year' else _dateweight for key,a,b in matches])+sum([min(len(a),len(b)) if key!='_year' else _dateweight for key,a,b in mismatches]) > 0 else 0;
        if len(matches)+len(mismatches) > 0:
            log(['Matchprec:',matchprec,'Precision:',prec,'Recall:',rec,'\n___________________________________'],OUT);
            log(['Matches:   ',matches],OUT);
            log(['Mismatches:',mismatches,'\n___________________________________'],OUT);
        title = source['title'][0] if isinstance(source['title'],list) and len(source['title'])>0 else '' if isinstance(source['title'],list) else source['title'];
        if score > great_score[query_field=='title']:
            log(['PASSED '+query_field.upper()+'-query: score',score,'>',great_score[query_field=='title']],OUT);
            if matchprec >= thr_prec and len(matches)>1 and id_field in source:
                log(['DID MATCH:',matchprec,'>=',thr_prec,'and #matches =',len(matches)],OUT);
                return (source[id_field],matchobj,);
            log(['DID NOT MATCH.'],OUT);
        elif score > ok_score[query_field=='title'] and title and distance(query_val,title) < max_rel_diff[query_field=='title']:
            log(['PASSED '+query_field.upper()+'-query: score',score,'>',ok_score[query_field=='title'],'and distance',distance(query_val,title),'<',max_rel_diff[query_field=='title']],OUT);
            if matchprec >= thr_prec and len(matches)>1 and id_field in source:
                log(['DID MATCH:',matchprec,'>=',thr_prec,'and #matches =',len(matches)],OUT);
                return (source[id_field],matchobj,);
            log(['DID NOT MATCH.'],OUT);
            #matchID = source[id_field] if matchprec >= thr_prec and id_field in source else None;
            #return (matchID,matchobj) if matchID else (None,None);
        if (not query_val) or not title:
            log(['FAILED:',query_val,title],OUT);
        else:
            log(['FAILED: score',score,'<=',ok_score[query_field=='title'],'and/or distance',distance(query_val,title),'>=',max_rel_diff[query_field=='title'],'and/or did not match'],OUT);
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

def find(refobjects,client,index,field,query_doi,query_title,query_refstring,great_score,ok_score,thr_prec,max_rel_diff,threshold,transformap,id_field,OUT,cur):
    sourcefields = list(set(['title',id_field]+[match[8:-2] for match in SOURCEKEY.findall(" ".join([fro for to,fro in transformap]))]));
    ids          = [];
    matchobjects = [];
    for i in range(len(refobjects)):
        results_doi    = [];
        results_title  = [];
        results_refstr = [];
        ID, match_obj  = None, None;
        t              = time.time();
        log(['------------------------------------------------'],OUT);
        if query_doi and 'doi' in refobjects[i] and refobjects[i]['doi']:
            query                 = copy(query_doi);
            query['match']['doi'] = refobjects[i]['doi'][:_max_val_len]; log([query],OUT);
            results_doi           = lookup(query,index,cur) if _use_buffered else None;
            if not results_doi:
                results_doi      = client.search(index=index,query=query,size=5,_source=sourcefields)
                results_doi,took = results_doi['hits']['hits'], results_doi['took'];
                store(query,results_doi,index,cur); # This requires no significant time
                log(['doi:',took,time.time()-t,len(query['match']['doi'])],OUT); t = time.time();
            best_results_doi    = [(result_doi['_score'],result_doi['_source'],) for result_doi in results_doi];
            ID, match_obj       = get_best_match(refobjects[i],best_results_doi,'doi',query['match']['doi'  ],great_score,ok_score,thr_prec,max_rel_diff,threshold,transformap,id_field,OUT) if best_results_doi else [None,None];
            log(results_doi,OUT);
        if ('title' in refobjects[i] and refobjects[i]['title']) or ('reference' in refobjects[i] and refobjects[i]['reference']):
            query                   = copy(query_title);
            query['match']['title'] = refobjects[i]['title'][:_max_val_len] if 'title' in refobjects[i] and refobjects[i]['title'] else refobjects[i]['reference'][:_max_val_len]; log([query],OUT);
            results_title           = lookup(query,index,cur) if _use_buffered else None;
            if not results_title:
                results_title      = client.search(index=index,query=query,size=5,_source=sourcefields)
                results_title,took = results_title['hits']['hits'], results_title['took'];
                store(query,results_title,index,cur); # This requires no significant time
                log(['title:',took,time.time()-t,len(query['match']['title'])],OUT); t = time.time();
            best_results_title  = [(result_title['_score'],result_title['_source'],) for result_title in results_title];
            ID, match_obj       = get_best_match(refobjects[i],best_results_title ,'title'    ,query['match']['title'],great_score,ok_score,thr_prec,max_rel_diff,threshold,transformap,id_field,OUT) if best_results_title  and not ID else [ID,match_obj];
            log(results_title,OUT);
        if 'reference' in refobjects[i] and refobjects[i]['reference']:
            query                    = copy(query_refstring);
            query['match']['refstr'] = refobjects[i]['reference'][:_max_val_len]; log([query],OUT);
            results_refstr           = lookup(query,index,cur) if _use_buffered else None;
            if not results_refstr:
                results_refstr      = client.search(index=index,query=query,size=5,_source=sourcefields)
                results_refstr,took = results_refstr['hits']['hits'], results_refstr['took'];
                store(query,results_refstr,index,cur); # This requires no significant time
                log(['reference:',took,time.time()-t,len(query['match']['refstr'])],OUT); t = time.time();
            best_results_refstr = [(result_refstr['_score'],result_refstr['_source'],) for result_refstr in results_refstr];
            ID, match_obj       = get_best_match(refobjects[i],best_results_refstr,'reference',query['match']['refstr'],great_score,ok_score,thr_prec,max_rel_diff,threshold,transformap,id_field,OUT) if best_results_refstr and not ID else [ID,match_obj];
            log(results_refstr,OUT);
        if ID != None:
            refobjects[i][field[:-1]] = ID;
            ids.append(ID);
            matchobjects.append(match_obj);
            #print(refobjects[i]);
        else:
            if field[:-1] in refobjects[i]:
                del refobjects[i][field[:-1]];
    return ids, refobjects, matchobjects;

def search(field,id_field,query_fields,index,index_m,great_score,ok_score,thr_prec,max_rel_diff,threshold,transformap,recheck):
    #----------------------------------------------------------------------------------------------------------------------------------
    OUT = open(index_m+'_'+_logfile,'w');
    con = sqlite3.connect(index_m+'_'+_bufferdb);
    cur = con.cursor();
    cur.execute("CREATE TABLE IF NOT EXISTS "+index_m+"(query TEXT PRIMARY KEY, result TEXT)");
    #----------------------------------------------------------------------------------------------------------------------------------
    #ref_fields      = ['id'] + [transformap[key][0].split('.')[0] for key in transformap] + [field[:-1]];
    #match_fields    = [id_field] + list(transformap.keys());
    body            = { '_op_type': 'update', '_index': index, '_id': None, '_source': { 'doc': { 'processed_'+field: True, field: None } } };
    query_doi       = { 'match': { 'doi'  : None } } if 'doi' in query_fields else None;
    query_title     = { 'match': { 'title': None } };
    #query_refstring = { 'multi_match':  { 'query': None, 'fields': query_fields } };
    query_refstring = { 'match': { 'refstr': None } }; #TODO: Change to this after refstr is available!
    #scr_query       = { "ids": { "values": _ids } } if _ids else {'bool':{'must_not':{'term':{'processed_'+field: True}}}} if not recheck else {'match_all':{}};
    scr_query       = { "ids": { "values": _ids } } if _ids else { 'bool':{'must_not': [{'term':{'processed_'+field: True}}], 'should': [{'term':{'has_'+refobj:True}} for refobj in _refobjs] } } if not recheck else {'bool': {'should': [{'term':{'has_'+refobj:True}} for refobj in _refobjs]}};
    #----------------------------------------------------------------------------------------------------------------------------------
    print('------------------->',scr_query);
    client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
    client_m = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
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
                log(['---->',refobj],OUT);
                new_ids, new_refobjects, matchobjects    = find(previous_refobjects,client_m,index_m,field,query_doi,query_title,query_refstring,great_score,ok_score,thr_prec,max_rel_diff,threshold,transformap,id_field,OUT,cur) if isinstance(previous_refobjects,list) else (set([]),previous_refobjects,[]);
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
                body['_source']['doc']['silver_refobjects'] = silver_refobjects;#{refobj: {index_m: new_refobjects}}; # The updated ones #TODO: Use again, just could not overwrite index
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
