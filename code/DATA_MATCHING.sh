DOCINDEX=$1

LOGFILE=logs/${DOCINDEX}.out
ERRFILE=logs/${DOCINDEX}.err

cd /home/outcite/data_matching/

> $LOGFILE
> $ERRFILE

echo "...Tries to find matches for extracted (and parsed) references to the target collections stored locally as the OUTCITE target Elasticsearch indices."
echo "   This involves first issuing different Elasticsearch queries (currently doi-match, title-match, and refstring-match)"
echo "   as well as a following feature-by-feature matching procedure that filters out non-matching results returned by Elasticsearch."

for target in ssoar gesis_bib research_data sowiport econbiz dnb crossref arxiv openalex; do
    echo "...${target}"
    python code/update_${target}.py $DOCINDEX >>$LOGFILE 2>>$ERRFILE
done
