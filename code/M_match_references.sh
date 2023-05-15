index=$1

codedir_matching=/home/outcite/data_matching/code/
logdir_matching=/home/outcite/data_matching/logs/

mkdir -p $logdir_matching

for target in sowiport crossref dnb openalex ssoar arxiv econbiz gesis_bib research_data; do
    echo matching to ${target}
    python ${codedir_matching}update_${target}.py ${index} >${logdir_matching}${index}_${target}.out  2>${logdir_matching}${index}_${target}.err;
done
