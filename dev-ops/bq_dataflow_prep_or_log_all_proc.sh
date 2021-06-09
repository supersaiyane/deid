#som-rit-phi-starr-dev.rit_phi_clarity_shc_20130101_20180930 som-rit-phi-starr-dev.rit_phi_clarity_deid_shc $3
echo "FROM:$1 join $2 TO:$3"

read -r -d '' _QUERY <<-BND_OF_QUERY

select text_table.LOG_ID,text_table.LINE,text_table.COMMENTS,emp.name as EMP_NAME,pat.*
from \`$1.OR_LOG_ALL_PROC\`  text_table
left join \`$1.OR_LOG\`  log on log.log_id = text_table.log_id
left join \`$2.CLARITY_EMP\` emp on emp.user_id = log.primary_phys_id
left join \`$2.PHI_MERGED\`  pat on log.pat_id = pat.pat_id

where text_table.COMMENTS is not null

BND_OF_QUERY
QUERY=$(echo "$_QUERY" | sed 's/\n/ /g')

echo $QUERY

set -x
bq --location=US query --use_cache=false --destination_table $3  --use_legacy_sql=false "`echo $QUERY`"
set +x
