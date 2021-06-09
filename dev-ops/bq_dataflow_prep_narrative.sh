#  som-rit-phi-starr-dev:rit_phi_clarity_lpch_20130101_20180930   som-rit-phi-starr-dev:rit_phi_clarity_deid_lpch $3
echo "FROM:$1 $2 TO:$3"

read -r -d '' _QUERY <<-BND_OF_QUERY

select text_table.ORDER_PROC_ID,text_table.FULL_NARRATIVE,pat.*
from \`$2.ORDER_NARRATIVE_MERGED\`  text_table
left join \`$1.ORDER_PROC\`  order_proc on order_proc.ORDER_PROC_ID = text_table.ORDER_PROC_ID
left join \`$2.PHI_MERGED\`  pat on order_proc.pat_id = pat.pat_id
where text_table.FULL_NARRATIVE is not null

BND_OF_QUERY
QUERY=$(echo "$_QUERY" | sed 's/\n/ /g')

echo $QUERY

set -x
bq --location=US query --use_cache=false --destination_table $3  --use_legacy_sql=false "`echo $QUERY`"
set +x
