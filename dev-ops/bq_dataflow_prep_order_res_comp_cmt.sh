#som-rit-phi-starr-dev.rit_phi_clarity_shc_20130101_20180930 som-rit-phi-starr-dev.rit_phi_clarity_deid_shc $3
echo "FROM:$1 join $2 TO:$3"

read -r -d '' _QUERY <<-BND_OF_QUERY

select text_table.ORDER_ID,text_table.CONTACT_DATE_REAL,text_table.LINE_COMP,text_table.LINE_COMMENT,text_table.RESULTS_COMP_CMT,ser.prov_name as EMP_NAME,pat.*
from \`$1.ORDER_RES_COMP_CMT\`  text_table
left join (\`$1.ORDER_PROC_FULL\`  proc inner join \`$2.PHI_MERGED\` pat on pat.pat_id = proc.pat_id ) on proc.order_proc_id = text_table.order_id
left join (\`$1.ORDER_PROC_FULL\`  proc2 inner join \`$1.CLARITY_SER\` ser on ser.prov_id = proc2.authrzing_prov_id ) on proc2.order_proc_id = text_table.order_id

where text_table.RESULTS_COMP_CMT is not null

BND_OF_QUERY
QUERY=$(echo "$_QUERY" | sed 's/\n/ /g')

echo $QUERY

set -x
bq --location=US query --use_cache=false --headless --destination_table $3  --use_legacy_sql=false "`echo $QUERY`"
set +x
