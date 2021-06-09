
echo "FROM:$1 TO:$2"

read -r -d '' _QUERY <<-BND_OF_QUERY

SELECT narrativeText.ORDER_PROC_ID, string_agg(narrativeText.NARRATIVE, ' '  order by narrativeText.line ) as FULL_NARRATIVE

from \`$1.ORDER_NARRATIVE\` narrativeText

where narrativeText.NARRATIVE is not null

group by narrativeText.ORDER_PROC_ID

BND_OF_QUERY
QUERY=$(echo "$_QUERY" | sed 's/\n/ /g')

echo $QUERY

set -x
bq --location=US query --destination_table $3  --use_legacy_sql=false "`echo $QUERY`"
set +x
