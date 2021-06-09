#som-rit-phi-starr-dev:rit_phi_clarity_shc_20130101_20180930 som-rit-phi-starr-dev:rit_phi_clarity_deid_shc som-rit-phi-starr-dev:rit_phi_clarity_deid_shc.HNO_NOTE_TEXT_MERGED

echo "FROM:$1 TO:$2"

read -r -d '' _QUERY <<-BND_OF_QUERY

SELECT impressionText.ORDER_PROC_ID, string_agg(impressionText.IMPRESSION, ' '  order by impressionText.line ) as FULL_IMPRESSION

from \`$1.ORDER_IMPRESSION\` impressionText

where impressionText.IMPRESSION is not null

group by impressionText.ORDER_PROC_ID

BND_OF_QUERY
QUERY=$(echo "$_QUERY" | sed 's/\n/ /g')

echo $QUERY

set -x
bq --location=US query --destination_table $3  --use_legacy_sql=false "`echo $QUERY`"
set +x
