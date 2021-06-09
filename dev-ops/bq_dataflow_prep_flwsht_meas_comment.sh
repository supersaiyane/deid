#som-rit-phi-starr-dev.rit_phi_clarity_shc_20130101_20180930 som-rit-phi-starr-dev.rit_phi_clarity_deid_shc $3
echo "FROM:$1 join $2 TO:$3"

read -r -d '' _QUERY <<-BND_OF_QUERY

select text_table.FSD_ID,text_table.LINE,text_table.MEAS_VALUE,text_table.meas_comment,pat.*
from \`$1.IP_FLWSHT_MEAS\`  text_table
left join \`$1.IP_FLWSHT_REC\`  rec on rec.FSD_ID = text_table.FSD_ID
left join \`$2.PHI_MERGED\`  pat on rec.pat_id = pat.pat_id and text_table.MEAS_VALUE is not null and ARRAY_LENGTH(SPLIT( text_table.MEAS_VALUE,' '))>1


BND_OF_QUERY
QUERY=$(echo "$_QUERY" | sed 's/\n/ /g')

echo $QUERY

set -x
bq --location=US query --use_cache=false --headless --destination_table $3  --use_legacy_sql=false "`echo $QUERY`"
set +x
