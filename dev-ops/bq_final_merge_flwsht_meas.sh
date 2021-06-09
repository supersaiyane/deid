#bash bq_query_final_merge_flwsht_meas.sh som-rit-phi-starr-dev.rit_phi_clarity_lpch_20130101_20180930 som-rit-phi-starr-dev.rit_phi_deidentified_lpch som-rit-phi-starr-dev:rit_phi_deidentified_lpch.IP_FLWSHT_MEAS_FULL_DEIDED
#bash bq_query_final_merge_flwsht_meas.sh som-rit-phi-starr-dev.rit_phi_clarity_shc_20130101_20180930 som-rit-phi-starr-dev.rit_phi_deidentified_shc som-rit-phi-starr-dev:rit_phi_deidentified_shc.IP_FLWSHT_MEAS_FULL_DEIDED

echo "FROM:$1 COALESCE $2 TO:$3"

read -r -d '' _QUERY <<-BND_OF_QUERY

select text_table.FSD_ID,text_table.LINE,
IF((run2.text_1 is not null), run2.text_1,(IF((run1.MEAS_VALUE is not null), run1.MEAS_VALUE,text_table.MEAS_VALUE))) as MEAS_VALUE,
IF((run2.text_2 is not null), run2.text_2,(IF((run1.MEAS_COMMENT is not null), run1.MEAS_COMMENT,text_table.MEAS_COMMENT))) as MEAS_COMMENT
from \`$1.IP_FLWSHT_MEAS\`  text_table
left join \`$2.IP_FLWSHT_MEAS_PHI_PARTIAL_RERUN_201901181037\`  run2 on cast(run2.text_id_1 as String) = text_table.FSD_ID and run2.text_id_2 = text_table.LINE
left join \`$2.IP_FLWSHT_MEAS_VALUE_COMMENT_DEIDED\`  run1  on run1.FSD_ID = text_table.FSD_ID and run1.LINE = text_table.LINE

BND_OF_QUERY
QUERY=$(echo "$_QUERY" | sed 's/\n/ /g')

echo $QUERY

set -x
bq --location=US query --use_cache=false --headless --destination_table $3  --use_legacy_sql=false "`echo $QUERY`"
set +x



