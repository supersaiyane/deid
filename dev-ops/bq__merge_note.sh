#som-rit-phi-starr-dev:rit_phi_clarity_shc_20130101_20180930 som-rit-phi-starr-dev:rit_phi_clarity_deid_shc som-rit-phi-starr-dev:rit_phi_clarity_deid_shc.HNO_NOTE_TEXT_MERGED

echo "FROM:$1 TO:$2"

read -r -d '' _QUERY <<-BND_OF_QUERY

SELECT noteText.note_id, string_agg(noteText.note_text, ' '  order by noteText.line ) as fullnote,
info.pat_id,
info.patmsg_pat_id,
info.rte_trigrd_pat_id,
info.comment_user_id,
info.cm_phy_owner_id ,
info.cm_log_owner_id,
info.entry_user_id,
info.ecg_technician_id,
info.rte_trigger_user_id,
info.ib_resp_user_id,
noteText.note_csn_id,
enc.contact_num,
enc.CONTACT_SERIAL_NUM,
enc.ENTRY_INSTANT_DTTM,
emp.name as EMP_NAME

from \`$1.HNO_NOTE_TEXT\` noteText
left join \`$1.HNO_INFO\` info on info.pat_id is not null and info.note_id = noteText.note_id
left join \`$1.NOTE_ENC_INFO\` enc on noteText.note_csn_id is not null and enc.contact_serial_num = noteText.note_csn_id and enc.NOTE_STATUS_C != '4'
left join \`$2.CLARITY_EMP\` emp
    on info.entry_user_id is not null and emp.user_id = info.entry_user_id

where noteText.IS_ARCHIVED_YN = 'N'

group by noteText.note_id, noteText.note_CSN_id, enc.contact_num, enc.CONTACT_SERIAL_NUM, enc.ENTRY_INSTANT_DTTM, info.pat_id,info.patmsg_pat_id, info.rte_trigrd_pat_id, info.comment_user_id, info.cm_phy_owner_id , info.cm_log_owner_id, info.entry_user_id, info.ecg_technician_id, info.rte_trigger_user_id, info.ib_resp_user_id,
emp.name

BND_OF_QUERY
QUERY=$(echo "$_QUERY" | sed 's/\n/ /g')

echo $QUERY

set -x
bq --location=US query --destination_table $3  --use_legacy_sql=false "`echo $QUERY`"
set +x
