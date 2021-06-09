#som-rit-phi-starr-dev:rit_phi_clarity_shc_20130101_20180930 som-rit-phi-starr-dev:rit_phi_clarity_deid_shc som-rit-phi-starr-dev:rit_phi_clarity_deid_shc.HNO_NOTE_TEXT_MERGED

echo "FROM:$1 TO:$2"

read -r -d '' _QUERY <<-BND_OF_QUERY

select note.note_id,note.fullnote,
note.patmsg_pat_id,
note.rte_trigrd_pat_id,
note.comment_user_id,
note.cm_phy_owner_id ,
note.cm_log_owner_id,
note.entry_user_id,
note.ecg_technician_id,
note.rte_trigger_user_id,
note.ib_resp_user_id,
note.note_csn_id,
note.contact_num,
note.CONTACT_SERIAL_NUM,
note.ENTRY_INSTANT_DTTM,
emp.name as EMP_NAME,
pat.* from \`$1.HNO_NOTE_TEXT_MERGED\`  note
left join \`$1.PHI_MERGED\`  pat on note.pat_id = pat.pat_id
left join \`$1.CLARITY_EMP\` emp on note.entry_user_id is not null and emp.user_id = note.entry_user_id

BND_OF_QUERY
QUERY=$(echo "$_QUERY" | sed 's/\n/ /g')

echo $QUERY

set -x
bq --location=US query --use_cache=false --destination_table $2  --use_legacy_sql=false "`echo $QUERY`"
set +x
