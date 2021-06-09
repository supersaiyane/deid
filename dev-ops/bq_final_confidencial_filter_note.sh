#som-rit-phi-starr-dev.rit_phi_deidentified_shc.HNO_NOTE_TEXT_MERGED_DEIDED_2 som-rit-phi-starr-dev:rit_phi_deidentified_shc.HNO_NOTE_TEXT_MERGED_FILTERED
#som-rit-phi-starr-dev.rit_phi_deidentified_lpch.HNO_NOTE_TEXT_MERGED_DEIDED_2 som-rit-phi-starr-dev:rit_phi_deidentified_lpch.HNO_NOTE_TEXT_MERGED_FILTERED


FROM="$1"
TO="$2"
echo "FROM:$FROM TO:$TO"

read -r -d '' _QUERY <<-BND_OF_QUERY

SELECT note_id , note_csn_id , TEXT_DEID_fullnote, fullnote FROM \`${FROM}\` where (not STARTS_WITH(TEXT_DEID_fullnote,"confidential") or STRPOS(TEXT_DEID_fullnote,"confidential") > 80) and not STARTS_WITH(TEXT_DEID_fullnote,"no dictation") and not REGEXP_CONTAINS(TEXT_DEID_fullnote, r"(?i)document deleted|incomplete report|confidential psychiatric record|protected diagnosis|psychiatric diagnosis|protected note psychotherapy|protected note|psychology evaluation confidential|psychology addendum confidential|psychology consultation confidential|confidential neuropsychological evaluation|refer to medical record for result|see patient's record for consultation note or visit note|confidential patient name|confidential neuropsychological assessment|protected record|this document is confidential|confidential adolescent history|confidential admission?: yes|confidential social hx|start confidential|confidential material|confidential adolescent information|confidential neuropsychological evaluation|do not release to patient or others|private and confidential|sensitive confidential information|confidential record|confidential social history|confidential neuropsychological|confidential patient|confidential history|confidential portion|confidential status|confidential section|strictly prohibited without permission of the patient|confidential this report may contain sensitive information|begin confidential|following history is confidential|confidential hx|confidential heads|confidential assessment|confidential concerns|protected psychiatric diagnosis|confidential document|confidential psychiatric|confidential diagnos|confidential dx|confidential neuropsychiatric|kept confidential|completely confidential|confidential social|cannot be released to insurance companies and/or parents without express written consent of the patient")

BND_OF_QUERY
QUERY=$(echo "$_QUERY" | sed 's/\n/ /g')

echo $QUERY

set -x
bq --location=US query --use_cache=false --destination_table ${TO}  --use_legacy_sql=false "`echo $QUERY`"
set +x
