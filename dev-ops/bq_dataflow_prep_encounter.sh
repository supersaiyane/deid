
echo "FROM:$1 join $2 TO:$3"

read -r -d '' _QUERY <<-BND_OF_QUERY

select text_table.PAT_ENC_CSN_ID,text_table.LINE,text_table.COMMENTS,
pat.* from \`$1.PAT_ENC_RSN_VISIT\`  text_table
left join \`$2.PHI_MERGED\`  pat on text_table.pat_id = pat.pat_id where text_table.COMMENTS is not null

BND_OF_QUERY
QUERY=$(echo "$_QUERY" | sed 's/\n/ /g')

echo $QUERY

set -x
bq --location=US query --use_cache=false --destination_table $3  --use_legacy_sql=false "`echo $QUERY`"
set +x
