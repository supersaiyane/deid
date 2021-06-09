FROM="$1"
TO="$2"

echo "FROM:$FROM TO:$TO"

read -r -d '' _QUERY <<-BND_OF_QUERY


CREATE TEMP FUNCTION
  countFindingByType(findingstr STRING)
  RETURNS ARRAY<STRUCT<t STRING,
  cnt INT64>>
  LANGUAGE js AS """

  var resultMap = {};
  findings = JSON.parse(findingstr);
  for (var key in findings) {
      finding = findings[key];
    var type = finding.type;
    if (resultMap[type]===undefined || resultMap[type]=='undefined' || !Number.isInteger(resultMap[type])) {
      resultMap[type] = 1;
    } else {
      resultMap[type] = resultMap[type] + 1;
    }
  }
  var out = [];
  for (var k in resultMap) {
    out.push ({t:k,cnt:resultMap[k]});
  }
  return out;
""";
CREATE TEMP FUNCTION
  countFindingByMethod(findingstr STRING)
  RETURNS ARRAY<STRUCT<t STRING,
  cnt INT64>>
  LANGUAGE js AS """

  var resultMap = {};
  findings = JSON.parse(findingstr);
  for (var key in findings) {
      finding = findings[key];
    var foundBy = finding.foundBy;
    if (resultMap[foundBy]===undefined || resultMap[foundBy]=='undefined' || !Number.isInteger(resultMap[foundBy])) {
      resultMap[foundBy] = 1;
    } else {
      resultMap[foundBy] = resultMap[foundBy] + 1;
    }
  }
  var out = [];
  for (var k in resultMap) {
    out.push ({t:k,cnt:resultMap[k]});
  }
  return out;
""";
CREATE TEMP FUNCTION
  countUniqueFinding(findingstr STRING)
  RETURNS INT64
  LANGUAGE js AS """

  var resultMap = {};
  findings = JSON.parse(findingstr);
  for (var f in findings) {
    finding = findings[f];
    var key = 'pos_'+finding.start+'_'+finding.end;
    if (resultMap[key]===undefined || resultMap[key]=='undefined' || !Number.isInteger(resultMap[key])) {
      resultMap[key] = 1;
    } else {
      resultMap[key] = resultMap[key] + 1;
    }
  }
  var out = 0;
  for (var k in resultMap) {
    //out = out + (resultMap[k]);
    out ++;
  }
  return out;
""";
CREATE TEMP FUNCTION
  INT_ARRAY_TO_STRING(x ARRAY<INT64>,
    y STRING)
  RETURNS STRING
  LANGUAGE js AS """
  let out = '';
  for ( var i = 0 ; i < x.length; i ++) {
    out = out + x[i] + y;
  }
  return out.length > 0 ? out.substring(0, out.length - 2) : out;
""";

WITH
  findingsPerText AS (
  SELECT AS STRUCT
    countFindingByType (deid.FINDING_note_text) AS type,
    countFindingByMethod (deid.FINDING_note_text) AS foundBy,
    countUniqueFinding (deid.FINDING_note_text) AS uniqueFindingCount
  FROM

    \`${FROM}\` deid
  WHERE
    FINDING_CNT_note_text > 0 ),

  noteDistribution AS (
  SELECT AS STRUCT FINDING_CNT_note_text as phi_cnt, COUNT(1) as note_cnt from \`${FROM}\`
  group by FINDING_CNT_note_text
  ),

  flattenFindingsByType AS (
      SELECT
      type.*
      FROM findingsPerText f
      CROSS JOIN UNNEST(f.type) as type
  ),

  flattenFindingsByMethod AS (
      SELECT
      foundBy.*
      FROM findingsPerText f
      CROSS JOIN UNNEST(f.foundBy) as foundBy
  )

SELECT * FROM (
  SELECT '${FROM}' AS datasource,
  CURRENT_DATE() as reportDate,
  ( SELECT SUM(uniqueFindingCount)
    FROM findingsPerText
  ) AS totalUniqueFindingCount,
  ARRAY(
   SELECT AS STRUCT * FROM noteDistribution
  ) AS noteDistribution,
  ARRAY(SELECT AS STRUCT t AS phi_type, SUM(cnt) AS count_per_type FROM flattenFindingsByType group by t) as type,
  ARRAY(SELECT AS STRUCT t AS phi_type, SUM(cnt) AS count_per_type FROM flattenFindingsByMethod group by t) as foundBy
)

BND_OF_QUERY

QUERY=$(echo "$_QUERY" | sed 's/\n/ /g')
echo "$QUERY"

set -x
bq --location=US query --use_cache=false --append_table --destination_table ${TO}  --use_legacy_sql=false "$QUERY"

set +x


