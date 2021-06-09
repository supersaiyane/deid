FULL_INPUT="$1"
FILTERED_INPUT="$2"
DEIDED="$3"
TO="$4"


echo "DEIDED:$DEIDED TO:$TO"

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
  findingsPerText_measurement AS (
  SELECT AS STRUCT
    countFindingByType (deid.FINDING_measurement_source_value) AS type,
    countFindingByMethod (deid.FINDING_measurement_source_value) AS foundBy,
    countUniqueFinding (deid.FINDING_measurement_source_value) AS uniqueFindingCount
  FROM

    \`${DEIDED}\` deid
  WHERE
    FINDING_CNT_measurement_source_value > 0 ),

  textDistribution_measurement AS (
  SELECT AS STRUCT FINDING_CNT_measurement_source_value as phi_cnt, COUNT(1) as note_cnt from \`${DEIDED}\`
  group by FINDING_CNT_measurement_source_value
  order by phi_cnt
  ),

  flattenFindingsByType_measurement AS (
      SELECT
      type.*
      FROM findingsPerText_measurement f
      CROSS JOIN UNNEST(f.type) as type
  ),

  flattenFindingsByMethod_measurement AS (
      SELECT
      foundBy.*
      FROM findingsPerText_measurement f
      CROSS JOIN UNNEST(f.foundBy) as foundBy
  ),



  findingsPerText_value AS (
  SELECT AS STRUCT
    countFindingByType (deid.FINDING_value_source_value) AS type,
    countFindingByMethod (deid.FINDING_value_source_value) AS foundBy,
    countUniqueFinding (deid.FINDING_value_source_value) AS uniqueFindingCount
  FROM

    \`${DEIDED}\` deid
  WHERE
    FINDING_CNT_value_source_value > 0 ),

  textDistribution_value AS (
  SELECT AS STRUCT FINDING_CNT_value_source_value as phi_cnt, COUNT(1) as note_cnt from \`${DEIDED}\`
  group by FINDING_CNT_value_source_value
  order by phi_cnt
  ),

  flattenFindingsByType_value AS (
      SELECT
      type.*
      FROM findingsPerText_value f
      CROSS JOIN UNNEST(f.type) as type
  ),

  flattenFindingsByMethod_value AS (
      SELECT
      foundBy.*
      FROM findingsPerText_value f
      CROSS JOIN UNNEST(f.foundBy) as foundBy
  )



SELECT * FROM (
  SELECT '${FILTERED_INPUT}.measurement_source_value' AS datasource,
  'measurement.measurement_source_value' AS dataField,
  CURRENT_DATE() as reportDate,
  (select count(1) from \`${FULL_INPUT}\`) as totalTextDataRowCount,
  (select count(1) from \`${FILTERED_INPUT}\` ) as totalProcessedTextDataRowCount,
  (select count(1) from \`${FILTERED_INPUT}\` where measurement_source_value is not null) as totalProcessedNonNullTextDataRowCount,
  (select count(1) from \`${DEIDED}\`) as totalDeidedRowCount,

  ( SELECT SUM(uniqueFindingCount)
    FROM findingsPerText_measurement
  ) AS totalUniqueFindingCount,
  ARRAY(
   SELECT AS STRUCT * FROM textDistribution_measurement
  ) AS textDistribution,
  ARRAY(SELECT AS STRUCT t AS phi_type, SUM(cnt) AS count_per_type FROM flattenFindingsByType_measurement group by t order by count_per_type desc) as type,
  ARRAY(SELECT AS STRUCT t AS phi_type, SUM(cnt) AS count_per_type FROM flattenFindingsByMethod_measurement group by t order by count_per_type desc) as foundBy
)

UNION ALL

SELECT * FROM (
  SELECT '${FILTERED_INPUT}.value_source_value' AS datasource,
  'measurement.value_source_value' AS dataField,
  CURRENT_DATE() as reportDate,
  (select count(1) from \`${FULL_INPUT}\`) as totalTextDataRowCount,
  (select count(1) from \`${FILTERED_INPUT}\`) as totalProcessedTextDataRowCount,
  (select count(1) from \`${FILTERED_INPUT}\` where value_source_value is not null) as totalProcessedNonNullTextDataRowCount,
  (select count(1) from \`${DEIDED}\`) as totalDeidedRowCount,

  ( SELECT SUM(uniqueFindingCount)
    FROM findingsPerText_value
  ) AS totalUniqueFindingCount,
  ARRAY(
   SELECT AS STRUCT * FROM textDistribution_value
  ) AS textDistribution,
  ARRAY(SELECT AS STRUCT t AS phi_type, SUM(cnt) AS count_per_type FROM flattenFindingsByType_value group by t order by count_per_type desc) as type,
  ARRAY(SELECT AS STRUCT t AS phi_type, SUM(cnt) AS count_per_type FROM flattenFindingsByMethod_value group by t order by count_per_type desc) as foundBy
)


BND_OF_QUERY

QUERY=$(echo "$_QUERY" | sed 's/\n/ /g')
echo "$QUERY"

set -x
bq --location=US query --use_cache=false --append_table --destination_table ${TO}  --use_legacy_sql=false "$QUERY"

set +x


