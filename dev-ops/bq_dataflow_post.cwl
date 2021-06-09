# This cwl file is a temporary place to keep partial cwl configurations for deid all text tables.
# for observation table, we need to extract text from json value in a field, and put it back to the correct place after deid.

# observation:
# id: observation_id,STUDY_ID,value_as_string,isjson
# text: value_as_string_extract
query:
        default: >-

          CREATE TEMP FUNCTION
          replaceJsonValue(input STRING, repl STRING)
          RETURNS STRING
          LANGUAGE js AS """

          var inputStruct = JSON.parse(input);
          for (var key in inputStruct.values) {
            var value = inputStruct.values[key];
            var source = value.source;
            if (source == 'ip_flwsht_meas.meas_value') {
              inputStruct.values[key].value = repl
              break;
            }
          }

          return JSON.stringify(inputStruct);
          """;


          SELECT
          CASE deid.isjson
                      WHEN TRUE  THEN replaceJsonValue(value_as_string, TEXT_DEID_value_as_string_extract)
                      ELSE value_as_string
                      END as value_as_string_deid


          FROM `[TEMP_PROJECT_ID].[TEMP_DATASET].deid_observation_text` deid
          where FINDING_CNT_value_as_string_extract > 0

        valueFrom: >-
          ${
            var query = inputs.query;
            query = query.replace(new RegExp('\\[FILTERED_CDM_PROJECT_ID\\]', 'g'), inputs.filtered_cdm_project_id);
            query = query.replace(new RegExp('\\[FILTERED_CDM_DATASET\\]',    'g'), inputs.filtered_cdm_dataset);
            query = query.replace(new RegExp('\\[TEMP_PROJECT_ID\\]',         'g'), inputs.temp_project_id);
            query = query.replace(new RegExp('\\[TEMP_DATASET\\]',            'g'), inputs.temp_working_dataset);
            return query;
          }
      destination_table:
        valueFrom: >-
          ${
            return JSON.stringify({
              "projectId": inputs.temp_project_id,
              "datasetId": inputs.temp_working_dataset,
              "tableId"  : "cdm_observation_deid"
            });
          }
      replace:
        default: true
      dry_run: dry_run


# LEFT JOIN BACK INTO OBSERVATION TABLE
  CREATE TEMP FUNCTION
  replaceJsonValue(input STRING, repl STRING)
  RETURNS STRING
  LANGUAGE js AS """

  var inputStruct = JSON.parse(input);
  for (var key in inputStruct.values) {
    var value = inputStruct.values[key];
    var source = value.source;
    if (source == 'ip_flwsht_meas.meas_value') {
      inputStruct.values[key].value = repl
      break;
    }
  }

  return JSON.stringify(inputStruct);
""";

SELECT observation.* EXCEPT (value_as_string),
IF((deid.value_as_string is null), observation.value_as_string,(
CASE deid.isjson
            WHEN TRUE  THEN replaceJsonValue(deid.value_as_string, TEXT_DEID_value_as_string_extract)
            ELSE deid.value_as_string
            END
)) as value_as_string

FROM `[TEMP_PROJECT_ID].[TEMP_DATASET].deid_observation_text` deid
LEFT JOIN `[TEMP_PROJECT_ID].[TEMP_DATASET].observation` observation
ON observation.observation_id = deid.observation_id


# LEFT JOIN BACK INTO MEASUREMENT TABLE

SELECT measurement.* EXCEPT (value_source_value,measurement_source_value),
IF((deid.value_source_value is null), measurement.value_source_value,deid.value_source_value) as value_source_value,
IF((deid.measurement_source_value is null), measurement.measurement_source_value,deid.measurement_source_value) as measurement_source_value

FROM `[TEMP_PROJECT_ID].[TEMP_DATASET].deid_measurement_text` deid
LEFT JOIN `[TEMP_PROJECT_ID].[TEMP_DATASET].measurement` measurement
ON observation.measurement_id = deid.measurement_id
