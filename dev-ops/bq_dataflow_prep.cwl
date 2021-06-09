# This cwl file is a temporary place to keep partial cwl configurations for deid all text tables.
# since the temp table phi is used by all these tables, we should separate it out of this step so we can reuse it in the pipeline.

# note
query:
        default: >-
          WITH
            phi AS (
            SELECT
              *
            FROM
              `[TEMP_PROJECT_ID].[TEMP_DATASET].shc_phi_codebook_merged`
            UNION ALL
            SELECT
              *
            FROM
              `[TEMP_PROJECT_ID].[TEMP_DATASET].lpch_phi_codebook_merged`
            WHERE
              ID NOT IN (
              SELECT
                ID
              FROM
                `[TEMP_PROJECT_ID].[TEMP_DATASET].shc_phi_codebook_merged`) )
          SELECT
            note.note_id,
            note.note_text,
            emp.provider_name AS EMP_NAME,
            phi.*
          FROM
            `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].note` note
          JOIN
            `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].person` person
          ON
            person.person_id = note.person_id
          LEFT JOIN
            `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].provider` emp
          ON
            note.provider_id IS NOT NULL
            AND emp.provider_id = note.provider_id
          LEFT JOIN
            phi
          ON
            person.person_source_value = phi.ID
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
              "tableId"  : "cdm_note_with_phi"
            });
          }
      replace:
        default: true
      dry_run: dry_run




# measurement:
# id: measurement_id
# text: value_source_value, measurement_source_value
query:
        default: >-
          WITH
            phi AS (
            SELECT
              *
            FROM
              `[TEMP_PROJECT_ID].[TEMP_DATASET].shc_phi_codebook_merged`
            UNION ALL
            SELECT
              *
            FROM
              `[TEMP_PROJECT_ID].[TEMP_DATASET].lpch_phi_codebook_merged`
            WHERE
              ID NOT IN (
              SELECT
                ID
              FROM
                `[TEMP_PROJECT_ID].[TEMP_DATASET].shc_phi_codebook_merged`) )
          SELECT
            measurement.measurement_id,
            measurement.value_source_value,
            measurement.measurement_source_value,
            emp.provider_name AS EMP_NAME,
            phi.*
          FROM
            `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].measurement` measurement
          JOIN
            `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].person` person
          ON
            person.person_id = measurement.person_id
          LEFT JOIN
            `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].provider` emp
          ON
            measurement.provider_id IS NOT NULL
            AND emp.provider_id = measurement.provider_id
          LEFT JOIN
            phi
          ON
            person.person_source_value = phi.ID

          WHERE

          (measurement.value_source_value is not null AND length(measurement.value_source_value) > 2 and SAFE_CAST(REGEXP_REPLACE (measurement.value_source_value, "\\||\\/|\\\\|-|'|\"|<|>", "") AS FLOAT64) is null
          AND measurement.value_source_value not in ('','Negative','N/A','arterial/capillary','SEE NOTE','Not Detected','SEE TEXT','Yellow','Clear',
          'Clinical Laboratory Medical Director','Rare','Detailed information on file in HIS.','Abnormal','Positive','Automated urine microscopic exam performed.',
          'Comment','none','NEG','Few','NEGATIVE','neg','O Positive','Compatible','No significant amount of bacteria detected.','No significant abnormalities except as indicated by the indices.',
          'CLEAN CATCH','Trace','No growth at 5 days.','Normal','See manual differential','Hazy','Transfused','Colorless','BLOOD','A Positive','O Pos',
          'Moderate','Differential not reported when the WBC is < 0.6 K/uL.','Red Blood Cells','Random','Unknown','RBC product order received.',
          'NONE SEEN','Done','Plasma','See slide diff','Many','Trough','No significant abnormalities.','ARTERIAL','Two swabs in culturette received','Completed','negative','NOT APPLICABLE','None Detected'
          )
          )

          OR

          (measurement.measurement_source_value is not null AND length(measurement.measurement_source_value) > 2 and SAFE_CAST(REGEXP_REPLACE (measurement.measurement_source_value, "\\||\\/|\\\\|-|'|\"|<|>", "") AS FLOAT64) is null
          AND measurement.measurement_source_value not in (
          'weight','bp_systolic','bp_diastolic','pulse','bmi','GLUCOSE BY METER','temperature','height','SOURCE','POCT COMMENT','"METABOLIC PANEL, COMPREHENSIVE"',
          'POC:GLUCOSE BY METER','CBC WITH DIFF','CBC WITH DIFFERENTIAL','QTC INTERVAL','QRSD INTERVAL','RR','QT INTERVAL','"METABOLIC PANEL, BASIC"','SPECIMEN','P-R INTERVAL'
          )
          )


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
              "tableId"  : "cdm_measurement_with_phi"
            });
          }
      replace:
        default: true
      dry_run: dry_run




# observation:
# id: observation_id,STUDY_ID,value_as_string,isjson
# text: value_as_string_extract
query:
        default: >-
          CREATE TEMP FUNCTION
          extractMeasValue(input STRING)
          RETURNS STRING
          LANGUAGE js AS """

          var inputArray = null;
          try{
            inputArray = JSON.parse(input).values;
          }catch(e){
            return input;
          }

          for (var key in inputArray) {
            var value = inputArray[key];
            var source = value.source;
            if (source == 'ip_flwsht_meas.meas_value') {
              return value.value;
            }
          }

          return input;
          """;


          WITH
              phi AS (
              SELECT
                *
              FROM
                `[TEMP_PROJECT_ID].[TEMP_DATASET].shc_phi_codebook_merged`
              UNION ALL
              SELECT
                *
              FROM
                `[TEMP_PROJECT_ID].[TEMP_DATASET].lpch_phi_codebook_merged`
              WHERE
                ID NOT IN (
                SELECT
                  ID
                FROM
                  `[TEMP_PROJECT_ID].[TEMP_DATASET].shc_phi_codebook_merged`) ),
            exractedObservation AS (
              SELECT
              observation.observation_id,
              observation.value_as_string,
              extractMeasValue(observation.value_as_string) as value_as_string_extract,
              STARTS_WITH(observation.value_as_string, "{\"values\":[{") as isjson,
              emp.provider_name AS EMP_NAME,
              phi.*
            FROM
              `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].observation` observation
            JOIN
              `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].person` person
            ON
              person.person_id = observation.person_id
            LEFT JOIN
              `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].provider` emp
            ON
              observation.provider_id IS NOT NULL
              AND emp.provider_id = observation.provider_id
            LEFT JOIN
              phi
            ON
              person.person_source_value = phi.ID
            )

            select * from exractedObservation
            where length(value_as_string_extract) > 2 and SAFE_CAST(
            REGEXP_REPLACE (value_as_string_extract, "\\||\\/|\\\\|-|'|\"|<|>", "") AS FLOAT64) is null
            AND  value_as_string_extract not in (
            'Clean, Dry & Intact','Verbal 0-10','IPPV','Automatic/Non-Invasive BP','Regular','Never','Never Smoker','Active','Sitting','Diminished','WNL','Never Used','Other (Comment)','Warm','Left;Upper Extremity',
            'Dry ;Intact','+2;Palpable','Strong','Unknown','Siderails up x2','MAN/SPONT','Right;Upper Extremity','Independent','PCV','Pillow','30 Degrees','Medication','Oriented to person, place, time, circumstance',
            'Infusing','Transparent','Dry;Intact','Moderate','Knee High SCD','Non Labored','Warm;Dry','Patent','Automatic','Bed','Required','Abdomen','SIMV','Exceptions','2-3 seconds','Unable to Assess','Non Verbal',
            'Reinforced','Normal','Room air','Yes;Reinforced','Siderails up x3','Dependent','Clean;Dry;Intact','Appropriate for Race','Turns by self','Moderate Assistance','FROM','Quit','Former Smoker','1 Person Assist',
            'Supine','<2 seconds','PSV','UTA','Flushed with Saline','Right arm','Weak','Coarse','Mild','Steady','3 mm','Standard Fall Interventions (see details)','Aching','FLACC','Set-up only','Mod','Minimal Assistance',
            'Bedrest','Paged','Pale','No complications','Assist','No Intervention Required','Head','Back','Clear;Diminished','Not Removed','Pupil Constriction','+1;Palpable','Standing Scale','Symmetrical','Left arm','No Dressing',
            'Warm;Dry;Intact','Unsteady','VCP500','Left Arm','Anterior only','Maximal Assistance','Gauze',
            'CONTINUED','Brown','White','< 3 seconds','Limited Range of Motion','2 People Assist','Covered','STANDBY','Home','Sleeping',
            'Negative','Applied','Yes','None','Natural','Done','Absent','Present','Good','Passed',
            'High','Medium','Intact',
            'Male','Female','Fair','Family','Outpatient',
            'Pink','Yellow','Yellow/Straw','Red',
            'Right','Left','Lab','FWB','Leg','Mid','Low','Thick',
            'N/A','NPO','N/C','NSR','WDL','Reg','Clear','Brisk','Adult','Unit','Thin','Soft','Peds','Lying','Lower','Round','Trace','Voids')

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
              "tableId"  : "cdm_observation_with_phi"
            });
          }
      replace:
        default: true
      dry_run: dry_run






# condition_occurrence:
# id: condition_occurrence_id
# text: stop_reason
query:
        default: >-
          WITH
            phi AS (
            SELECT
              *
            FROM
              `[TEMP_PROJECT_ID].[TEMP_DATASET].shc_phi_codebook_merged`
            UNION ALL
            SELECT
              *
            FROM
              `[TEMP_PROJECT_ID].[TEMP_DATASET].lpch_phi_codebook_merged`
            WHERE
              ID NOT IN (
              SELECT
                ID
              FROM
                `[TEMP_PROJECT_ID].[TEMP_DATASET].shc_phi_codebook_merged`) )
          SELECT
            condition_occurrence.condition_occurrence_id,
            condition_occurrence.stop_reason,
            emp.provider_name AS EMP_NAME,
            phi.*
          FROM
            `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].condition_occurrence` condition_occurrence
          JOIN
            `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].person` person
          ON
            person.person_id = condition_occurrence.person_id
          LEFT JOIN
            `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].provider` emp
          ON
            condition_occurrence.provider_id IS NOT NULL
            AND emp.provider_id = condition_occurrence.provider_id
          LEFT JOIN
            phi
          ON
            person.person_source_value = phi.ID
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
              "tableId"  : "cdm_condition_occurrence_with_phi"
            });
          }
      replace:
        default: true
      dry_run: dry_run



# drug_exposure:
# id: drug_exposure_id
# text: stop_reason,sig,route_source_value,dose_unit_source_value,
query:
        default: >-
          WITH
            phi AS (
            SELECT
              *
            FROM
              `[TEMP_PROJECT_ID].[TEMP_DATASET].shc_phi_codebook_merged`
            UNION ALL
            SELECT
              *
            FROM
              `[TEMP_PROJECT_ID].[TEMP_DATASET].lpch_phi_codebook_merged`
            WHERE
              ID NOT IN (
              SELECT
                ID
              FROM
                `[TEMP_PROJECT_ID].[TEMP_DATASET].shc_phi_codebook_merged`) )
          SELECT
            drug_exposure.drug_exposure_id,
            drug_exposure.stop_reason,
            drug_exposure.sig,
            drug_exposure.route_source_value,
            drug_exposure.dose_unit_source_value,
            emp.provider_name AS EMP_NAME,
            phi.*
          FROM
            `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].drug_exposure` drug_exposure
          JOIN
            `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].person` person
          ON
            person.person_id = drug_exposure.person_id
          LEFT JOIN
            `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].provider` emp
          ON
            drug_exposure.provider_id IS NOT NULL
            AND emp.provider_id = drug_exposure.provider_id
          LEFT JOIN
            phi
          ON
            person.person_source_value = phi.ID
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
              "tableId"  : "cdm_drug_exposure_with_phi"
            });
          }
      replace:
        default: true
      dry_run: dry_run




# specimen:
# id: specimen_id
# text: specimen_source_id,specimen_source_value,unit_source_value,anatomic_site_source_value,disease_status_source_value,
query:
        default: >-
          WITH
            phi AS (
            SELECT
              *
            FROM
              `[TEMP_PROJECT_ID].[TEMP_DATASET].shc_phi_codebook_merged`
            UNION ALL
            SELECT
              *
            FROM
              `[TEMP_PROJECT_ID].[TEMP_DATASET].lpch_phi_codebook_merged`
            WHERE
              ID NOT IN (
              SELECT
                ID
              FROM
                `[TEMP_PROJECT_ID].[TEMP_DATASET].shc_phi_codebook_merged`) )
          SELECT
            specimen.specimen_id,
            specimen.specimen_source_id,
            specimen.specimen_source_value,
            specimen.unit_source_value,
            specimen.anatomic_site_source_value,
            specimen.disease_status_source_value,
            phi.*
          FROM
            `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].specimen` specimen
          JOIN
            `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].person` person
          ON
            person.person_id = specimen.person_id

          LEFT JOIN
            phi
          ON
            person.person_source_value = phi.ID
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
              "tableId"  : "cdm_specimen_with_phi"
            });
          }
      replace:
        default: true
      dry_run: dry_run



# payer_plan_period:
# id: payer_plan_period_id
# text: stop_reason_source_value
query:
        default: >-
          WITH
            phi AS (
            SELECT
              *
            FROM
              `[TEMP_PROJECT_ID].[TEMP_DATASET].shc_phi_codebook_merged`
            UNION ALL
            SELECT
              *
            FROM
              `[TEMP_PROJECT_ID].[TEMP_DATASET].lpch_phi_codebook_merged`
            WHERE
              ID NOT IN (
              SELECT
                ID
              FROM
                `[TEMP_PROJECT_ID].[TEMP_DATASET].shc_phi_codebook_merged`) )
          SELECT
            payer_plan_period.payer_plan_period_id,
            payer_plan_period.stop_reason_source_value,
            phi.*
          FROM
            `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].payer_plan_period` payer_plan_period
          JOIN
            `[FILTERED_CDM_PROJECT_ID].[FILTERED_CDM_DATASET].person` person
          ON
            person.person_id = payer_plan_period.person_id

          LEFT JOIN
            phi
          ON
            person.person_source_value = phi.ID
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
              "tableId"  : "cdm_payer_plan_period_with_phi"
            });
          }
      replace:
        default: true
      dry_run: dry_run

