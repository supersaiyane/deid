# This cwl file is a temporary place to keep partial cwl configurations for running deid jobs for all text tables.


# note
  deid_workflow:
    run: ../deid-text/wf-deid-text.cwl
    in:
      depends_on_multiple_job_ids:
        - install_deid_dataflow_template/job_done
        - bq_temp_table_cdm_note_with_phi/gcp_bq_job_id
      bq_gcp_credentials_key_file: gcp_credentials_key_file
      df_gcp_credentials_key_file: gcp_credentials_key_file
      extract_source_table:
        source:
          - deid_cdm_project_id
          - generate_temp_name/generated_name
        valueFrom: >-
          ${
            return JSON.stringify({
              "projectId": self[0],
              "datasetId": self[1],
              "tableId"  : "cdm_note_with_phi"
            });
          }
      extract_target_bucket         : create_gcs_temp_working_urls/gcs_temp_working_df_extract_target_bucket
      dataflow_project              : deid_cdm_project_id
      dataflow_run_regional_endpoint: dataflow_run_regional_endpoint
      param_dataflow_run_env_service_account_email: dataflow_run_env_service_account_email
      param_temp_location                         : create_gcs_temp_working_urls/gcs_temp_working_df_temp_url
      param_dataflow_run_env_zone                 : dataflow_run_env_zone
      param_dataflow_run_env_max_workers          : dataflow_run_env_max_workers
      dataflow_run_launch_template_parameters:
        valueFrom: >-
          ${
            return JSON.stringify({
                "jobName": "deid",
                "environment": {
                  "serviceAccountEmail": inputs.param_dataflow_run_env_service_account_email,
                  "tempLocation"       : inputs.param_temp_location,
                  "zone"               : inputs.param_dataflow_run_env_zone,
                  "maxWorkers"         : inputs.param_dataflow_run_env_max_workers
                },
                "parameters": {
                  "inputResource"  : inputs.extract_target_bucket,
                  "textInputFields": "note_text",
                  "textIdFields"   : "note_id,STUDY_ID",
                  "inputType"      : "gcp_gcs",
                  "outputResource" : inputs.deid_output_resource
                }
              }
            );
          }
      dataflow_template_location_dir: create_gcs_temp_working_urls/gcs_temp_working_df_template_url
      deid_output_resource          : create_gcs_temp_working_urls/gcs_temp_working_df_deid_output_resource
      load_target_table:
        source:
          - deid_cdm_project_id
          - generate_temp_name/generated_name
        valueFrom: >-
          ${
            return JSON.stringify({
              "projectId": self[0],
              "datasetId": self[1],
              "tableId"  : "deid_note_text"
            });
          }
    out:
      - extract_job_id
      - deid_job_id
      - load_job_id




# measurement:
# id: measurement_id
# text: value_source_value
  deid_workflow:
    run: ../deid-text/wf-deid-text.cwl
    in:
      depends_on_multiple_job_ids:
        - install_deid_dataflow_template/job_done
        - bq_temp_table_cdm_measurement_with_phi/gcp_bq_job_id
      bq_gcp_credentials_key_file: gcp_credentials_key_file
      df_gcp_credentials_key_file: gcp_credentials_key_file
      extract_source_table:
        source:
          - deid_cdm_project_id
          - generate_temp_name/generated_name
        valueFrom: >-
          ${
            return JSON.stringify({
              "projectId": self[0],
              "datasetId": self[1],
              "tableId"  : "cdm_measurement_with_phi"
            });
          }
      extract_target_bucket         : create_gcs_temp_working_urls/gcs_temp_working_df_extract_target_bucket
      dataflow_project              : deid_cdm_project_id
      dataflow_run_regional_endpoint: dataflow_run_regional_endpoint
      param_dataflow_run_env_service_account_email: dataflow_run_env_service_account_email
      param_temp_location                         : create_gcs_temp_working_urls/gcs_temp_working_df_temp_url
      param_dataflow_run_env_zone                 : dataflow_run_env_zone
      param_dataflow_run_env_max_workers          : dataflow_run_env_max_workers
      dataflow_run_launch_template_parameters:
        valueFrom: >-
          ${
            return JSON.stringify({
                "jobName": "deid",
                "environment": {
                  "serviceAccountEmail": inputs.param_dataflow_run_env_service_account_email,
                  "tempLocation"       : inputs.param_temp_location,
                  "zone"               : inputs.param_dataflow_run_env_zone,
                  "maxWorkers"         : inputs.param_dataflow_run_env_max_workers
                },
                "parameters": {
                  "inputResource"  : inputs.extract_target_bucket,
                  "textInputFields": "value_source_value,measurement_source_value",
                  "textIdFields"   : "measurement_id,STUDY_ID",
                  "inputType"      : "gcp_gcs",
                  "outputResource" : inputs.deid_output_resource
                }
              }
            );
          }
      dataflow_template_location_dir: create_gcs_temp_working_urls/gcs_temp_working_df_template_url
      deid_output_resource          : create_gcs_temp_working_urls/gcs_temp_working_df_deid_output_resource
      load_target_table:
        source:
          - deid_cdm_project_id
          - generate_temp_name/generated_name
        valueFrom: >-
          ${
            return JSON.stringify({
              "projectId": self[0],
              "datasetId": self[1],
              "tableId"  : "deid_measurement_text"
            });
          }
    out:
      - extract_job_id
      - deid_job_id
      - load_job_id



# observation:
# id: observation_id
# text: value_as_string,qualifier_source_value
  deid_workflow:
    run: ../deid-text/wf-deid-text.cwl
    in:
      depends_on_multiple_job_ids:
        - install_deid_dataflow_template/job_done
        - bq_temp_table_cdm_observation_with_phi/gcp_bq_job_id
      bq_gcp_credentials_key_file: gcp_credentials_key_file
      df_gcp_credentials_key_file: gcp_credentials_key_file
      extract_source_table:
        source:
          - deid_cdm_project_id
          - generate_temp_name/generated_name
        valueFrom: >-
          ${
            return JSON.stringify({
              "projectId": self[0],
              "datasetId": self[1],
              "tableId"  : "cdm_observation_with_phi"
            });
          }
      extract_target_bucket         : create_gcs_temp_working_urls/gcs_temp_working_df_extract_target_bucket
      dataflow_project              : deid_cdm_project_id
      dataflow_run_regional_endpoint: dataflow_run_regional_endpoint
      param_dataflow_run_env_service_account_email: dataflow_run_env_service_account_email
      param_temp_location                         : create_gcs_temp_working_urls/gcs_temp_working_df_temp_url
      param_dataflow_run_env_zone                 : dataflow_run_env_zone
      param_dataflow_run_env_max_workers          : dataflow_run_env_max_workers
      dataflow_run_launch_template_parameters:
        valueFrom: >-
          ${
            return JSON.stringify({
                "jobName": "deid",
                "environment": {
                  "serviceAccountEmail": inputs.param_dataflow_run_env_service_account_email,
                  "tempLocation"       : inputs.param_temp_location,
                  "zone"               : inputs.param_dataflow_run_env_zone,
                  "maxWorkers"         : inputs.param_dataflow_run_env_max_workers
                },
                "parameters": {
                  "inputResource"  : inputs.extract_target_bucket,
                  "textInputFields": "value_as_string_extract",
                  "textIdFields"   : "observation_id,STUDY_ID,value_as_string,isjson",
                  "inputType"      : "gcp_gcs",
                  "outputResource" : inputs.deid_output_resource
                }
              }
            );
          }
      dataflow_template_location_dir: create_gcs_temp_working_urls/gcs_temp_working_df_template_url
      deid_output_resource          : create_gcs_temp_working_urls/gcs_temp_working_df_deid_output_resource
      load_target_table:
        source:
          - deid_cdm_project_id
          - generate_temp_name/generated_name
        valueFrom: >-
          ${
            return JSON.stringify({
              "projectId": self[0],
              "datasetId": self[1],
              "tableId"  : "deid_observation_text"
            });
          }
    out:
      - extract_job_id
      - deid_job_id
      - load_job_id



# condition_occurrence:
# id: condition_occurrence_id
# text: stop_reason
  deid_workflow:
    run: ../deid-text/wf-deid-text.cwl
    in:
      depends_on_multiple_job_ids:
        - install_deid_dataflow_template/job_done
        - bq_temp_table_cdm_condition_occurrence_with_phi/gcp_bq_job_id
      bq_gcp_credentials_key_file: gcp_credentials_key_file
      df_gcp_credentials_key_file: gcp_credentials_key_file
      extract_source_table:
        source:
          - deid_cdm_project_id
          - generate_temp_name/generated_name
        valueFrom: >-
          ${
            return JSON.stringify({
              "projectId": self[0],
              "datasetId": self[1],
              "tableId"  : "cdm_condition_occurrence_with_phi"
            });
          }
      extract_target_bucket         : create_gcs_temp_working_urls/gcs_temp_working_df_extract_target_bucket
      dataflow_project              : deid_cdm_project_id
      dataflow_run_regional_endpoint: dataflow_run_regional_endpoint
      param_dataflow_run_env_service_account_email: dataflow_run_env_service_account_email
      param_temp_location                         : create_gcs_temp_working_urls/gcs_temp_working_df_temp_url
      param_dataflow_run_env_zone                 : dataflow_run_env_zone
      param_dataflow_run_env_max_workers          : dataflow_run_env_max_workers
      dataflow_run_launch_template_parameters:
        valueFrom: >-
          ${
            return JSON.stringify({
                "jobName": "deid",
                "environment": {
                  "serviceAccountEmail": inputs.param_dataflow_run_env_service_account_email,
                  "tempLocation"       : inputs.param_temp_location,
                  "zone"               : inputs.param_dataflow_run_env_zone,
                  "maxWorkers"         : inputs.param_dataflow_run_env_max_workers
                },
                "parameters": {
                  "inputResource"  : inputs.extract_target_bucket,
                  "textInputFields": "stop_reason",
                  "textIdFields"   : "condition_occurrence_id,STUDY_ID",
                  "inputType"      : "gcp_gcs",
                  "outputResource" : inputs.deid_output_resource
                }
              }
            );
          }
      dataflow_template_location_dir: create_gcs_temp_working_urls/gcs_temp_working_df_template_url
      deid_output_resource          : create_gcs_temp_working_urls/gcs_temp_working_df_deid_output_resource
      load_target_table:
        source:
          - deid_cdm_project_id
          - generate_temp_name/generated_name
        valueFrom: >-
          ${
            return JSON.stringify({
              "projectId": self[0],
              "datasetId": self[1],
              "tableId"  : "deid_condition_occurrence_text"
            });
          }
    out:
      - extract_job_id
      - deid_job_id
      - load_job_id




# drug_exposure:
# id: drug_exposure_id
# text: stop_reason,sig,route_source_value,dose_unit_source_value,
  deid_workflow:
    run: ../deid-text/wf-deid-text.cwl
    in:
      depends_on_multiple_job_ids:
        - install_deid_dataflow_template/job_done
        - bq_temp_table_cdm_drug_exposure_with_phi/gcp_bq_job_id
      bq_gcp_credentials_key_file: gcp_credentials_key_file
      df_gcp_credentials_key_file: gcp_credentials_key_file
      extract_source_table:
        source:
          - deid_cdm_project_id
          - generate_temp_name/generated_name
        valueFrom: >-
          ${
            return JSON.stringify({
              "projectId": self[0],
              "datasetId": self[1],
              "tableId"  : "cdm_drug_exposure_with_phi"
            });
          }
      extract_target_bucket         : create_gcs_temp_working_urls/gcs_temp_working_df_extract_target_bucket
      dataflow_project              : deid_cdm_project_id
      dataflow_run_regional_endpoint: dataflow_run_regional_endpoint
      param_dataflow_run_env_service_account_email: dataflow_run_env_service_account_email
      param_temp_location                         : create_gcs_temp_working_urls/gcs_temp_working_df_temp_url
      param_dataflow_run_env_zone                 : dataflow_run_env_zone
      param_dataflow_run_env_max_workers          : dataflow_run_env_max_workers
      dataflow_run_launch_template_parameters:
        valueFrom: >-
          ${
            return JSON.stringify({
                "jobName": "deid",
                "environment": {
                  "serviceAccountEmail": inputs.param_dataflow_run_env_service_account_email,
                  "tempLocation"       : inputs.param_temp_location,
                  "zone"               : inputs.param_dataflow_run_env_zone,
                  "maxWorkers"         : inputs.param_dataflow_run_env_max_workers
                },
                "parameters": {
                  "inputResource"  : inputs.extract_target_bucket,
                  "textInputFields": "stop_reason,sig,route_source_value,dose_unit_source_value",
                  "textIdFields"   : "drug_exposure_id,STUDY_ID",
                  "inputType"      : "gcp_gcs",
                  "outputResource" : inputs.deid_output_resource
                }
              }
            );
          }
      dataflow_template_location_dir: create_gcs_temp_working_urls/gcs_temp_working_df_template_url
      deid_output_resource          : create_gcs_temp_working_urls/gcs_temp_working_df_deid_output_resource
      load_target_table:
        source:
          - deid_cdm_project_id
          - generate_temp_name/generated_name
        valueFrom: >-
          ${
            return JSON.stringify({
              "projectId": self[0],
              "datasetId": self[1],
              "tableId"  : "deid_drug_exposure_text"
            });
          }
    out:
      - extract_job_id
      - deid_job_id
      - load_job_id



# specimen:
# id: specimen_id
# text: specimen_source_id,specimen_source_value,unit_source_value,anatomic_site_source_value,disease_status_source_value,
  deid_workflow:
    run: ../deid-text/wf-deid-text.cwl
    in:
      depends_on_multiple_job_ids:
        - install_deid_dataflow_template/job_done
        - bq_temp_table_cdm_specimen_with_phi/gcp_bq_job_id
      bq_gcp_credentials_key_file: gcp_credentials_key_file
      df_gcp_credentials_key_file: gcp_credentials_key_file
      extract_source_table:
        source:
          - deid_cdm_project_id
          - generate_temp_name/generated_name
        valueFrom: >-
          ${
            return JSON.stringify({
              "projectId": self[0],
              "datasetId": self[1],
              "tableId"  : "cdm_specimen_with_phi"
            });
          }
      extract_target_bucket         : create_gcs_temp_working_urls/gcs_temp_working_df_extract_target_bucket
      dataflow_project              : deid_cdm_project_id
      dataflow_run_regional_endpoint: dataflow_run_regional_endpoint
      param_dataflow_run_env_service_account_email: dataflow_run_env_service_account_email
      param_temp_location                         : create_gcs_temp_working_urls/gcs_temp_working_df_temp_url
      param_dataflow_run_env_zone                 : dataflow_run_env_zone
      param_dataflow_run_env_max_workers          : dataflow_run_env_max_workers
      dataflow_run_launch_template_parameters:
        valueFrom: >-
          ${
            return JSON.stringify({
                "jobName": "deid",
                "environment": {
                  "serviceAccountEmail": inputs.param_dataflow_run_env_service_account_email,
                  "tempLocation"       : inputs.param_temp_location,
                  "zone"               : inputs.param_dataflow_run_env_zone,
                  "maxWorkers"         : inputs.param_dataflow_run_env_max_workers
                },
                "parameters": {
                  "inputResource"  : inputs.extract_target_bucket,
                  "textInputFields": "specimen_source_id,specimen_source_value,unit_source_value,anatomic_site_source_value,disease_status_source_value",
                  "textIdFields"   : "specimen_id,STUDY_ID",
                  "inputType"      : "gcp_gcs",
                  "outputResource" : inputs.deid_output_resource
                }
              }
            );
          }
      dataflow_template_location_dir: create_gcs_temp_working_urls/gcs_temp_working_df_template_url
      deid_output_resource          : create_gcs_temp_working_urls/gcs_temp_working_df_deid_output_resource
      load_target_table:
        source:
          - deid_cdm_project_id
          - generate_temp_name/generated_name
        valueFrom: >-
          ${
            return JSON.stringify({
              "projectId": self[0],
              "datasetId": self[1],
              "tableId"  : "deid_specimen_text"
            });
          }
    out:
      - extract_job_id
      - deid_job_id
      - load_job_id



# payer_plan_period:
# id: payer_plan_period_id
# text: stop_reason_source_value

  deid_workflow:
    run: ../deid-text/wf-deid-text.cwl
    in:
      depends_on_multiple_job_ids:
        - install_deid_dataflow_template/job_done
        - bq_temp_table_cdm_payer_plan_period_with_phi/gcp_bq_job_id
      bq_gcp_credentials_key_file: gcp_credentials_key_file
      df_gcp_credentials_key_file: gcp_credentials_key_file
      extract_source_table:
        source:
          - deid_cdm_project_id
          - generate_temp_name/generated_name
        valueFrom: >-
          ${
            return JSON.stringify({
              "projectId": self[0],
              "datasetId": self[1],
              "tableId"  : "cdm_payer_plan_period_with_phi"
            });
          }
      extract_target_bucket         : create_gcs_temp_working_urls/gcs_temp_working_df_extract_target_bucket
      dataflow_project              : deid_cdm_project_id
      dataflow_run_regional_endpoint: dataflow_run_regional_endpoint
      param_dataflow_run_env_service_account_email: dataflow_run_env_service_account_email
      param_temp_location                         : create_gcs_temp_working_urls/gcs_temp_working_df_temp_url
      param_dataflow_run_env_zone                 : dataflow_run_env_zone
      param_dataflow_run_env_max_workers          : dataflow_run_env_max_workers
      dataflow_run_launch_template_parameters:
        valueFrom: >-
          ${
            return JSON.stringify({
                "jobName": "deid",
                "environment": {
                  "serviceAccountEmail": inputs.param_dataflow_run_env_service_account_email,
                  "tempLocation"       : inputs.param_temp_location,
                  "zone"               : inputs.param_dataflow_run_env_zone,
                  "maxWorkers"         : inputs.param_dataflow_run_env_max_workers
                },
                "parameters": {
                  "inputResource"  : inputs.extract_target_bucket,
                  "textInputFields": "stop_reason_source_value",
                  "textIdFields"   : "payer_plan_period_id,STUDY_ID",
                  "inputType"      : "gcp_gcs",
                  "outputResource" : inputs.deid_output_resource
                }
              }
            );
          }
      dataflow_template_location_dir: create_gcs_temp_working_urls/gcs_temp_working_df_template_url
      deid_output_resource          : create_gcs_temp_working_urls/gcs_temp_working_df_deid_output_resource
      load_target_table:
        source:
          - deid_cdm_project_id
          - generate_temp_name/generated_name
        valueFrom: >-
          ${
            return JSON.stringify({
              "projectId": self[0],
              "datasetId": self[1],
              "tableId"  : "deid_payer_plan_period_text"
            });
          }
    out:
      - extract_job_id
      - deid_job_id
      - load_job_id
