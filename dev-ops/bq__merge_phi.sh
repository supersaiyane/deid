#som-rit-phi-starr-dev:rit_phi_clarity_shc_20130101_20180930 som-rit-phi-starr-dev:rit_phi_clarity_deid_shc som-rit-phi-starr-dev:rit_phi_clarity_deid_shc.HNO_NOTE_TEXT_MERGED

echo "FROM:$1 and $2 TO:$3"

read -r -d '' _QUERY <<-BND_OF_QUERY

SELECT
patient.pat_id,
patient.pat_name,
patient.add_line_1, patient.add_line_2, patient.city,patient.zip,
patient.home_phone, patient.work_phone,
patient.email_address,
patient.birth_date,
patient.sex_c,
patient.ssn, patient.medicare_num,patient.medicaid_num,
patient.mother_pat_id,patient.father_pat_id,patient.birth_wrist_band,
patient.epic_pat_id,
patient.PRIM_CVG_ID,patient.PRIM_EPP_ID,
patient.PAT_MRN_ID,
patient.tmp_addr_line_1, patient.tmp_addr_line_2, patient.tmp_city,patient.tmp_zip,
patient.tmp_home_phone, patient.PAT_LAST_NAME, patient.PAT_FIRST_NAME, patient.PAT_MIDDLE_NAME,patient.PROXY_NAME, patient.PROXY_PHONE,
patient.EMPLOYER_ID,patient.GUARDIAN_NAME,
patient.cur_pcp_prov_id,
ser.prov_name as PROV_NAME,
emergency_contacts.father_name,
emergency_contacts.father_addr_ln_1,
emergency_contacts.father_addr_ln_2,
emergency_contacts.father_city,
emergency_contacts.father_zip,
emergency_contacts.father_cell_phone,
emergency_contacts.mother_name,
emergency_contacts.mother_addr_ln_1,
emergency_contacts.mother_addr_ln_2,
emergency_contacts.mother_city,
emergency_contacts.mother_zip,
emergency_contacts.mother_cell_phone,
emergency_contacts.emerg_pat_rel_c,
string_agg(DISTINCT rad_acc.acc_num, ' ' ) as accession_num

from  \`$1.PATIENT\` patient
left join \`$2.CLARITY_SER\` ser
    on patient.cur_pcp_prov_id is not null and (ser.prov_id = patient.cur_pcp_prov_id)
left join \`$2.EMERGENCY_CONTACTS\` emergency_contacts
    on emergency_contacts.pat_id = patient.pat_id
left join \`$1.ORDER_PROC\` rad
    on rad.pat_id = patient.pat_id
left join \`$2.ORDER_RAD_ACC_NUM\` rad_acc
    on rad_acc.order_proc_id = rad.order_proc_id

group by patient.pat_id, patient.pat_name,
patient.add_line_1, patient.add_line_2, patient.city,patient.zip,
patient.home_phone, patient.work_phone,
patient.email_address,
patient.birth_date,
patient.sex_c,
patient.ssn, patient.medicare_num,patient.medicaid_num,
patient.mother_pat_id,patient.father_pat_id,patient.birth_wrist_band,
patient.epic_pat_id,
patient.PRIM_CVG_ID,patient.PRIM_EPP_ID,
patient.PAT_MRN_ID,
patient.tmp_addr_line_1, patient.tmp_addr_line_2, patient.tmp_city,patient.tmp_zip,
patient.tmp_home_phone, patient.PAT_LAST_NAME, patient.PAT_FIRST_NAME, patient.PAT_MIDDLE_NAME,patient.PROXY_NAME, patient.PROXY_PHONE,
patient.EMPLOYER_ID,patient.GUARDIAN_NAME,
patient.cur_pcp_prov_id,
ser.prov_name,
emergency_contacts.father_name,
emergency_contacts.father_addr_ln_1,
emergency_contacts.father_addr_ln_2,
emergency_contacts.father_city,
emergency_contacts.father_zip,
emergency_contacts.father_cell_phone,
emergency_contacts.mother_name,
emergency_contacts.mother_addr_ln_1,
emergency_contacts.mother_addr_ln_2,
emergency_contacts.mother_city,
emergency_contacts.mother_zip,
emergency_contacts.mother_cell_phone,
emergency_contacts.emerg_pat_rel_c

BND_OF_QUERY
QUERY=$(echo "$_QUERY" | sed 's/\n/ /g')

echo $QUERY

set -x
bq --location=US query --destination_table $3  --use_legacy_sql=false "`echo $QUERY`"
set +x
