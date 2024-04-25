WITH aptus_org AS (
  SELECT supplier_name_account_name,
  supplier_name_legal_name,
  supplier_name_diversity_types,
  supplier_name_certifying_entity,
  supplier_name_ein,
  CASE 
    WHEN TRIM(supplier_id_vms_ats)='-' THEN NULL 
    ELSE supplier_id_vms_ats 
  END AS supplier_id_vms_ats,
  vms_tool,
  program,
  supplier_name_contact_email, 
  CASE
    WHEN TRIM(supplier_id_erp)='-' THEN NULL
    --WHEN REGEXP_CONTAINS(supplier_id_erp, r'^-?[0-9]+$') THEN supplier_id_erp ELSE NULL
    ELSE supplier_id_erp
  END AS supplier_id_erp,
  CASE 
    WHEN TRIM(supplier_name_tax_id)='-' THEN NULL 
    ELSE supplier_name_tax_id 
  END AS supplier_name_tax_id,
  supplier_name_dba,
  CASE 
    WHEN TRIM(customer_code)='-' THEN NULL 
    ELSE customer_code 
  END AS customer_code,
  supplier_name_holding_company,
  supplier_name_parent_account_id,
  supplier_name_parent_account_name,
  supplier_name_randstad_business_line,
  supplier_name_randstad_entity_name,
  supplier_name_supplier_apttus_id,
  supplier_name_validated_ultimate_holding_company
  from db_nam_aptus.msp_slsfrc_nam_aptus_mst_supplier_diversity_updated
), aptus_hash AS (
  SELECT *,
    MD5(concat(COALESCE(supplier_name_account_name,'NA'),COALESCE(supplier_name_legal_name,'NA'),COALESCE(supplier_name_diversity_types,'NA'),COALESCE(supplier_name_certifying_entity,'NA'),COALESCE(supplier_name_ein,'NA'),COALESCE(supplier_id_vms_ats,'NA'),
COALESCE(vms_tool,'NA'),COALESCE(program,'NA'),COALESCE(supplier_name_contact_email,'NA'),COALESCE(supplier_id_erp,'NA'),COALESCE(supplier_name_tax_id,'NA'),COALESCE(supplier_name_dba,'NA'),COALESCE(customer_code,'NA'),COALESCE(supplier_name_holding_company,'NA'),
COALESCE(supplier_name_parent_account_id,'NA'),COALESCE(supplier_name_parent_account_name,'NA'),COALESCE(supplier_name_randstad_business_line,'NA'),COALESCE(supplier_name_randstad_entity_name,'NA'),
COALESCE(supplier_name_supplier_apttus_id,'NA'),COALESCE(supplier_name_validated_ultimate_holding_company,'NA'))) AS rsr_hash_key
FROM aptus_org
WHERE customer_code IS NOT NULL AND customer_code<>'' AND supplier_id_erp IS NOT NULL AND supplier_id_erp<>''
      AND supplier_id_vms_ats IS NOT NULL AND supplier_id_vms_ats<>''
      AND customer_code NOT IN ('DELUXE','IPSE','LENX','WEC') -- this exclusion list given by John on 3/26/24
), aptus_dup_ct AS (
  SELECT t.*,t.supplier_id_vms_ats AS supplier_code,dist_sup.rsr_supp_id_erp_dist_ct,overall_ct.rsr_overall_ct
  FROM aptus_hash t 
  LEFT JOIN (SELECT customer_code,supplier_id_vms_ats,COUNT(DISTINCT supplier_id_erp) as rsr_supp_id_erp_dist_ct
        FROM aptus_hash
        GROUP BY customer_code,supplier_id_vms_ats
        --HAVING COUNT(DISTINCT supplier_id_erp)>1
      ) dist_sup ON dist_sup.customer_code=t.customer_code AND dist_sup.supplier_id_vms_ats=t.supplier_id_vms_ats
  LEFT JOIN (SELECT customer_code,supplier_id_vms_ats,supplier_id_erp,COUNT(*) as rsr_overall_ct
        FROM aptus_hash
        GROUP BY customer_code,supplier_id_vms_ats,supplier_id_erp
        --HAVING COUNT(*)>1
      ) overall_ct ON overall_ct.customer_code=t.customer_code AND overall_ct.supplier_id_vms_ats=t.supplier_id_vms_ats
        AND overall_ct.supplier_id_erp=t.supplier_id_erp
)
SELECT *
FROM aptus_dup_ct
;


