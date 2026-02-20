
SELECT
case
 when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
 else cid
 end as cid_
from bronze.erp_cust_az12;

---checking when cid is not matching with cust_key from crm_cust_info  table


select * 
from bronze.erp_cust_az12
where cid not like  'NAS%' 


---cheking if nay person bdate is greater than today

select distinct 
bdate from bronze.erp_cust_az12
where bdate > getdate()  or bdate <='1926-02-06' 


---QC -- on gen

select distinct gen
from bronze.erp_cust_az12;

select 
case
 when UPPER(TRIM(GEN)) in ('M','MALE') then 'Male'
 when upper(trim(GEN)) in ('F','FEMALE') then 'Female'
 else 'n/a'
end as gen
from bronze.erp_cust_az12;
