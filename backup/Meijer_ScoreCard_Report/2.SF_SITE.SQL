alter table SF_SITE add DIVISION VARCHAR2(50);

/

UPDATE sf_site pss
SET
    DIVISION = (
        select nvl(division.division_name,nvl(district.district_name,nvl(region.region_name,''))) division
from sf_site ss ,cmn_site cs,sf_customer sc,
 (select   
               party_name.first_name as rg_first_name, party_name.last_name as rg_last_name, cust_level_site.site_id, refe.ref_name as region_name, cust_level_hier.cust_level_id as cust_level_region_ID  
          from   
               CMN_PARTY_NAME party_name, CMN_CUST_LEVEL_HIER cust_level_hier, MSS_CUST_LEVEL_SITE cust_level_site, cmn_reference refe  
           where  
               cust_level_site.cust_level_id = cust_level_hier.cust_level_id   
               and cust_level_hier.cust_level_id = refe.ref_id  
               and cust_level_hier.cust_level_type_id = 3  
               and cust_level_hier.party_name_id = party_name.party_name_id(+) and  
               cust_level_hier.status_cd = '1'  
         ) region,
          (select   
               party_name.first_name as ds_first_name, party_name.last_name as ds_last_name, cust_level_site.site_id, refe.ref_name as district_name, cust_level_hier.cust_level_id as cust_level_district_ID  
          from   
               CMN_PARTY_NAME party_name, CMN_CUST_LEVEL_HIER cust_level_hier, MSS_CUST_LEVEL_SITE cust_level_site, cmn_reference refe  
           where  
               cust_level_site.cust_level_id = cust_level_hier.cust_level_id   
               and cust_level_hier.cust_level_id = refe.ref_id  
               and cust_level_hier.cust_level_type_id = 1  
               and cust_level_hier.party_name_id = party_name.party_name_id(+) and  
               cust_level_hier.status_cd = '1'  
         ) district  ,
         
              (select   
               party_name.first_name as dm_first_name, party_name.last_name as dm_last_name, cust_level_site.site_id, refe.ref_name as division_name, cust_level_hier.cust_level_id as cust_level_division_ID  
          from   
               CMN_PARTY_NAME party_name, CMN_CUST_LEVEL_HIER cust_level_hier, MSS_CUST_LEVEL_SITE cust_level_site, cmn_reference refe  
           where  
               cust_level_site.cust_level_id = cust_level_hier.cust_level_id   
               and cust_level_hier.cust_level_id = refe.ref_id  
               and cust_level_hier.cust_level_type_id = 5   
               and cust_level_hier.party_name_id = party_name.party_name_id(+) and  
               cust_level_hier.status_cd = '1'  
         ) division  
where ss.mss_site_id =cs.site_id
and ss.sf_cust_id=sc.sf_cust_id
and cs.site_id = region.site_id(+)
and cs.site_id = district.site_id(+)
and cs.site_id = division.site_id(+)
and sc.sf_cust_name='Meijer HQ'
and ss.sf_site_id = pss.sf_site_id
);
	
/

commit;