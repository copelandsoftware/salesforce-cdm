--see how many sites need update
  select * from sf_site_last_communication where mss_cust_id is null 

--update sf_cust_id
update sf_site_last_communication ss set sf_cust_id=(select sf_cust_id from sf_site where sf_site_id=ss.sf_site_id) where ss.mss_cust_id is null;

--commit

--verify, works when there is records returning

 SELECT
		trim(site.sf_site_name) store_name
		,control.ip_address	ip_address
		,mslc.last_received_date
	FROM
		JAM.MSS_LOOKUP ml,
		jam.sf_customer c,
		jam.sf_site site,
        jam.sf_control_sys control,
		jam.sf_site_last_communication mslc
	WHERE
		ml.mss_lookup_desc = c.sf_cust_name
		AND mslc.sf_cust_id = c.sf_cust_id
		AND mslc.sf_site_id = site.sf_site_id
        AND mslc.sf_asset_id = control.sf_control_sys_id
        AND upper(ml.is_active) = 'TRUE'
        AND upper(control.is_active) = 'TRUE'
		AND ml.mss_lookup_type = 'MSSPING'

--