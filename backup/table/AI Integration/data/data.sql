update sf_site ss set mss_cust_id=(select mss_cust_id from sf_customer where sf_cust_id=ss.sf_cust_id);

update SF_CUST_ALARM_FLOW_CONFIG set alarm_flow_flag ='SF';