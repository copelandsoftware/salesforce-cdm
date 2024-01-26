create or replace TRIGGER sf_work_order_tg BEFORE
    INSERT OR UPDATE ON sf_work_order
    FOR EACH ROW
DECLARE
    v_update_flag     VARCHAR2(1) := 'N';
    v_emrn_category   VARCHAR2(255);
    v_sf_cust_id      VARCHAR2(255);
BEGIN
    BEGIN
        IF updating THEN
            IF (updating('SF_ASSET_ID') OR updating('PROBLEM_TYPE') OR updating('SF_SERVICE_PROVIDER_ID')) THEN
                v_update_flag := 'Y';
            END IF;
        END IF;

        IF ( inserting OR v_update_flag = 'Y' ) THEN

           dbms_output.put_line ( 'Before Cust ID ');
           select 
             sf_cust_id
           into
             v_sf_cust_id
           from
             sf_site
           where
             sf_site_id = :new.sf_site_id;
           dbms_output.put_line ( 'After Cust ID ');
    
    	
    	--get Emerson category,location,euqipment
            FOR asset IN (
                SELECT
                    category,
                    equipment,
                    location
                FROM
                    sf_control_sys scs
                WHERE
                    scs.sf_control_sys_id = :new.sf_asset_id
            ) LOOP
                v_emrn_category := asset.category;
                :new.category := asset.equipment;
                :new.equipment := asset.equipment;
                :new.location := asset.location;
            END LOOP;

   	--get mapped site name

            FOR site IN (
                SELECT
                    ss.sf_cust_id,
                    nvl(sdm.external_value, ss.sf_site_name) AS site_name
                FROM
                    sf_site ss,
                    sf_dispatch_mapping sdm
                WHERE
                    ss.sf_site_name = sdm.sf_value (+)
                    AND ss.sf_site_id = :new.sf_site_id
                    AND ss.sf_cust_id = sdm.sf_cust_id (+)
                    AND sdm.field_type (+) = 'Site'
            ) LOOP
                v_sf_cust_id := site.sf_cust_id;
                :new.site_name := site.site_name;
            END LOOP;
 	--get service provider name
	
	FOR sp in (
	    select FIRST_NAME,LAST_NAME 
		from SF_SERVICE_PROVIDER
		where SF_SERVICE_PROVIDER_ID = :new.SF_SERVICE_PROVIDER_ID
	) LOOP
	  :new.CONTRACTOR_NAME := sp.FIRST_NAME||' '||sp.LAST_NAME;
	END LOOP;
 	  
    --get mapped euqipment value

           dbms_output.put_line ( 'Before Equipement ');
            FOR equip IN (
                SELECT
                    sdm.external_value
                FROM
                    sf_dispatch_mapping sdm
                WHERE
                    sdm.sf_cust_id = v_sf_cust_id
                    AND UPPER(sdm.category) = UPPER(v_emrn_category)
                    AND sdm.field_type = 'Equipment'
                    and sdm.sf_value = :new.equipment
            ) LOOP
                :new.equipment := equip.external_value;
            END LOOP;

      --get mapped location value

           dbms_output.put_line ( 'Before Location ');
            FOR locate IN (
                SELECT
                    sdm.external_value
                FROM
                    sf_dispatch_mapping sdm
                WHERE
                    sdm.sf_cust_id = v_sf_cust_id
                    AND sdm.field_type = 'Location'
                    and sdm.sf_value = :new.location
            ) LOOP
                :new.location := locate.external_value;
            END LOOP;

      --get mapped problem type value

           dbms_output.put_line ( 'Before problem type ');
            FOR problem IN (
                SELECT
                    sdm.external_value
                FROM
                    sf_dispatch_mapping sdm
                WHERE
                    sdm.sf_cust_id = v_sf_cust_id
                    AND UPPER(sdm.category) = UPPER(v_emrn_category)
                    AND sdm.field_type = 'Problem Type'
                    and sdm.sf_value = :new.problem_type
            ) LOOP
                :new.problem_type := problem.external_value;
            END LOOP;
--get mapped severity level
          
            FOR severity IN (
                SELECT
                    nvl(sdm.external_value, ss.s_l) AS severity_level
                FROM
                    (
                        SELECT
                            :new.severity_level AS s_l,
                            sf_cust_id
                        FROM
                            sf_site
                        WHERE
                            sf_site_id = :new.sf_site_id
                    ) ss,
                    sf_dispatch_mapping sdm
                WHERE
                    ss.s_l = sdm.sf_value (+)
                    AND ss.sf_cust_id = sdm.sf_cust_id (+)
                    AND sdm.field_type (+) = 'Severity'
            ) LOOP
                :new.wo_type := severity.severity_level;
            END LOOP;
          
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line(sqlerrm);
    END;
    -- other columns

    :new.operational_status := 'Not Working';
    :new.requested_by := 'Emerson';
    :new.location_detail := 'Emerson WO' || :new.work_order_no;
END;