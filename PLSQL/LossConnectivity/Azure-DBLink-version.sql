CREATE DATABASE LINK "JAM_TO_UARD"
   CONNECT TO "UARD" IDENTIFIED BY VALUES ':1'
   USING 'MSSP1';

/

Insert into MSS_SYS_CONFIG (SYS_CONFIG_ID,SYS_CONFIG_TYPE_CD,SYS_CONFIG_CD,SERVICE_PROVIDER_ID,SYS_CONFIG_VALUE,STATUS_CD,CREATED_BY,CREATED_ON,MODIFIED_BY,MODIFIED_ON,REQUEST_ID,PROGRAM_APP_ID,VERSION_NUMBER,CUST_ID)
values (111,'CDMServerHostName','CDMServerHostName',null,'Azure CDM Host(10.195.65.10)','1','SEEDED',sysdate,'SEEDED',sysdate,null,null,null,null);

/

create or replace PACKAGE     MSS_LOSTCONN_ALARMS_PKG
AS

 PROCEDURE  send_lc_alarm(  errbuf          OUT VARCHAR2,
                        retcode         OUT VARCHAR2
                                );
 FUNCTION F_Get_Timezone(i_timevalue IN DATE) RETURN DATE;

END MSS_LOSTCONN_ALARMS_PKG;


/

create or replace PACKAGE BODY     MSS_LOSTCONN_ALARMS_PKG
AS

   g_directory                    VARCHAR2(130) := 'MSSAUTOPROC';
   g_ctr_info_msz                 VARCHAR2 (2000);
   v_file_name                    UTL_FILE.FILE_TYPE;

  PROCEDURE LOG_FILE (directory_name IN VARCHAR2,file_name OUT UTL_FILE.FILE_TYPE, o_return_status OUT VARCHAR2) AS

	log_file UTL_FILE.FILE_TYPE;

  BEGIN

	log_file := UTL_FILE.FOPEN(directory_name,'MSSR_CTR'||'.log','a');

	file_name := log_file;

	 o_return_status := 'S';

  EXCEPTION WHEN UTL_FILE.INVALID_PATH THEN
	RAISE_APPLICATION_ERROR(-20100,'Invalid Path');
	o_return_status := 'E';
  WHEN UTL_FILE.INVALID_MODE THEN
	RAISE_APPLICATION_ERROR(-20101,'Invalid Mode');
	o_return_status := 'E';
  WHEN UTL_FILE.INVALID_OPERATION then
	RAISE_APPLICATION_ERROR(-20102,'Invalid Operation');
	o_return_status := 'E';
  WHEN UTL_FILE.INVALID_FILEHANDLE then
	RAISE_APPLICATION_ERROR(-20103,'Invalid Filehandle');
	o_return_status := 'E';
  WHEN UTL_FILE.WRITE_ERROR then
	RAISE_APPLICATION_ERROR(-20104,'Write Error');
	o_return_status := 'E';
  WHEN UTL_FILE.READ_ERROR then
	RAISE_APPLICATION_ERROR(-20105,'Read Error');
	o_return_status := 'E';
  WHEN UTL_FILE.INTERNAL_ERROR then
	RAISE_APPLICATION_ERROR(-20106,'Internal Error');
	o_return_status := 'E';

  WHEN OTHERS THEN
  	UTL_FILE.FCLOSE(log_file);
	o_return_status := 'E';
  END LOG_FILE;

  PROCEDURE WRITE_LOG_FILE (file_name IN UTL_FILE.FILE_TYPE, info IN VARCHAR2,o_return_status OUT VARCHAR2 ) AS
  BEGIN

	UTL_FILE.PUT_LINE(file_name,info);

	o_return_status := 'S';

  EXCEPTION
	WHEN UTL_FILE.INVALID_PATH THEN
	RAISE_APPLICATION_ERROR(-20100,'Invalid Path');
	o_return_status := 'E';
	WHEN UTL_FILE.INVALID_MODE THEN
	RAISE_APPLICATION_ERROR(-20101,'Invalid Mode');
	o_return_status := 'E';
	WHEN UTL_FILE.INVALID_OPERATION then
	RAISE_APPLICATION_ERROR(-20102,'Invalid Operation');
	o_return_status := 'E';
	WHEN UTL_FILE.INVALID_FILEHANDLE then
	RAISE_APPLICATION_ERROR(-20103,'Invalid Filehandle');
	o_return_status := 'E';
	WHEN UTL_FILE.WRITE_ERROR then
	RAISE_APPLICATION_ERROR(-20104,'Write Error');
	o_return_status := 'E';
	WHEN UTL_FILE.READ_ERROR then
	RAISE_APPLICATION_ERROR(-20105,'Read Error');
	o_return_status := 'E';
	WHEN UTL_FILE.INTERNAL_ERROR then
	RAISE_APPLICATION_ERROR(-20106,'Internal Error');
	o_return_status := 'E';
	WHEN OTHERS THEN
	o_return_status := 'E';
  END WRITE_LOG_FILE;

  PROCEDURE CLOSE_LOG_FILE (file_name IN UTL_FILE.FILE_TYPE) AS
  BEGIN

	 IF utl_file.is_open(file_name) THEN
    		utl_file.fclose_all;
 	 END IF;

  END CLOSE_LOG_FILE;

  PROCEDURE get_sender_p(  p_sender  OUT VARCHAR2 ) AS
  BEGIN

  	select
  		SYS_CONFIG_VALUE
  	into
  		p_sender
  	from
  		MSS_SYS_CONFIG
  	where
  		SYS_CONFIG_TYPE_CD = 'CDMServerHostName';

  EXCEPTION WHEN NO_DATA_FOUND THEN
         p_sender := 'CDM database server';
  END get_sender_p;

   --
   --Get Timezone
   --
 FUNCTION F_Get_Timezone(
            I_TIMEVALUE IN DATE)
          RETURN DATE
        IS
          V_UPDATED_DATE DATE;
        BEGIN
          SELECT
            I_TIMEVALUE                             - (SUBSTR (TZ_OFFSET(SESSIONTIMEZONE), 1, INSTR (TZ_OFFSET(
            SESSIONTIMEZONE), ':')                  - 1) / 24 + SUBSTR (TZ_OFFSET(SESSIONTIMEZONE),
            INSTR (TZ_OFFSET(SESSIONTIMEZONE), ':') + 1, 2) / 1440)
          INTO
            V_UPDATED_DATE
          FROM
            DUAL;
          RETURN V_UPDATED_DATE;
          --
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
          --
          dbms_output.put_line ( 'Execution Error in getting the timezone details' );
        WHEN OTHERS THEN
          --
          dbms_output.put_line ('Unhandled exception in F_Get_Timezone');
          dbms_output.put_line (SQLERRM);
          --
    END F_Get_Timezone;

 PROCEDURE  send_lc_alarm(
                        errbuf 		OUT VARCHAR2,
                    	retcode 	OUT VARCHAR2
  		     ) AS

    v_host  VARCHAR2(100);
    v_sender    VARCHAR2(100);

    v_cnt number(10) := 0;

    v_gmt_now date :=systimestamp AT TIME ZONE 'GMT';
    v_est_now date :=LOCALTIMESTAMP AT TIME ZONE 'US/Eastern';

	CURSOR c_failed_stores IS
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
		AND last_received_date < (sysdate-nvl(ml.mss_lookup_value,4)/24)
    union
    SELECT
		trim(site.sf_site_name) store_name
		,control.ip_address	ip_address
		,mslc.last_received_date
	FROM
		JAM.MSS_LOOKUP ml,
		jam.sf_site site,
        jam.sf_control_sys control,
		jam.sf_site_last_communication mslc
	WHERE
		ml.mss_lookup_desc = site.sf_site_name
        AND mslc.sf_asset_id = control.sf_control_sys_id
        AND upper(ml.is_active) = 'TRUE'
        AND upper(control.is_active) = 'TRUE'
		AND ml.mss_lookup_type = 'MSSPING'
        AND ml.mss_lookup_format = 'SITE'
		AND last_received_date < (sysdate-nvl(ml.mss_lookup_value,4)/24);

     v_return_status varchar2(25);

  BEGIN

    LOG_FILE (directory_name => g_directory,file_name => v_file_name,o_return_status => v_return_status);

    g_ctr_info_msz := '----------------------------------------------------------------------------------'||
                      chr(13) || chr(10)||' CTR Package Execution Started @ '||to_char(sysdate,'MM/DD/YYYY HH24:MI:SS')||
                      chr(13) || chr(10)||'----------------------------------------------------------------------------------';

    WRITE_LOG_FILE(file_name => v_file_name,info     => g_ctr_info_msz ,o_return_status  => v_return_status) ;

    get_sender_p(p_sender =>v_sender);



    FOR failed_rec in c_failed_stores LOOP
        INSERT INTO RAW_ALARM@"JAM_TO_UARD" (
            ALM_ID,
            site_name,
            receiver,
            description,
            source,
            time_received,
            time_occurred,
            time_dialout,
            controller,
            sub_controller,
            alm_type,
            alm_state,
            alm_priority,
            caller_id,
            receiver_id,
            src_id,
            desc_id,
            file_id,
            field1,
            field2,
            field3,
            field4,
            field5,
            field6,
            field7,
            field8,
            field9,
            field10,
            field11,
            field12,
            field13,
            field14,
            field15,
            create_date,
            controller_instance,
            site_mapping_id,
            comm_mapping_id,
            receiver_host,
            rtn_date
        ) VALUES (
            RAWALARM_SEQ.NEXTVAL@"JAM_TO_UARD",
            failed_rec.store_name,
            'CDM Store Procedure',
            'Lost connectivity',
            failed_rec.ip_address,
            v_est_now,
            v_est_now,
            v_est_now,
            'Nagios',
            NULL,
            'Alarm',
            NULL,
            NULL,
            'CDM Store Procedure',
            '9999',
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            'PING',
            '0',
            '0',
            NULL,
            NULL,
            v_gmt_now,
            '1',
            NULL,
            NULL,
            v_sender,
            NULL
        );
        v_cnt := v_cnt + 1;

    END LOOP;

    COMMIT;

    g_ctr_info_msz := ' Insert LC alarm into UARD => ' || v_cnt;

    WRITE_LOG_FILE(file_name => v_file_name,info     => g_ctr_info_msz ,o_return_status  => v_return_status ) ;

    g_ctr_info_msz := '----------------------------------------------------------------------------------'||
                         chr(13) || chr(10)||' CTR Package Execution Completed @  '||to_char(sysdate,'MM/DD/YYYY HH24:MI:SS')||
                         chr(13) || chr(10)||'----------------------------------------------------------------------------------'
                         ;

    WRITE_LOG_FILE(file_name => v_file_name,info     => g_ctr_info_msz,o_return_status  => v_return_status) ;

    CLOSE_LOG_FILE(v_file_name);

    EXCEPTION WHEN OTHERS THEN
        DBMS_OUTPUT.put_line(DBMS_UTILITY.format_error_stack);
        DBMS_OUTPUT.put_line(DBMS_UTILITY.format_call_stack);

     	IF utl_file.is_open(v_file_name) THEN
	  WRITE_LOG_FILE(file_name => v_file_name,info     => DBMS_UTILITY.format_error_stack,o_return_status  => v_return_status) ;
 	END IF;
        CLOSE_LOG_FILE(v_file_name);

  END send_lc_alarm;

END MSS_LOSTCONN_ALARMS_PKG;

/

DECLARE job_exists number;
BEGIN
--DROP IF JOB EXISTS.
	select COUNT(*) INTO job_exists from user_scheduler_jobs
	    where job_name = 'MSS_CTR';
	IF job_exists >= 1
	    THEN dbms_scheduler.drop_job(job_name => 'MSS_CTR');
	END IF;
--CREATE JOB
    dbms_scheduler.create_job
    (job_name => 'MSS_CTR',
    job_type => 'PLSQL_BLOCK',
    job_action=> 'DECLARE
                  v_errbuf varchar(100);
                  v_retcode varchar(100);
                  BEGIN
                    MSS_LOSTCONN_ALARMS_PKG.send_lc_alarm
                    (errbuf => v_errbuf,
                     retcode => v_retcode
                    );
                  END;',
    start_date=> TRUNC(LOCALTIMESTAMP, 'HH24') + INTERVAL '1' HOUR,
    repeat_interval=> 'FREQ=HOURLY; INTERVAL=1',
    enabled=>true,
    auto_drop=>false,
    comments=>  'Job start date is ' || to_char(sysdate,'MM/DD/YYYY HH24:MI:SS') ||', running per hour.'
    );
END;
/