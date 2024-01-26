create or replace PACKAGE BODY     MSS_LOSTCONN_ALARMS_PKG
AS

   g_directory                    VARCHAR2(130) := 'MSSAUTOPROC';
   g_ctr_info_msz                 VARCHAR2 (2000);
   v_file_name                    UTL_FILE.FILE_TYPE;

	--insert into JAM.MSS_LOOKUP(mss_lookup_id,mss_lookup_type,mss_lookup_code,mss_lookup_format, mss_lookup_desc, mss_lookup_value, created_on, created_by, modified_on, modified_by)
	--values ( (select max(mss_lookup_id) from JAM.MSS_LOOKUP ) + 1, 'MSSPING','MSS Ping','CUSTOMER','Aldi',2,sysdate,'administrator',sysdate,'administrator')

	/*

	***** Nagios *****

	Notification Type: PROBLEM

	Service: PING
	Host: SHOPPERS_02381
	Address: 10.121.73.61
	State: CRITICAL

	Date/Time: Thu Sept 7 10:22:41 EDT 2017

	Additional Info:

	CRITICAL - 10.121.73.65: rta nan, lost 100%

	*/

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
  		SYS_CONFIG_TYPE_CD = 'EmailSenderAddr';

  EXCEPTION WHEN NO_DATA_FOUND THEN
         p_sender := 'MSS@emerson.com';
  END get_sender_p;


  PROCEDURE get_mailhost_p( p_mailhost  OUT VARCHAR2 ) AS
  BEGIN

  	select
       		SYS_CONFIG_VALUE
       	into
       		p_mailhost
       	from
       		MSS_SYS_CONFIG
       	where
       		SYS_CONFIG_TYPE_CD='EmailSenderHost';

  EXCEPTION WHEN NO_DATA_FOUND THEN
         p_mailhost := 'INETMAIL.EMRSN.NET';
  END get_mailhost_p;



 PROCEDURE  send_mail(
                        errbuf 		OUT VARCHAR2,
                    	retcode 	OUT VARCHAR2,
                    	p_recipient	IN VARCHAR2,
                        p_subject       IN VARCHAR2
  		     ) AS

    v_mailhost  VARCHAR2(100);                       --ex:'linux3.ersapps.local';--###Change Emerson available mail server###
    v_sender    VARCHAR2(100);

    v_conn  UTL_SMTP.connection;
    v_msg varchar2(4000);
    v_body varchar2(2000);

    v_cnt number(10) := 0;

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
     v_email_host varchar2(255);
     v_from_email_addr varchar2(255);
     v_recipient varchar2(255);

  BEGIN

    LOG_FILE (directory_name => g_directory,file_name => v_file_name,o_return_status => v_return_status);

    g_ctr_info_msz := '----------------------------------------------------------------------------------'||
                      chr(13) || chr(10)||' Email CTR Package Execution Started @ '||to_char(sysdate,'MM/DD/YYYY HH24:MI:SS')||
                      chr(13) || chr(10)||'----------------------------------------------------------------------------------';

    WRITE_LOG_FILE(file_name => v_file_name,info     => g_ctr_info_msz ,o_return_status  => v_return_status) ;


    get_sender_p(p_sender =>v_sender);

    --get_mailhost_p(p_mailhost =>v_mailhost);

    --  v_mailhost := 'linux3.ers.na.emersonclimate.org';

    v_mailhost := 'INETMAIL.EMRSN.NET';

    v_conn := UTL_SMTP.open_connection(v_mailhost, 25);
    UTL_SMTP.ehlo(v_conn, v_mailhost);

    FOR failed_rec in c_failed_stores LOOP

        v_msg := NULL;
        v_cnt := v_cnt + 1;

    	v_body := ' ***** Nagios ***** ' || UTL_TCP.CRLF
    			     	|| 'Notification Type: PROBLEM' || UTL_TCP.CRLF
    			     	|| 'Service: PING' || UTL_TCP.CRLF
    			     	|| 'Host: ' || failed_rec.store_name || UTL_TCP.CRLF
    			     	|| 'Address:' || failed_rec.ip_address || UTL_TCP.CRLF
    			     	|| 'State: CRITICAL' || UTL_TCP.CRLF
    			     	|| 'Date/Time: ' || Sysdate || UTL_TCP.CRLF
    			     	|| 'Additional Info:CRITICAL - ' || failed_rec.ip_address || ' rta nan, lost 100%' || UTL_TCP.CRLF;


        UTL_SMTP.mail(v_conn, v_sender);
        UTL_SMTP.rcpt(v_conn, p_recipient);

    	v_msg := 'Content-Type: text/html; Date:'|| TO_CHAR(SYSDATE, 'dd mon yy hh24:mi:ss')|| UTL_TCP.CRLF ||
    	'From: '|| v_sender || '<' || v_sender || '>'
        || UTL_TCP.CRLF || 'To: '  || p_recipient || '<' || p_recipient || '>'
        || UTL_TCP.CRLF || 'Subject: ' || p_subject
        || UTL_TCP.CRLF || UTL_TCP.CRLF
        || v_body ;

    	UTL_SMTP.open_data(v_conn);
    	UTL_SMTP.write_raw_data(v_conn, UTL_RAW.cast_to_raw(v_msg));
    	UTL_SMTP.close_data(v_conn);

     	DBMS_OUTPUT.put_line(' Email Sent...' || v_cnt);

    END LOOP;

    UTL_SMTP.quit(v_conn);

    g_ctr_info_msz := ' Emais sent Successfully => ' || v_cnt;

    WRITE_LOG_FILE(file_name => v_file_name,info     => g_ctr_info_msz ,o_return_status  => v_return_status ) ;

    g_ctr_info_msz := '----------------------------------------------------------------------------------'||
                         chr(13) || chr(10)||' Email CTR Package Execution Completed @  '||to_char(sysdate,'MM/DD/YYYY HH24:MI:SS')||
                         chr(13) || chr(10)||'----------------------------------------------------------------------------------'
                         ;

    WRITE_LOG_FILE(file_name => v_file_name,info     => g_ctr_info_msz,o_return_status  => v_return_status) ;

    CLOSE_LOG_FILE(v_file_name);

    DBMS_OUTPUT.put_line('Email Sent Successfully');

    EXCEPTION WHEN OTHERS THEN
        DBMS_OUTPUT.put_line(DBMS_UTILITY.format_error_stack);
        DBMS_OUTPUT.put_line(DBMS_UTILITY.format_call_stack);

     	IF utl_file.is_open(v_file_name) THEN
	  WRITE_LOG_FILE(file_name => v_file_name,info     => DBMS_UTILITY.format_error_stack,o_return_status  => v_return_status) ;
 	END IF;
        CLOSE_LOG_FILE(v_file_name);

  END send_mail;

END MSS_LOSTCONN_ALARMS_PKG;