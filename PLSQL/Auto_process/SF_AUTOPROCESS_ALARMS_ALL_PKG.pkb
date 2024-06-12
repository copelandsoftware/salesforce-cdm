create or replace PACKAGE BODY             SF_AUTOPROCESS_ALARMS_ALL_PKG
AS
   /*=========================================================================================
   ||  PROJECT NAME          : Monitoring Service Systems Replacement(MSSR)
   ||  APPLICATION NAME      : Oracle Based Enrichment and Routing
   ||  SCRIPT NAME           : mss_autoprocess_alarms_pkg
   ||
   ||  SCRIPT DESCRIPTION / USAGE
   ||     This Package is used for enrichment of alarms in SF_NORM_ALARM
   ||
   =========================================================================================== */

   -- Following are the package body level global variables
   g_commit_record_count          NUMBER := 50;

   -- g_limit_rows              NUMBER              := 100;

   --Global table declarations for alarm update details
   TYPE g_char1_type
   IS
      TABLE OF VARCHAR2 (1)
         INDEX BY BINARY_INTEGER;

   TYPE g_number_type
   IS
      TABLE OF NUMBER (38)
         INDEX BY BINARY_INTEGER;

   TYPE g_varchar_type
   IS
      TABLE OF VARCHAR2 (2000)
         INDEX BY BINARY_INTEGER;

   TYPE g_date_type
   IS
      TABLE OF DATE
         INDEX BY BINARY_INTEGER;

   --TYPE g_date_type            IS VARRAY(366) OF DATE;
   TYPE alm_act_stat_notes_rec
   IS
      TABLE OF mss_alarm_action_status_notes%ROWTYPE;

   TYPE g_varchar_ID_type
   IS
      TABLE OF VARCHAR2 (18)
         INDEX BY BINARY_INTEGER;


   l_almtable                     alm_act_stat_notes_rec;
   g_alm_id                       g_number_type;
   g_processed_flag               g_varchar_type;
   g_auto_disregard_flag          g_varchar_type;
   g_24hrs_count                  g_number_type;
   g_7days_count                  g_number_type;
   g_email_processed              g_varchar_type;
   --chandra, oct,2018 salesforce update
   g_alert_id                     g_varchar_ID_type;
   g_almactstat_alm_id            g_number_type;
   g_techn_level_id               g_number_type;
   g_lifecycle_no                 g_number_type;
   g_ins_action_id                g_varchar_type;
   g_last_action_name             g_varchar_type;
   g_alarm_action_id              g_number_type; -- Added By mritunjay on 22Sep2012 as per CR 22240
   g_ins_status_id                g_number_type;
   g_ins_notes                    g_varchar_type;
   g_last_action_comments         g_varchar_type;
   g_ins_current_status           g_varchar_type; -- Added by Mritunjay on 27feb2012
   g_ins_created_by               g_varchar_type;
   g_ins_program_app_id           g_number_type;
   g_request_id                   g_number_type;
   --chandra sep,2018 salesforce update
   g_routing_group                g_varchar_type; --Added by Mritunjay Sinha on 28-August-2012 as implementing CB maintenace functionlity in MSSR
   g_ins_site_contact_id          g_varchar_type;

   g_version_number               g_number_type;
   g_time_available_process       g_date_type;
   --g_time_available_process  g_varchar_type;
   g_email_alarm_flag             g_varchar_type;-- gary sun added by email rule
   g_auto_email_alarm_flag        g_varchar_type;
   g_auto_email_cdm_cust          g_varchar_type;
   g_email_alarm_address          g_varchar_type;
   g_include_val                  NUMBER := 1;
   g_repeat_age                   NUMBER;
   g_alarm_count                  NUMBER;
   g_testcall_action_id           VARCHAR2(200) := '0';
   g_testcall_status_id           NUMBER := 0;
   g_disregard_action_id          VARCHAR2(200) := '0';
   g_disregard_status_id          NUMBER := 0;
   g_repeat_action_id             VARCHAR2(200) := '0';
   g_repeat_status_id             NUMBER := 0;
   g_dup_action_id                VARCHAR2(200) := '0';
   g_dup_status_id                NUMBER := 0;
   g_threshold_action_id          VARCHAR2(200) := '0';
   g_threshold_status_id          NUMBER := 0;
   g_include_action_id            VARCHAR2(200) := '0';
   g_include_status_id            NUMBER := 0;
   g_contacted_action_id          VARCHAR2(200) := '0';
   g_rtn_action_id                VARCHAR2(200) := '0';            --Added By Mritunjay
   g_rtn_status_id                NUMBER := 0;            --Added By Mritunjay
   g_rtn_disregard_action_id      VARCHAR2(200) := '0';
   g_rtn_disregard_status_id      NUMBER := 0;
   g_eventtoqueue_action_id       NUMBER := 0;
   g_eventtoqueue_status_id       NUMBER := 0;

   g_mobile_alert_action_id       NUMBER := 0;
   g_mobile_alert_status_id       NUMBER := 0;
   --Added by Mritunjay Sinha As per CR 22920
   g_lmresolved_action_id         VARCHAR2(200) := '0';
   g_contresolved_action_id       VARCHAR2(200) := '0';
   g_woresolved_action_id         VARCHAR2(200) := '0';
   g_callwowsolved_action_id      VARCHAR2(200) := '0';
   g_contactedfursolved_action_id VARCHAR2(200) := '0';
   g_storesolved_action_id        VARCHAR2(200) := '0';
   --
   --Added by Mritunjay Sinha As per CR 22804
   g_system_thresh_action_id      NUMBER := 0;
   g_system_thresh_status_id      NUMBER := 0;
   g_system_thresh_msg            VARCHAR2 (300);
   --
   g_almactstat_count             NUMBER;
   g_repeat_alm_cnt               NUMBER;
   g_duplicate_alm_cnt            NUMBER;

   --chandra sep,2018 salesforce changes
   g_dummy_sf_party_id               VARCHAR2(18);
   g_dummy_sf_site_id                VARCHAR2(18);

   --g_dummy_site_name         VARCHAR2(30);--Modified on 7-Sep-2011
   g_dummy_customer_name          VARCHAR2 (30);
   g_dummy_site_found             BOOLEAN;
   g_threshold_index              NUMBER := 0;            --Added By Mritunjay
   --chandra, aug,21
   g_runtime_error_group_id       NUMBER := 1;
   g_runtime_error_level_id       NUMBER := 3;
   g_runtime_error_category_id    NUMBER := 4;
   g_runtime_error_alert_grp_id   NUMBER := 1;
   g_runtime_error_language_id    NUMBER := 1;
   g_app_name                     VARCHAR2 (30) := 'MSS_AUTOPROCESS_ALARM';
   g_project_name                 VARCHAR2 (30) := 'MSSR';
   g_created_by                   VARCHAR2 (30) := 'MSSR';
   g_modified_by                  VARCHAR2 (30) := 'MSSR';
   g_testcall_notes               VARCHAR2 (2000) := NULL;
    --Mritunjay Sinha Adding below variables to write all DBMS into a file
   g_directory                    VARCHAR2(130) := 'MSSAUTOPROC';
   g_info_msz                     VARCHAR2 (2000);
   --chandra sep,2018 salesforce update
   g_disregard_Message_Type       Varchar2(14) := 'Disregard';
   g_rtn_Message_Type             VARCHAR2(14) := 'RTN';
   g_repeat_Message_Type          VARCHAR2(14) := 'Repeat';
   g_duplicate_Message_Type       VARCHAR2(14) := 'Duplicate';
   g_include_Message_Type         VARCHAR2(14) := 'Include';
   g_adm_Message_Type             VARCHAR2(14) := 'ADM';
   g_auto_email_Message_Type      VARCHAR2(14) := 'AutoEmail';
   g_adm_ad_Message_Type          VARCHAR2(14) := 'ADMDisregard';
   v_file_name                    UTL_FILE.FILE_TYPE;
   v_return_status                 VARCHAR2 (1);

   g_rule_approved_status         VARCHAR2(20) := 'APPROVED';
   g_cariteria_description VARCHAR2(50):='Description';
   g_cariteria_source VARCHAR2(50):='Source';
   g_cariteria_sourceDescr VARCHAR2(50):='Source + Description';

   g_adm_support_controller VARCHAR2(1000):='E2;E3;Site Supv';

  --Mritunjay Sinha Added the Procedure LOG_FILE to create a Dynamic file where we can write the log of the Autoprocess Package

PROCEDURE LOG_FILE (directory_name IN VARCHAR2,log_name IN VARCHAR2,file_name OUT UTL_FILE.FILE_TYPE, o_return_status OUT VARCHAR2) AS

log_file UTL_FILE.FILE_TYPE;

BEGIN

	--chandra sep,2018 salesforce update
	log_file := UTL_FILE.FOPEN(directory_name,log_name||'.log','a');

	file_name := log_file;

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
UTL_FILE.FCLOSE(log_file);
o_return_status := 'E';
END LOG_FILE;

--Mritunjay Sinha Added the Procedure WRITE_LOG_FILE to write the log of the Autoprocess Package

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

--Mritunjay Sinha Added the Procedure CLOSE_LOG_FILE to write the log of the Autoprocess Package

PROCEDURE CLOSE_LOG_FILE (file_name IN UTL_FILE.FILE_TYPE) AS
BEGIN
 IF utl_file.is_open(file_name) THEN
    utl_file.fclose_all;
 END IF;
END CLOSE_LOG_FILE;

   --chandra, Aug,21
   --Mritunjay,November 07 2012 , modified the logic to insert the Error details in to the Error table
   --now we are populating the error details through DBMS_OUTPUT as per CR# 17520

   PROCEDURE log_error_table (i_alm_id       IN NUMBER,
                              i_error_name   IN VARCHAR2,
                              i_error        IN VARCHAR2)
   IS
   -- v_error_id   NUMBER;
   BEGIN
      --  v_error_id := NULL;

      DBMS_OUTPUT.put_line('INFO:- description is :- '||'Error Nmae :-'||i_error_name||'   '|| SUBSTR ('Alm Id = ' || i_alm_id || '  ' || i_error,1,250));

      g_info_msz := 'INFO:- description is :- '||'Error Nmae :-'||i_error_name||'   '|| SUBSTR ('Alm Id = ' || i_alm_id || '  ' || i_error,1,250);

       WRITE_LOG_FILE(file_name => v_file_name
                       ,info     => g_info_msz
                       ,o_return_status  => v_return_status
                     ) ;
   END;

   --chandra sep,2018 salesforce update
   PROCEDURE get_site_contact_id (p_site_id                IN     VARCHAR2
                                  ,o_site_contact_id        OUT VARCHAR2
                                  ,o_get_site_cont_status   OUT VARCHAR2
                                  ,o_err_msg                   OUT VARCHAR2)
   IS
   BEGIN
      SELECT   sf_contact_id
        INTO   o_site_contact_id
        FROM   sf_site_contact
       WHERE   sf_site_id = p_site_id
       and rownum = 1
       order by sf_contact_id asc;


	o_get_site_cont_status := 'S';
   EXCEPTION
      WHEN OTHERS
      THEN
         o_get_site_cont_status := 'E';
         o_err_msg := 'INFO :- ' || SUBSTR(SQLERRM,1,1600);
         DBMS_OUTPUT.put_line (o_err_msg);

      WRITE_LOG_FILE(file_name => v_file_name ,info     => o_err_msg ,o_return_status  => v_return_status );

      SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'SF Auto processed PL/SQL',
          p_error_message =>  'INFO :- ' || SUBSTR(SQLERRM,1,1600),
              p_error_code=>'20019'
          );
   END get_site_contact_id;

   --
   PROCEDURE cache_values (o_return_status OUT VARCHAR2)
   IS
      --
      -- Default customer and site names.
      CURSOR c_dummy_site
      IS
         SELECT
           cc.sf_cust_id,
           cc.sf_cust_name,
           cs.sf_site_id
           --cs.site_name  --Modified on 7-Sep-2011
         FROM
           --chandra sep,2018 salesforce changes
           sf_customer cc,
           sf_site cs
          WHERE
            cc.sf_cust_name = 'ERSMON'
            AND cc.sf_cust_id = cs.sf_cust_id;

     CURSOR c_eventtoqueue_action
      IS
      SELECT action_id
        FROM mss_action_type_ref matr
        WHERE matr.action_name='EVENT_TO_QUEUE';

      CURSOR c_eventtoqueue_status
      IS
      SELECT status_id
        FROM mss_status_type_ref mstr
        WHERE mstr.process_status_name='Unassigned';

      CURSOR c_action
      IS
         SELECT   matr.action_id, msc.sys_config_cd, msc.sys_config_value -- chandra, Aug,21
           FROM   mss_sys_config msc, mss_action_type_ref matr
          WHERE   msc.sys_config_cd IN
                        ('TESTCALLACTION',
                         'DISREGARDACTION',
                         'REPEATACTION',
                         'DUPLICATEACTION',
                         'THRESHOLDACTION',
                         'INCLUDEACTION',
                         'CONTACTEDACTION',
                         'RTNACTION',
                         'LEFT_MSG_RESOLVED', -- Added as per CR 22920 by Mritunjay Sinha October,19 2012
                         'CONTACTED_RESOLVED', -- Added as per CR 22920 by Mritunjay Sinha October,19 2012
                         'WORK_ORDER_RESOLVED',
                         'CALL_WOW_RESOLVED',
                         'CONTACT_FUR_RESOLVED',
                         'STORE_ACT_RESOLVED',
                         'SYSTEM THRESHOLD',-- Added as per CR 229804 by Mritunjay Sinha November,29 2012
                         'MOBILEALERTDISREGARD',
                         'RTNDISREGARD')
                  AND msc.sys_config_value = matr.action_name;

      CURSOR c_status
      IS
         SELECT   mstr.status_id, msc.sys_config_cd, msc.sys_config_value --chandra, Aug,21
           FROM   mss_status_type_ref mstr, mss_sys_config msc
          WHERE   msc.sys_config_cd IN
                        ('TESTCALLSTATUS',
                         'DISREGARDSTATUS',
                         'REPEATSTATUS',
                         'DUPLICATESTATUS',
                         'THRESHOLDSTATUS',
                         'INCLUDESTATUS',
                         'RTNSTATUS',
                         'SYSTHRESHSTATUS',-- Added as per CR 22804 by Mritunjay Sinha November,29 2012
                         'MOBILEALERTSTATUS',
                         'RTNDISREGARDSTATUS')
                  AND msc.sys_config_value = mstr.process_status_name;

      --chandra, Aug,21
      CURSOR c_notes
      IS
         SELECT   msc.sys_config_cd, msc.sys_config_value
           FROM   mss_sys_config msc
          WHERE   msc.sys_config_cd IN ('TESTCALLNOTES', 'SYSTHRESHMSG'); -- Added as per CR 22804 by Mritunjay Sinha November,29 2012

    CURSOR C_Message_Type_Ref
    IS
    SELECT Message_Type_Id,Message_Type_Code
    FROM   mss_message_type_ref
    WHERE  MESSAGE_TYPE_CODE IN ('RTN','DISREGARD','REPEAT','DUPLICATE','INCLUDE');

    CURSOR c_adm_support_controller
    IS
       SELECT SYS_CONFIG_VALUE
       FROM MSS_SYS_CONFIG
       WHERE SYS_CONFIG_TYPE_CD ='ADM' AND SYS_CONFIG_CD='SUPPORT_CONTROLLER';


      v_mapping_found   BOOLEAN := FALSE;
      v_return_status   VARCHAR2 (1);
      v_cache_exception EXCEPTION;
   --
   BEGIN
      g_alarm_count := 0;
      g_almactstat_count := 0;

    /* chandra sep,2018 salesforce update

    FOR message_type_ref_rec in c_message_type_ref LOOP
        IF  message_type_ref_rec.MESSAGE_TYPE_CODE ='RTN' THEN
            g_rtn_message_type_id := message_type_ref_rec.message_type_id;
        ELSIF    message_type_ref_rec.MESSAGE_TYPE_CODE ='DISREGARD' THEN
          --chandra sep,2018 salesforce update  g_disregard_message_type_id := message_type_ref_rec.message_type_id;
          null;
        ELSIF  message_type_ref_rec.MESSAGE_TYPE_CODE ='REPEAT' THEN
            g_repeat_message_type_id := message_type_ref_rec.message_type_id;
        ELSIF  message_type_ref_rec.MESSAGE_TYPE_CODE ='DUPLICATE' THEN
            g_duplicate_message_type_id := message_type_ref_rec.message_type_id;
        ELSIF  message_type_ref_rec.MESSAGE_TYPE_CODE ='INCLUDE' THEN
            g_include_message_type_id := message_type_ref_rec.message_type_id;
        End If;
     END LOOP;

     chandra sep,2018 salesforce update end */

     FOR c_support_controller_rec IN c_adm_support_controller
     LOOP
       g_adm_support_controller:=c_support_controller_rec.SYS_CONFIG_VALUE;
     END LOOP;

      --Fetch the Dummy Site Details
      FOR c_dummy_site_rec IN c_dummy_site
      LOOP
         g_dummy_sf_party_id := c_dummy_site_rec.sf_cust_id;
         g_dummy_sf_site_id := c_dummy_site_rec.sf_site_id;
         -- g_dummy_site_name        := C_Dummy_Site_Rec.site_name;  --Modified on 7-Sep-2011
         g_dummy_customer_name := c_dummy_site_rec.sf_cust_name;
      END LOOP;

      IF g_dummy_sf_site_id IS NULL
      THEN
         log_error_table (i_alm_id       => NULL,
                          i_error_name   => ' ERSMON Dummy Site Not Found',
                          i_error        => ' ERSMON Dummy Site Not Found');
         --dbms_output.put_line('Dummy Site ERSMON Is Not Found. Contact Administrator.');
         g_dummy_site_found := FALSE;
      END IF;

      FOR c_eventtoqueue_action_rec IN c_eventtoqueue_action
      LOOP
      g_eventtoqueue_action_id:=c_eventtoqueue_action_rec.action_id;
      END LOOP;
      DBMS_OUTPUT.put_line('g_eventtoqueue_action_id'||g_eventtoqueue_action_id);
      FOR c_eventtoqueue_status_rec IN c_eventtoqueue_status
      LOOP
      g_eventtoqueue_status_id:=c_eventtoqueue_status_rec.status_id;
      END LOOP;
       DBMS_OUTPUT.put_line('g_eventtoqueue_status_id'||g_eventtoqueue_status_id);
      --dbms_output.put_line('before status cursor.');
      FOR c_status_rec IN c_status
      LOOP
         IF c_status_rec.sys_config_cd = 'TESTCALLSTATUS'
         THEN
            g_testcall_status_id := c_status_rec.status_id;
         ELSIF c_status_rec.sys_config_cd = 'DISREGARDSTATUS'
         THEN
            g_disregard_status_id := c_status_rec.status_id;
         ELSIF c_status_rec.sys_config_cd = 'REPEATSTATUS'
         THEN
            g_repeat_status_id := c_status_rec.status_id;
         ELSIF c_status_rec.sys_config_cd = 'DUPLICATESTATUS'
         THEN
            g_dup_status_id := c_status_rec.status_id;
         ELSIF c_status_rec.sys_config_cd = 'THRESHOLDSTATUS'
         THEN
            g_threshold_status_id := c_status_rec.status_id;
         ELSIF c_status_rec.sys_config_cd = 'INCLUDESTATUS'
         THEN
            g_include_status_id := c_status_rec.status_id;
         --
         ELSIF c_status_rec.sys_config_cd = 'RTNSTATUS'
         THEN
            g_rtn_status_id := c_status_rec.status_id;
         ELSIF c_status_rec.sys_config_cd = 'RTNDISREGARDSTATUS'
         THEN
            g_rtn_disregard_status_id := c_status_rec.status_id;
         ELSIF c_status_rec.sys_config_cd = 'SYSTHRESHSTATUS'
         THEN
            g_system_thresh_status_id := c_status_rec.status_id; -- Added By Mritunjay Sinha as per change for CR 22804
         ELSIF c_status_rec.sys_config_cd = 'MOBILEALERTSTATUS'
         THEN
            g_mobile_alert_status_id:= c_status_rec.status_id;
         END IF;
      END LOOP;

      FOR c_action_rec IN c_action
      LOOP
         IF c_action_rec.sys_config_cd = 'TESTCALLACTION'
         THEN
            g_testcall_action_id := c_action_rec.sys_config_value;
         ELSIF c_action_rec.sys_config_cd = 'DISREGARDACTION'
         THEN
            g_disregard_action_id := c_action_rec.sys_config_value;
         ELSIF c_action_rec.sys_config_cd = 'REPEATACTION'
         THEN
            g_repeat_action_id := c_action_rec.sys_config_value;
         ELSIF c_action_rec.sys_config_cd = 'DUPLICATEACTION'
         THEN
            g_dup_action_id := c_action_rec.sys_config_value;
         ELSIF c_action_rec.sys_config_cd = 'THRESHOLDACTION'
         THEN
            g_threshold_action_id := c_action_rec.sys_config_value;
         ELSIF c_action_rec.sys_config_cd = 'INCLUDEACTION'
         THEN
            g_include_action_id := c_action_rec.sys_config_value;
         ELSIF c_action_rec.sys_config_cd = 'CONTACTEDACTION'
         THEN
            g_contacted_action_id := c_action_rec.sys_config_value;
         ELSIF c_action_rec.sys_config_cd = 'RTNACTION'
         THEN
            g_rtn_action_id := c_action_rec.sys_config_value;
         ELSIF c_action_rec.sys_config_cd = 'RTNDISREGARD'
         THEN
            g_rtn_disregard_action_id := c_action_rec.sys_config_value;
         ELSIF c_action_rec.sys_config_cd = 'LEFT_MSG_RESOLVED'
         THEN
            g_lmresolved_action_id := c_action_rec.sys_config_value;
         ELSIF c_action_rec.sys_config_cd = 'CONTACTED_RESOLVED'
         THEN
            g_contresolved_action_id := c_action_rec.sys_config_value;
         ELSIF c_action_rec.sys_config_cd = 'WORK_ORDER_RESOLVED'
         THEN
            g_woresolved_action_id := c_action_rec.sys_config_value;
         ELSIF c_action_rec.sys_config_cd = 'CALL_WOW_RESOLVED'
         THEN
            g_callwowsolved_action_id := c_action_rec.sys_config_value;
         ELSIF c_action_rec.sys_config_cd = 'CONTACT_FUR_RESOLVED'
         THEN
            g_contactedfursolved_action_id := c_action_rec.sys_config_value;
         ELSIF c_action_rec.sys_config_cd = 'STORE_ACT_RESOLVED'
         THEN
            g_storesolved_action_id := c_action_rec.sys_config_value;
         ELSIF c_action_rec.sys_config_cd = 'SYSTEM THRESHOLD'
         THEN
            g_system_thresh_action_id := c_action_rec.action_id; -- Added By Mritunjay Sinha as per change for CR 22804
         ELSIF c_action_rec.sys_config_cd = 'MOBILEALERTDISREGARD'
         THEN
            g_mobile_alert_action_id :=c_action_rec.action_id;
         END IF;
      END LOOP;

      FOR notes_rec IN c_notes
      LOOP
         IF notes_rec.sys_config_cd = 'TESTCALLNOTES'
         THEN
            g_testcall_notes := notes_rec.sys_config_value;
         ELSIF notes_rec.sys_config_cd = 'SYSTHRESHMSG'
         THEN
            g_system_thresh_msg := notes_rec.sys_config_value;
         END IF;
      END LOOP;

      o_return_status := 'S';
   EXCEPTION
      WHEN v_cache_exception
      THEN
         DBMS_OUTPUT.put_line ('INFO :- Unable to caching the values');
          g_info_msz := 'Unable to caching the values';

       WRITE_LOG_FILE(file_name => v_file_name
                       ,info     => g_info_msz
                       ,o_return_status  => v_return_status
                     ) ;
         o_return_status := 'E';
      WHEN OTHERS
      THEN
        DBMS_OUTPUT.put_line ('INFO :- The Info for cache_values Procedure is ' || SUBSTR(SQLERRM,1,1600) );
         g_info_msz := 'The Info for cache_values Procedure is ' || SUBSTR(SQLERRM,1,1600);

       WRITE_LOG_FILE(file_name => v_file_name
                       ,info     => g_info_msz
                       ,o_return_status  => v_return_status
                     ) ;

         o_return_status := 'E';
          SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'Auto processed PL/SQL',
          p_error_message =>  'The Info for cache_values Procedure is ' || SUBSTR(SQLERRM,1,1600),
              p_error_code=>'20018'
          );
   END cache_values;

   PROCEDURE generate_alarm_query (p_adm_flag      IN     VARCHAR2,
                                  o_query_string OUT VARCHAR2)
   IS
   v_operation VARCHAR2 (10);
   BEGIN
      IF p_adm_flag='Y' THEN
        v_operation:='IN';
      ELSE
        v_operation:='NOT IN';
      END IF;
      o_query_string:='SELECT
                    ALARM_ID,
                    RECEIVER,
                    DESCR,
                    SOURCE,
                    TIME_RECEIVED,
                    TIME_OCCURRED,
                    CONTROLLER_INSTANCE,
                    CONTROLLER,
                    CREATED_ON,
                    NORM_DESC_ID,
                    NORM_SOURCE_ID,
                    sf_SITE_ID,
                    mna.sf_CUST_ID,
                    DECODE (field14,
                            ''CB Maintenance'',
                            NVL (cb.sf_cb_routing_group, mna.SF_ROUTING_GROUP),
                            mna.SF_ROUTING_GROUP)
                    routing_group,
                    rtn_date,
                    FIELD5,
                    FIELD6,
                    FIELD8,
                    FIELD9,
                    FIELD14
             FROM   SF_NORM_ALARM mna,
                    (SELECT   	c.sf_cust_id,
                    	 	c.sf_cb_routing_group
                       FROM   sf_customer c
                      WHERE   c.sf_active_service_routing = ''Y'') cb
            WHERE   processed_flag = ''N''
                    AND cb.sf_cust_id(+) = mna.sf_cust_id
                    AND mna.sf_cust_id ' || v_operation ||' (
                       select sf_cust_id from sf_message_config
                       where message_type = ''ADM'' and sf_cust_id is not null
                    )
         ORDER BY   alarm_id ASC';
   END generate_alarm_query;

   --
   --Updated on 20 Feb 2012
   --Added a Procedure to convert the given time into Easten time
   --
   FUNCTION get_timezones_SERVER_TO_GMT (i_timevalue IN DATE)
      RETURN DATE
   IS
      --
      -- v_timezone_value   VARCHAR2 (6);
      v_updated_date   DATE;
   --
   BEGIN
      --
      --SELECT SESSIONTIMEZONE
      --INTO v_timezone_value
      --FROM DUAL;
      --
        SELECT
        i_timevalue - (SUBSTR (TZ_OFFSET(SESSIONTIMEZONE), 1, INSTR (TZ_OFFSET(
        SESSIONTIMEZONE), ':')- 1) / 24 + SUBSTR (TZ_OFFSET(SESSIONTIMEZONE),
        INSTR (TZ_OFFSET(SESSIONTIMEZONE), ':') + 1, 2) / 1440)
        INTO v_updated_date
        FROM DUAL;

      RETURN v_updated_date;
   --
   EXCEPTION
       WHEN NO_DATA_FOUND
      THEN
         --
         DBMS_OUTPUT.put_line ('INFO :- Unable to convert timezone' );

           g_info_msz := 'Unable to convert timezone';

          WRITE_LOG_FILE(file_name => v_file_name
                        ,info     => g_info_msz
                        ,o_return_status  => v_return_status
                        );

      WHEN OTHERS
      THEN
         --
         DBMS_OUTPUT.put_line ('INFO :- the Info for get_timezones is :- '||SUBSTR(SQLERRM,1,1600));

      g_info_msz := 'the Info for get_timezones is :- '||SUBSTR(SQLERRM,1,1600);

          WRITE_LOG_FILE(file_name => v_file_name
                        ,info     => g_info_msz
                        ,o_return_status  => v_return_status
                        );
                        SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'Auto processed PL/g_adm_Message_TypeSQL',
          p_error_message =>  'the Info for get_timezones is :- '||SUBSTR(SQLERRM,1,1600),
              p_error_code=>'20017'
          );
   --
   END get_timezones_SERVER_TO_GMT;

    FUNCTION get_timezones_GMT_TO_SERVER (i_timevalue IN DATE)
      RETURN DATE
   IS
      --
      -- v_timezone_value   VARCHAR2 (6);
      v_updated_date   DATE;
   --
   BEGIN
      --
      --SELECT SESSIONTIMEZONE
      --INTO v_timezone_value
      --FROM DUAL;
      --
        SELECT
        i_timevalue + (SUBSTR (TZ_OFFSET(SESSIONTIMEZONE), 1, INSTR (TZ_OFFSET(
        SESSIONTIMEZONE), ':')- 1) / 24 + SUBSTR (TZ_OFFSET(SESSIONTIMEZONE),
        INSTR (TZ_OFFSET(SESSIONTIMEZONE), ':') + 1, 2) / 1440)
        INTO v_updated_date
        FROM DUAL;

      RETURN v_updated_date;
   --
   EXCEPTION
       WHEN NO_DATA_FOUND
      THEN
         --
         DBMS_OUTPUT.put_line ('INFO :- Unable to convert timezone' );

           g_info_msz := 'Unable to convert timezone';

          WRITE_LOG_FILE(file_name => v_file_name
                        ,info     => g_info_msz
                        ,o_return_status  => v_return_status
                        );

      WHEN OTHERS
      THEN
         --
         DBMS_OUTPUT.put_line ('INFO :- the Info for get_timezones is :- '||SUBSTR(SQLERRM,1,1600));

      g_info_msz := 'the Info for get_timezones is :- '||SUBSTR(SQLERRM,1,1600);

          WRITE_LOG_FILE(file_name => v_file_name
                        ,info     => g_info_msz
                        ,o_return_status  => v_return_status
                        );
                        SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'Auto processed PL/SQL',
          p_error_message =>  'the Info for get_timezones is :- '||SUBSTR(SQLERRM,1,1600),
              p_error_code=>'20017'
          );
   --
   END get_timezones_GMT_TO_SERVER;

   --


	FUNCTION GET_TIMEZONES(
		I_TIMEVALUE IN DATE)
	  RETURN DATE
	IS
	  --
	  -- v_timezone_value   VARCHAR2 (6);
	  V_UPDATED_DATE DATE;
	  --
	BEGIN
	  --
	  --SELECT SESSIONTIMEZONE
	  --INTO v_timezone_value
	  --FROM DUAL;
	  --
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
	  DBMS_OUTPUT.PUT_LINE ( 'Execution Error in getting the timezone details' );
	WHEN OTHERS THEN
	  --
	  DBMS_OUTPUT.PUT_LINE ('Unhandled exception in Get_timezones');
	  DBMS_OUTPUT.PUT_LINE (SQLERRM);
	  --
	END get_timezones;

   --chandra Aug,21 store alarm status with notes
   --chandra sep,2018 salesforce update
   PROCEDURE store_alarm_status_new (i_alm_id            IN     NUMBER,
                                     i_cust_id           IN     VARCHAR2, --chandra sep,2018 salesforce update
                                     i_site_id           IN     VARCHAR2,
                                     i_description       IN     VARCHAR2,
                                     i_controller_inst   IN     VARCHAR2,
                                     i_source            IN     VARCHAR2,
                                     i_request_id        IN     NUMBER,
                                     i_processed_flag    IN     VARCHAR2,
                                     i_adm_auto_disregard IN    VARCHAR2,
                                     i_email_processed   IN     VARCHAR2,
                                     i_email_alert_id    IN     VARCHAR2, --store alarm action, status, notes
                                     i_action_id         IN     VARCHAR2,
                                     i_status_id         IN     NUMBER,
                                     i_site_contact_id   IN     VARCHAR2, --chandra sep,2018 salesforce update
                                     i_notes             IN     VARCHAR2,
                                     i_current_status    IN     VARCHAR2, --Added by Mritunjay On 27feb2012
                                     i_time_available    IN     DATE,
                                     i_routing_group     IN     varchar2, --chandra sep,2018 salesforce update
                                     o_return_status        OUT VARCHAR2)
   IS
   BEGIN
	--      dbms_output.put_line('Storing Alarm '||i_alm_id);
	--      dbms_output.put_line('Processed_flag '||i_processed_flag);--mritunjay
	--       dbms_output.put_line('i_action_id =>'||i_action_id);--mritunjay

      g_alarm_count := g_alarm_count + 1;
      g_alm_id (g_alarm_count) := i_alm_id;
      g_processed_flag (g_alarm_count) := i_processed_flag;
      g_auto_disregard_flag (g_alarm_count) := i_adm_auto_disregard;
      g_email_processed (g_alarm_count) := i_email_processed;
      g_alert_id (g_alarm_count) := i_email_alert_id;
      g_time_available_process (g_alarm_count) := i_time_available;
      g_routing_group (g_alarm_count) := i_routing_group;
      g_last_action_name(g_alarm_count) :=i_action_id;
      g_last_action_comments(g_alarm_count) :=i_notes;

      --Added by Mritunjay Sinha on 28-August-2012 as implementing CB maintenace functionlity in MSSR

      --chandra Aug, 22
      IF i_processed_flag IN ('Y', 'P','M','PML') AND i_action_id IS NOT NULL -- Added as per CR 229804 by Mritunjay Sinha November,29 2012
      THEN
         --dbms_output.put_line('Value of i_site_contact_id is '||i_site_contact_id ||'for alarm id '||i_alm_id);
         --store alarm stats into the array
         g_almactstat_count := g_almactstat_count + 1;
         g_almactstat_alm_id (g_almactstat_count) := i_alm_id;
         g_ins_action_id (g_almactstat_count) := i_action_id;
         g_ins_status_id (g_almactstat_count) := i_status_id;
         g_ins_site_contact_id (g_almactstat_count) := i_site_contact_id;
         g_ins_notes (g_almactstat_count) := i_notes;
         g_ins_current_status (g_almactstat_count) := i_current_status; --Added by Mritunjay On 27feb2012
      END IF;

      --if the alarm is production alarm, then calculate the count last 24 hours, 7 days
      IF i_processed_flag NOT IN ('P','M','PML')
      THEN
         -- dbms_output.put_line('inside if part of store alarm');
         g_24hrs_count (g_alarm_count) := NULL;
         g_7days_count (g_alarm_count) := NULL;
      ELSE
         --dbms_output.put_line('inside else part of store alarm');
         BEGIN
            --chandra 12,21,2011 fixed when the count is 0 then it has be null
            SELECT   DECODE (COUNT ( * ), 0, NULL, COUNT ( * ))
              INTO   g_24hrs_count (g_alarm_count)
              --chandra sep,2018 salesforce updated
              FROM   sf_norm_alarm mna
             WHERE   mna.sf_cust_id = i_cust_id AND mna.sf_site_id = i_site_id
                     AND NVL (mna.descr, 'XXYYDD') = NVL (i_description, 'XXYYDD')
                     AND NVL (mna.controller_instance, 'XXYYCI') = NVL (i_controller_inst, 'XXYYCI')
                     AND NVL (mna.SOURCE, 'XXYYSS') = NVL (i_source, 'XXYYSS')
                     AND mna.alarm_id <= i_alm_id -- Added By Mritunjay Sinha on 18 April
                     AND mna.time_received >= SYSDATE - 1;
          EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               DBMS_OUTPUT.put_line (' INFO :- 1 day alarm count not found NO_DATA_FOUND ' || i_alm_id );
                g_info_msz := '1 day alarm count not found NO_DATA_FOUND ' || i_alm_id;

          WRITE_LOG_FILE(file_name => v_file_name
                        ,info     => g_info_msz
                        ,o_return_status  => v_return_status
                        );

            WHEN OTHERS
            THEN
               DBMS_OUTPUT.put_line (' INFO :-  1 day alarm count not found WHEN OTHERS ' || i_alm_id );
            g_info_msz := '1 day alarm count not found WHEN OTHERS ' || i_alm_id;

                WRITE_LOG_FILE(file_name => v_file_name
                        ,info     => g_info_msz
                        ,o_return_status  => v_return_status
                        );
         END;


         BEGIN
            --chandra 12,21,2011 fixed when the count is 0 then it has be null
            /* commented Jan 23 2020 begin
            SELECT   DECODE (COUNT ( * ), 0, NULL, COUNT ( * ))
              INTO   g_7days_count (g_alarm_count)
              FROM   sf_norm_alarm mna
            WHERE   mna.sf_cust_id = i_cust_id AND mna.sf_site_id = i_site_id
                     AND NVL (mna.descr, 'XXYYDD') = NVL (i_description, 'XXYYDD')
                     AND NVL (mna.controller_instance, 'XXYYCI') = NVL (i_controller_inst, 'XXYYCI')
                     AND NVL (mna.SOURCE, 'XXYYSS') = NVL (i_source, 'XXYYSS')
                     AND mna.alarm_id <= i_alm_id -- Added By Mritunjay Sinha on 18 April
                     AND mna.time_received >=SYSDATE- 7;
            commented Jan 23 2020 end */
           g_7days_count (g_alarm_count) := null;
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               DBMS_OUTPUT.put_line ('INFO :- 7 day alarm count not found ' || i_alm_id);
               g_info_msz := '7 day alarm count not found WHEN NO_DATA_FOUND' || i_alm_id;

                WRITE_LOG_FILE(file_name => v_file_name
                        ,info     => g_info_msz
                        ,o_return_status  => v_return_status
                        );
            WHEN OTHERS
            THEN
               DBMS_OUTPUT.put_line ('INFO :- 7 day alarm count not found ' || i_alm_id);
               g_info_msz := '7 day alarm count not found WHEN OTHERS ' || i_alm_id;

                WRITE_LOG_FILE(file_name => v_file_name
                        ,info     => g_info_msz
                        ,o_return_status  => v_return_status
                        );
                             SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'SF Auto processed PL/SQL',
          p_error_message =>  '7 day alarm count not found WHEN OTHERS ' || i_alm_id,
              p_error_code=>'20016'
          );
         END;
      END IF;

      o_return_status := 'S';
   END store_alarm_status_new;

   --
   -- Insert MSS_ALARM_ACTION_STATUS_NOTES
   --
   PROCEDURE insert_alm_act_stat_notes (o_return_status OUT VARCHAR2)
   IS
      v_id   NUMBER;
   BEGIN
      --Inserting into MSS_SERVICE_REQUEST table
      -- Request_ID in this table is generated from a Sequence which is called from a
      -- DB Trigger. Created by, Modified By will be 'MSS' and Created_on, Modified_On will be SYSDATE.
      --DBMS_OUTPUT.PUT_LINE ('alarm_id'||g_almactstat_alm_id);--Mritunjay
      --chandra aug,22 this array will be the high array size which is bug removed this
      /*
      FOR i IN 1 .. g_almactstat_count
      LOOP
      IF g_ins_current_status(i)!='Unassigned' THEN
         INSERT INTO mss_service_request (service_request_id,
                                          created_by,
                                          created_on,
                                          modified_by,
                                          modified_on,
                                          request_id,
                                          program_app_id,
                                          version_number)
            VALUES   (NULL,
                      g_created_by,
                      get_timezones_SERVER_TO_GMT (SYSDATE),
                      g_modified_by,
                      get_timezones_SERVER_TO_GMT (SYSDATE),
                      -1,
                      -1,
                      -1)
         RETURNING   service_request_id      INTO   v_id;
       END IF;
         g_service_request_id (i) := v_id;
         v_id:=null;
      --dbms_output.put_line('g_service_request_id(i) '|| g_service_request_id(i));
      END LOOP;
      */

      --chandra Aug 22 dbms_output.put_line('Count of g_service_request_id(i) ' ||g_service_request_id.count);

      --             RETURNING service_request_id BULK COLLECT INTO g_service_request_id;

      --Inserting into MSS_ALARM_ACTION_STATUS_NOTES
      --that are already stored in the global pl/sql tables
      FORALL i IN 1 .. g_almactstat_count
               INSERT INTO   SF_ALARM_ACTION_STATUS_NOTES ( sf_alarm_action_id  ,
                                                            techn_level_id,
                                                            lifecycle_number,
                                                            sf_action_id,
                                                            sf_status_id,
                                                            sf_site_contact_id,
                                                            notes,
                                                            created_by,
                                                            created_on,
                                                            modified_by,
                                                            modified_on,
                                                            program_app_id,
                                                            request_id,
                                                            version_number,
                                                            alarm_id,
                                                            log_type) -- Mritunjay Sinha April,06
                    VALUES   ( SF_ALARM_ACT_STAT_NOTES_SQ.nextval,
                              1,                            -- chandra, Aug,21
                              1,
                              g_ins_action_id (i),
                              g_ins_status_id (i),
                              g_ins_site_contact_id (i),
                              g_ins_notes (i),
                              g_created_by,
                              get_timezones_SERVER_TO_GMT (SYSDATE),
                              g_modified_by,
                              get_timezones_SERVER_TO_GMT (SYSDATE),
                              1,
                              1,
                              1,
                              g_almactstat_alm_id (i),
                              1)                    --Mritunjay Sinha April,06
                 RETURNING   sf_alarm_action_id BULK COLLECT INTO   g_alarm_action_id; --Mritunjay Sinha September, 22

      --   DBMS_OUTPUT.put_line('the value for g_alarm_action_id is ==>'||g_alarm_action_id);
      FOR i IN 1 .. g_almactstat_count
      LOOP
         g_techn_level_id (i) := NULL;
         g_lifecycle_no (i) := NULL;
         g_ins_action_id (i) := NULL;
         g_ins_status_id (i) := NULL;
         g_ins_site_contact_id (i) := NULL;
         g_ins_notes (i) := NULL;
         g_ins_created_by (i) := NULL;
         g_ins_program_app_id (i) := NULL;
         g_request_id (i) := NULL;
         g_version_number (i) := NULL;
      --g_alarm_action_id(i) := NULL;      --Mritunjay Sinha September, 22
      END LOOP;

      -- g_almactstat_count := 0; --Mritunjay 22,June 2012
      o_return_status := 'S';
  EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line('INFO :- the Info for insert_alm_act_stat_notes is '|| SUBSTR(SQLERRM,1,1600));
         g_info_msz := 'the Info for insert_alm_act_stat_notes is '|| SUBSTR(SQLERRM,1,1600);

                WRITE_LOG_FILE(file_name => v_file_name
                        ,info     => g_info_msz
                        ,o_return_status  => v_return_status
                        );

         o_return_status := 'E';
            SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'Auto processed PL/SQL',
          p_error_message => 'the Info for insert_alm_act_stat_notes is '|| SUBSTR(SQLERRM,1,1600),
              p_error_code=>'20015'
          );
   END insert_alm_act_stat_notes;

   --
   -- Test Call Alarms
   --
   PROCEDURE chk_testcall_alarms (p_alarm_id      IN     NUMBER,
                                  p_descr         IN     VARCHAR2,
                                  o_return_stat      OUT VARCHAR2,
                                  o_err_msg          OUT VARCHAR2)
   IS
   BEGIN
      o_return_stat := 'N';

      IF (p_descr) LIKE '%Test%Call%' --Modified as per CR# 22253 on 21,August 2012
      THEN
         o_return_stat := 'S';
      END IF;
    EXCEPTION
      WHEN OTHERS
      THEN
         o_err_msg := 'INFO :- Test Call ' || SUBSTR(SQLERRM,1,1600);
         DBMS_OUTPUT.put_line (o_err_msg);
         WRITE_LOG_FILE(file_name => v_file_name
                        ,info     => o_err_msg
                        ,o_return_status  => v_return_status
                        );
         o_return_stat := 'E';
          SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'SF Auto processed PL/SQL',
          p_error_message => 'INFO :- Test Call ' || SUBSTR(SQLERRM,1,1600),
              p_error_code=>'20014'
          );
   END chk_testcall_alarms;




   --
   --Disregard Alarms
   --

    --chandra sep,2018 salesforce update
   PROCEDURE chk_disregard_alarms (p_alarm_id              IN     NUMBER,
                                   p_cust_id               IN     VARCHAR2,
                                   p_site_id               IN     VARCHAR2,
                                   p_desc                  IN     VARCHAR2,
                                   p_source                IN     VARCHAR2,
                                   p_controller_instance   IN     VARCHAR2,
                                   p_cont_name             IN     VARCHAR2,
                                   p_time_revd             IN     DATE,
                                   p_adv_type              IN     VARCHAR2, --As per CR 20244,Added By Mritunjay on 29-March-2012
                                   p_adv_value             IN     VARCHAR2, --As per CR 20244,Added By Mritunjay on 29-March-2012
								   p_prop_name             IN     VARCHAR2, --As SPA 1873, Added By Ivy on 12-Nov-2020
                                   p_field14               IN     VARCHAR2, --Added by Mritunjay Sinha on 28-August-2012 as implementing CB maintenace functionlity in MSSR
                                   o_disregard_msg            OUT VARCHAR2,
                                   o_disregard_flag           OUT VARCHAR2,
                                   o_thresh_flag              OUT VARCHAR2,
                                   o_threshold_reached        OUT VARCHAR2,
                                   o_return_status            OUT VARCHAR2,
                                   o_cb_disregard_id          OUT VARCHAR2
                                   )
   IS
      --validating wheather the alarm is eligible for disregard autoprocess or not

      CURSOR c_disregard_alm
      IS
           SELECT   mmc.MESSAGE,
                    mmc.threshold_count,
                    mmc.repeat_duplicate_timer,
                    mmc.start_date,
                    mmc.action_id,
                    mmc.sf_site_id,
                    CASE WHEN mmc.sf_site_id IS NOT NULL
                    THEN 1
                    WHEN mmc.sf_CUST_LEVEL_ID IS NOT NULL
                    THEN 2
                    ELSE
                         3
                    END AS priority
             /* INTO   v_message,
                     v_thresh_count,
                     v_thresh_time,
                     v_start_date,
                     v_cb_disregard_id*/
                     --chandra sep,2018 salesforce update
             FROM   sf_message_config mmc,
                    sf_Source_Exceptions Mse                     -- CR #22749
            WHERE
            	mmc.message_type = g_disregard_message_type
                AND mmc.sf_message_id = mse.sf_message_id(+)        -- CR #22749
                AND mmc.sf_cust_id = p_cust_id
                -- Gary 09,29,2017 add division level
                /* chandra sep,2018 salesforce update needs to be reviewed begin
                AND NVL(sf_CUST_LEVEL_ID,-2) IN
                        (
                         SELECT CUST_LEVEL_ID
                         FROM MSS_CUST_LEVEL_SITE
                         WHERE SITE_ID=p_site_id
                         UNION
                         SELECT -2 AS CUST_LEVEL_ID
                         FROM DUAL
                        )
                 chandra sep,2018 salesforce update end*/
                    --chandra 12,21,2011 fixed this rule
                    AND NVL (mmc.sf_site_id, p_site_id) = p_site_id
                    --chandra 12,21,2011 fixed this rule
                    AND UPPER (NVL (mmc.description, NVL (p_desc, 'XXYYPP'))) =
                          UPPER (NVL (p_desc, 'XXYYPP'))
                    --chandra 12,21,2011 fixed this rule
                    AND UPPER (NVL (mmc.source_name, NVL (p_source, 'XXYYRR'))) =
                          UPPER (NVL (p_source, 'XXYYRR'))
                    -- CR #22749, start
                    AND (UPPER (
                            NVL (mmc.source_name, NVL (p_source, 'XXYYRR'))
                         ) NOT IN
                               (NVL (UPPER (mse.source_exception1), 'AXBYSS'),
                                NVL (UPPER (mse.source_exception2), 'AXBYSS'),
                                NVL (UPPER (mse.source_exception3), 'AXBYSS'),
                                NVL (UPPER (mse.source_exception4), 'AXBYSS'),
                                NVL (UPPER (mse.source_exception5), 'AXBYSS'),
                                NVL (UPPER (mse.source_exception6), 'AXBYSS'),
                                NVL (UPPER (mse.source_exception7), 'AXBYSS'),
                                NVL (UPPER (mse.source_exception8), 'AXBYSS'),
                                NVL (UPPER (mse.source_exception9), 'AXBYSS'),
                                NVL (UPPER (mse.source_exception10), 'AXBYSS')) --)
                                                                               ) -- CR #22749, end
                    --chandra 12,21,2011 fixed this rule
                    AND UPPER(NVL (mmc.controller_instance,
                                   NVL (p_controller_instance, 'XXYYCI'))) =
                          UPPER (NVL (p_controller_instance, 'XXYYCI'))
                    --chandra 12,21,2011 fixed this rule
                    AND UPPER (
                          NVL (mmc.cont_name, NVL (p_cont_name, 'XXYYSS'))
                       ) = UPPER (NVL (p_cont_name, 'XXYYSS'))
                    --chandra 12,21,2011 fixed this rule
					AND UPPER (
                          NVL (mmc.field5, NVL (p_prop_name, 'XXYYSF'))
                       ) = UPPER (NVL (p_prop_name, 'XXYYSF'))
                    --Ivy 11,12,2020 fixed this rule -SPA 1873, --- field 5
                    AND UPPER (
                          NVL (mmc.advisory_type, NVL (p_adv_type, 'AXBYCZ'))
                       ) = UPPER (NVL (p_adv_type, 'AXBYCZ'))
                    --As per CR 20244,Added By Mritunjay on 29-March-2012
                    AND UPPER (
                          NVL (mmc.advisory_value, NVL (p_adv_value, 'AXBYCZ'))
                       ) = UPPER (NVL (p_adv_value, 'AXBYCZ'))
                    AND UPPER (mmc.approved_ind) =g_rule_approved_status
                    AND UPPER (
                          NVL (mmc.source_type, NVL (p_field14, 'AXBYWZ'))
                       ) = UPPER (NVL (p_field14, 'AXBYWZ'))
                    AND p_time_revd BETWEEN mmc.start_date
                                        AND  NVL (
                                                              mmc.end_date,
                                                              (p_time_revd + 1)
                                                           )
                    AND ( (DECODE (week_day_no, NULL, 'N', 'Y') = 'N')
                         OR (DECODE (week_day_no, NULL, 'N', 'Y') = 'Y'
                             AND DECODE (
                                   INSTR (week_day_no,
                                          TO_CHAR (p_time_revd, 'D')),
                                   0,
                                   'No',
                                   'Yes'
                                ) = 'Yes'))
                    AND (p_time_revd >=
                            TO_DATE (
                               TO_CHAR (p_time_revd, 'MM/DD/YYYY') || ' '
                               || (CASE
                                      WHEN FLOOR (NVL (start_time, 0) / 3600) <
                                              10
                                      THEN
                                         '0'
                                         || FLOOR (NVL (start_time, 0) / 3600)
                                      ELSE
                                         TO_CHAR (
                                            FLOOR (NVL (start_time, 0) / 3600)
                                         )
                                   END
                                   || ':'
                                   || CASE
                                         WHEN MOD (FLOOR(NVL (start_time, 0)/60), 60) <
                                                 10
                                         THEN
                                            '0'
                                            || MOD (FLOOR(NVL (start_time, 0)/60), 60)
                                         ELSE
                                            TO_CHAR (
                                               MOD (FLOOR(NVL (start_time, 0)/60), 60)
                                            )
                                      END
                                   || ':00'),
                               'MM/DD/YYYY HH24:MI:SS'
                            )
                         AND p_time_revd <=
                               TO_DATE (
                                  TO_CHAR (p_time_revd, 'MM/DD/YYYY') || ' '
                                  || (CASE
                                         WHEN FLOOR (NVL (end_time, 86399) / 3600) <
                                                 10
                                         THEN
                                            '0'
                                            || FLOOR (
                                                  NVL (end_time, 86399) / 3600
                                               )
                                         ELSE
                                            TO_CHAR(FLOOR(NVL (end_time, 86399)
                                                          / 3600))
                                      END
                                      || ':'
                                      || CASE
                                            WHEN MOD (FLOOR(NVL (end_time, 86399)/60), 60) <
                                                    10
                                            THEN
                                               '0'
                                               || MOD (FLOOR(NVL (end_time, 86399)/60),
                                                       60)
                                            ELSE
                                               TO_CHAR(MOD (
                                                          FLOOR(NVL (end_time, 86399)/60),
                                                          60
                                                       ))
                                         END
                                      || ':59'),
                                  'MM/DD/YYYY HH24:MI:SS'
                               ))
         ORDER BY   priority ASC, mmc.modified_on DESC;

      v_start_date           DATE;
      v_message              VARCHAR2 (2000);
      --v_count                NUMBER          := 0;
      v_cb_disregard_id      VARCHAR2 (200);
      --chandra sep,2018 salesforce update
      v_site_id              VARCHAR2(18); --Added By Mritunjay Sinha on Oct,09 2012 As per CR 22748
      v_site_found           VARCHAR2 (2) := 'N';
   --v_dis_found            VARCHAR2 (2) := 'N';

   BEGIN
       DBMS_OUTPUT.put_line ('starting procedure CHK_DISREGARD_ALARMS');
       g_info_msz :=  'starting procedure CHK_DISREGARD_ALARMS';

       WRITE_LOG_FILE(file_name => v_file_name
                       ,info     => g_info_msz
                       ,o_return_status  => v_return_status
                     ) ;

      FOR r_disreg_rec IN c_disregard_alm
      LOOP

         IF v_site_found = 'N' --Modified as per CR 23701 by Mritunjay Sinha on November,30 2012
         THEN
            --
            v_start_date := r_disreg_rec.start_date;
            v_cb_disregard_id := r_disreg_rec.action_id;
            v_site_id := r_disreg_rec.sf_site_id;
            o_disregard_msg := r_disreg_rec.MESSAGE;
            o_disregard_flag := 'D';
            o_cb_disregard_id := v_cb_disregard_id;
         ELSE
            v_site_found := 'X';
            EXIT WHEN v_site_found = 'X';
         END IF;
         v_site_found := 'Y';
      END LOOP;

      IF o_disregard_flag IS NULL
      THEN
         o_disregard_flag := 'X';
      END IF;
      --DBMS_OUTPUT.put_line('o_threshold_reached'||o_threshold_reached);
      o_return_status := 'S';
      DBMS_OUTPUT.put_line ('o_return_status' || o_return_status);
   	--
    EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         DBMS_OUTPUT.put_line('NO DISREGARD SETUP IS DEFINED FOR THE CUSTOMER '
                              || p_cust_id
                              || 'AND SITE '
                              || p_site_id);

          g_info_msz :=    'NO DISREGARD SETUP IS DEFINED FOR THE CUSTOMER '
                              || p_cust_id
                              || 'AND SITE '
                              || p_site_id;

          WRITE_LOG_FILE(file_name => v_file_name
                              ,info     => g_info_msz
                              ,o_return_status  => v_return_status
                     ) ;
         o_disregard_flag := 'X';
      WHEN OTHERS
      THEN
         --
         DBMS_OUTPUT.put_line ( sqlerrm );
         DBMS_OUTPUT.put_line ( 'INFO:-  Unable to Disregard Auto Process the Alarm ID '||p_alarm_id);
          g_info_msz :=    ' Unable to Disregard Auto Process the Alarm ID '||p_alarm_id;

          WRITE_LOG_FILE(file_name => v_file_name
                              ,info     => g_info_msz
                              ,o_return_status  => v_return_status
                     ) ;

         o_return_status := 'E';
           SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'SF Auto processed PL/SQL',
          p_error_message => ' Unable to Disregard Auto Process the Alarm ID '||p_alarm_id,
              p_error_code=>'20012'
          );
   --
   END chk_disregard_alarms;

   --
   --
   --Repeat Alarm Cursor
   --chandra sep,2018 salesforce update
   PROCEDURE check_repeat_alarms (p_alarm_id              IN     NUMBER,
                                  p_cust_id               IN     VARCHAR2,
                                  p_site_id               IN     VARCHAR2,
                                  p_desc                  IN     VARCHAR2,
                                  p_source                IN     VARCHAR2,
                                  p_controller_instance   IN     VARCHAR2,
                                  p_cont_name             IN     VARCHAR2,
                                  p_time_revd             IN     DATE,
                                  p_time_occr             IN     DATE,
                                  o_repeat_msg               OUT VARCHAR2,
                                  o_repeat_flag              OUT VARCHAR2,
                                  o_return_status            OUT VARCHAR2)
   IS
      CURSOR c_rep_alm
      IS
         SELECT  mmc.sf_cust_id,
                 mmc.MESSAGE,
                 mmc.repeat_duplicate_timer,
                 mmc.start_date,
                 Mmc.End_Date
         From  sf_Message_Config Mmc
         Where Mmc.Message_Type = G_Repeat_Message_Type
           AND UPPER (mmc.approved_ind) =g_rule_approved_status
           AND mmc.sf_cust_id = p_cust_id;
   --
   BEGIN
      o_repeat_msg :='Auto Processed - Repeat';
      FOR r_rep_rec IN c_rep_alm
      LOOP
      FOR R_ALARM IN (
      SELECT   mna.alarm_id
           FROM    sf_norm_alarm mna
          WHERE
                  --Modified By Mritunjay on April,27
                  mna.sf_site_id = p_site_id
                  --Modified By Mritunjay on April,26
                  AND UPPER (NVL (mna.descr, 'XXYYZZ')) =
                        UPPER (NVL (p_desc, 'XXYYZZ'))
                  --Modified By Mritunjay on April,26
                  AND UPPER (NVL (mna.SOURCE, 'XXYYRR')) =
                        UPPER (NVL (p_source, 'XXYYRR'))
                  AND UPPER (mna.current_status) = 'RESOLVED' --Modified By Mritunjay
                  AND mna.alarm_id < p_alarm_id
                  AND mna.time_occurred <> p_time_occr
                  AND ( (p_time_revd - mna.time_received) * 86400) <=
                        r_rep_rec.repeat_duplicate_timer --Modified By Mritunjay As Per CR 19405
                  --
                  AND p_time_revd BETWEEN r_rep_rec.start_date
                                      AND  NVL (
                                                            r_rep_rec.end_date,
                                                            (p_time_revd + 1)
                                                         )
      )
      LOOP
       FOR R_RECORDS IN (
         SELECT 'REPEAT'
         FROM SF_ALARM_ACTION_STATUS_NOTES masn
         WHERE masn.alarm_id=R_ALARM.alarm_id
               AND masn.sf_action_id IN
                           ('Contacted',
                            'Left Msg - Resolved',
                            'Contacted Resolved',
                            'Create Work Order',
                            g_callwowsolved_action_id,
                            g_contactedfursolved_action_id,
                            g_storesolved_action_id
                            )
                            )
         LOOP
         o_repeat_msg := r_rep_rec.MESSAGE;
         o_repeat_flag := 'R';
         EXIT WHEN o_repeat_flag='R';
         END LOOP;
      --
      EXIT WHEN o_repeat_flag='R';
      END LOOP;
      --
      EXIT WHEN o_repeat_flag='R';
      END LOOP;

      --
      IF o_repeat_flag IS NULL
      THEN
         o_repeat_flag := 'X';
      END IF;

      o_return_status := 'S';
   --
   EXCEPTION
      WHEN OTHERS
      THEN
         --
          DBMS_OUTPUT.put_line('INFO :- Unable to Repeat Auto Process the Alarm ID '|| p_alarm_id);

          g_info_msz :=    'Unable to Repeat Auto Process the Alarm ID '|| p_alarm_id;

         o_return_status := 'E';
             SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'Auto processed PL/SQL',
          p_error_message => 'Unable to Repeat Auto Process the Alarm ID '|| p_alarm_id,
              p_error_code=>'20011'
          );
   --
   END check_repeat_alarms;

   --
   --Duplicate Alarm procedure
   --chandra sep,2018 salesforce update
   PROCEDURE check_duplicate_alarms (p_alarm_id              IN     NUMBER,
                                     p_cust_id               IN     VARCHAR2,
                                     p_site_id               IN     VARCHAR2,
                                     p_desc                  IN     VARCHAR2,
                                     p_source                IN     VARCHAR2,
                                     p_controller_instance   IN     VARCHAR2,
                                     p_cont_name             IN     VARCHAR2,
                                     p_time_revd             IN     DATE,
                                     p_time_occr             IN     DATE,
                                     o_duplicate_msg            OUT VARCHAR2,
                                     o_duplicate_flag           OUT VARCHAR2,
                                     o_return_status            OUT VARCHAR2)
   IS
     CURSOR c_dul_alm
       IS
         SELECT  mmc.sf_cust_id,
                 mmc.MESSAGE,
                 mmc.IS_EXCLUDE,
                 mmc.repeat_duplicate_timer,
                 mmc.start_date,
                 Mmc.End_Date
         From  sf_Message_Config Mmc
         Where Mmc.Message_Type = g_duplicate_Message_Type
         AND mmc.sf_cust_id = p_cust_id
         AND UPPER (mmc.approved_ind) =g_rule_approved_status
         AND p_time_revd BETWEEN Mmc.start_date
                                      AND  NVL (
                                                            Mmc.end_date,
                                                            (p_time_revd + 1)
                                                         );
   v_rule_found           VARCHAR2 (2) := 'N';
   BEGIN
      o_duplicate_msg := 'Auto Processed - Duplicate';
      FOR r_dul_rec IN c_dul_alm
        LOOP
          v_rule_found := 'Y';
          IF r_dul_rec.IS_EXCLUDE = 'false' THEN
            FOR D_ALARM IN(
              SELECT   mna.alarm_id
              FROM   sf_norm_alarm mna
              WHERE
                  mna.sf_site_id = p_site_id
                  --Modified By Mritunjay on April,26
                  AND UPPER (NVL (mna.descr, 'XXYYZZ')) =
                        UPPER (NVL (p_desc, 'XXYYZZ'))
                  --Modified By Mritunjay on April,26
                  AND UPPER (NVL (mna.SOURCE, 'XXYYRR')) =
                        UPPER (NVL (p_source, 'XXYYRR'))
                  AND mna.alarm_id < p_alarm_id
                  AND mna.time_occurred = p_time_occr
                  AND ( (p_time_revd - mna.time_received) * 86400) >=r_dul_rec.repeat_duplicate_timer
               )

          LOOP
            o_duplicate_flag := 'DUP';
            --
            EXIT WHEN o_duplicate_flag='DUP';
          END LOOP;
        END IF;
      END LOOP;
      IF v_rule_found = 'N' THEN
           FOR D_ALARM IN(
             SELECT   mna.alarm_id
             FROM   sf_norm_alarm mna
             WHERE
                  mna.sf_site_id = p_site_id
                  --Modified By Mritunjay on April,26
                  AND UPPER (NVL (mna.descr, 'XXYYZZ')) =
                        UPPER (NVL (p_desc, 'XXYYZZ'))
                  --Modified By Mritunjay on April,26
                  AND UPPER (NVL (mna.SOURCE, 'XXYYRR')) =
                        UPPER (NVL (p_source, 'XXYYRR'))
                  AND mna.alarm_id < p_alarm_id
                  AND mna.time_occurred = p_time_occr
                  AND ( (p_time_revd - mna.time_received) * 86400) >=64800
             )
        LOOP
        o_duplicate_flag := 'DUP';
        --
        EXIT WHEN o_duplicate_flag='DUP';
        END LOOP;
      END IF;
      --
      IF o_duplicate_flag IS NULL
      THEN
         o_duplicate_flag := 'X';
      END IF;

      o_return_status := 'S';
   --
   EXCEPTION
      WHEN OTHERS
      THEN
         --
         DBMS_OUTPUT.put_line('INFO :- Unable to Duplicate Auto Process the Alarm ID  '|| p_alarm_id);

            g_info_msz :=    ' Unable to Duplicate Auto Process the Alarm ID '||p_alarm_id;

          WRITE_LOG_FILE(file_name => v_file_name
                              ,info     => g_info_msz
                              ,o_return_status  => v_return_status
                     ) ;

         o_return_status := 'E';
         SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'Auto processed PL/SQL',
          p_error_message => ' Unable to Duplicate Auto Process the Alarm ID '||p_alarm_id,
              p_error_code=>'20010'
          );
   --
   END check_duplicate_alarms;

   --
   -- Global Message
   --
   /*PROCEDURE Global_Message (  p_alarm_id             IN NUMBER
                              ,p_cust_id              IN NUMBER
                              ,p_site_id              IN NUMBER
                              ,p_time_revd            IN DATE
                              ,o_global_msg           OUT VARCHAR2
                              ,o_return_status        OUT VARCHAR2
                               ) IS
    CURSOR   cur_global_message IS
    SELECT   mmc.message
    FROM     sf_message_config  mmc ,
             mss_message_type_ref mmt
    WHERE    mmc.sf_cust_id                 = p_cust_id
     AND     NVL(mmc.sf_site_id,-9999)      = NVL(p_site_id,-9999)
    AND      mmc.message_type_id         = mmt.message_type_id
    AND      mmt.message_type_code       ='GLOBAL MSG'
    AND      p_time_revd BETWEEN mmc.start_date AND nvl(mmc.end_date,(p_time_revd+1))
    ORDER BY mmc.modified_on DESC;

   --Jith Clarify
   -- As per the existing MSS logic, global message is distributed between external attributes 8, 9, and 12.
   -- In MSS R which fields will be loaded with the Global message
   -- For now, I am assigning the value into Comments column of MSS_NORM_ALARM table.

   BEGIN

      FOR c_global_msg IN cur_global_message LOOP
        o_global_msg := c_global_msg.message;
      END LOOP;

      IF o_global_msg IS NOT NULL THEN
        o_return_Status := 'S';
      ELSE
        o_return_Status := 'E';
      END IF;


   END Global_Message;*/
   --
   -- Email Address
   --chandra sep,2018 salesforce update
   PROCEDURE check_email_address (p_alarm_id             IN     NUMBER,
                                  p_cust_id              IN     VARCHAR2,
                                  p_site_id              IN     VARCHAR2,
                                  p_time_revd            IN     DATE,
                                  o_email_delivery_id       OUT VARCHAR2,
                                  o_email_address_flag      OUT VARCHAR2,
                                  o_return_status           OUT VARCHAR2,
                                  o_err_msg                 OUT VARCHAR2)
   IS
      CURSOR rc_email_address
      IS
         SELECT   email_delivery_id
           FROM   sf_alarm_email_delivery maed
          WHERE   maed.sf_cust_id = p_cust_id
                  AND NVL (maed.sf_site_id, -9999) = NVL (p_site_id, -9999)
                  AND p_time_revd BETWEEN maed.start_date
                                      AND  NVL (
                                                            maed.end_date,
                                                            (p_time_revd + 1)
                                                         );
   BEGIN
      --chandra aug,21
      o_email_address_flag := NULL;

      FOR c_email_address IN rc_email_address
      LOOP
         o_email_delivery_id := c_email_address.email_delivery_id;
         o_email_address_flag := 'Y';
      END LOOP;

      o_return_status := 'S';
   EXCEPTION
      WHEN OTHERS
      THEN
         o_err_msg := 'INFO :- unable to check Email Address ' || SUBSTR(SQLERRM,1,1600);
         DBMS_OUTPUT.put_line (o_err_msg);

         WRITE_LOG_FILE(file_name => v_file_name
                              ,info     => o_err_msg
                              ,o_return_status  => v_return_status
                     ) ;


         o_return_status := 'E';
          SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'SF Auto processed PL/SQL',
          p_error_message => 'INFO :- unable to check Email Address ' || SUBSTR(SQLERRM,1,1600),
              p_error_code=>'20009'
          );
   END check_email_address;

   --chandra sep,2018 salesforce update
   PROCEDURE check_include_found (p_alarm_id              IN     NUMBER,
                                  p_cust_id               IN     VARCHAR2,
                                  p_site_id               IN     VARCHAR2,
                                  p_time_revd             IN     DATE,
                                  p_controller_instance   IN     VARCHAR2,
                                  p_controller            IN     VARCHAR2,
                                  p_source                IN     VARCHAR2,
                                  p_desc                  IN     VARCHAR2,
                                  p_adv_type              IN     VARCHAR2,
                                  p_adv_value             IN     VARCHAR2,
                                  o_include_msg              OUT VARCHAR2,
                                  o_include_flag             OUT VARCHAR2,
                                  o_return_status            OUT VARCHAR2,
                                  o_cb_include_action        OUT VARCHAR2)
   IS

      --chandra sep,2018 salesforce update
      CURSOR rc_include_found
      IS
         SELECT   'INCLUDE FOUND' AS c1, mmc.MESSAGE, '1' AS action1 -- Mritunjay August,28,2012 CB maintenance
           FROM   sf_message_config mmc
          WHERE       mmc.sf_cust_id = p_cust_id
                  And NVL(Mmc.sf_site_Id,P_Site_Id) = P_Site_Id
                  AND mmc.message_type = g_include_message_type
                  --        AND UPPER (mmc.approved_ind) NOT IN ('N','R')               --As per CR 22123,Added By Mritunjay on 21-August-2012
                  AND UPPER(NVL (mmc.controller_instance,
                                 NVL (p_controller_instance, 'AXBYCZ'))) =
                        UPPER (NVL (p_controller_instance, 'AXBYCZ'))
                  AND UPPER (
                        NVL (mmc.cont_name, NVL (p_controller, 'AXBYCZ'))
                     ) = UPPER (NVL (p_controller, 'AXBYCZ'))
                  AND UPPER (NVL (mmc.source_name, NVL (p_source, 'AXBYCZ'))) =
                        UPPER (NVL (p_source, 'AXBYCZ'))
                  AND UPPER (NVL (mmc.description, NVL (p_desc, 'AXBYCZ'))) =
                        UPPER (NVL (p_desc, 'AXBYCZ'))
                  AND UPPER (
                          NVL (mmc.advisory_type, NVL (p_adv_type, 'AXBYCZ'))
                       ) = UPPER (NVL (p_adv_type, 'AXBYCZ'))
                  AND UPPER (mmc.approved_ind) =g_rule_approved_status
                  AND UPPER (
                          NVL (mmc.advisory_value, NVL (p_adv_value, 'AXBYCZ'))
                       ) = UPPER (NVL (p_adv_value, 'AXBYCZ'))
                  AND p_time_revd BETWEEN mmc.start_date
                                      AND  NVL (
                                                            mmc.end_date,
                                                            (p_time_revd + 1)
                                                         )
                  --Modified on 01,24,2012 by  Mritunjay
                  AND ( (DECODE (week_day_no, NULL, 'N', 'Y') = 'N')
                       OR (DECODE (week_day_no, NULL, 'N', 'Y') = 'Y'
                           AND DECODE (
                                 INSTR (week_day_no,
                                        TO_CHAR (p_time_revd, 'D')),
                                 0,
                                 'No',
                                 'Yes'
                              ) = 'Yes'))              --Modified By Mritunjay
                  AND (p_time_revd >=
                            TO_DATE (
                               TO_CHAR (p_time_revd, 'MM/DD/YYYY') || ' '
                               || (CASE
                                      WHEN FLOOR (NVL (start_time, 0) / 3600) <
                                              10
                                      THEN
                                         '0'
                                         || FLOOR (NVL (start_time, 0) / 3600)
                                      ELSE
                                         TO_CHAR (
                                            FLOOR (NVL (start_time, 0) / 3600)
                                         )
                                   END
                                   || ':'
                                   || CASE
                                         WHEN MOD (FLOOR(NVL (start_time, 0)/60), 60) <
                                                 10
                                         THEN
                                            '0'
                                            || MOD (FLOOR(NVL (start_time, 0)/60), 60)
                                         ELSE
                                            TO_CHAR (
                                               MOD (FLOOR(NVL (start_time, 0)/60), 60)
                                            )
                                      END
                                   || ':00'),
                               'MM/DD/YYYY HH24:MI:SS'
                            )
                         AND p_time_revd <=
                               TO_DATE (
                                  TO_CHAR (p_time_revd, 'MM/DD/YYYY') || ' '
                                  || (CASE
                                         WHEN FLOOR (NVL (end_time, 86399) / 3600) <
                                                 10
                                         THEN
                                            '0'
                                            || FLOOR (
                                                  NVL (end_time, 86399) / 3600
                                               )
                                         ELSE
                                            TO_CHAR(FLOOR(NVL (end_time, 86399)
                                                          / 3600))
                                      END
                                      || ':'
                                      || CASE
                                            WHEN MOD (FLOOR(NVL (end_time, 86399)/60), 60) <
                                                    10
                                            THEN
                                               '0'
                                               || MOD (FLOOR(NVL (end_time, 86399)/60),
                                                       60)
                                            ELSE
                                               TO_CHAR(MOD (
                                                          FLOOR(NVL (end_time, 86399)/60),
                                                          60
                                                       ))
                                         END
                                      || ':59'),
                                  'MM/DD/YYYY HH24:MI:SS'
                               ))
         UNION
         SELECT   'SITE FOUND' AS c1, mmc.MESSAGE, mmc.action_id AS action1 -- chandra apr,12,2012 CB maintenance
           FROM   sf_message_config mmc
          WHERE       mmc.sf_cust_id = p_cust_id
                  And NVL(Mmc.sf_site_Id,P_Site_Id) = P_Site_Id
                  AND  mmc.message_type =g_include_message_type
                  --AND mmc.approved_ind NOT IN ('N', 'R') --Modified as per CR# 22123 on 21,August 2012
                  AND p_time_revd BETWEEN mmc.start_date
                                      AND  NVL (
                                                            mmc.end_date,
                                                            (p_time_revd + 1)
                                                         )
                   AND ( (DECODE (week_day_no, NULL, 'N', 'Y') = 'N')
                       OR (DECODE (week_day_no, NULL, 'N', 'Y') = 'Y'
                           AND DECODE (
                                 INSTR (week_day_no,
                                        TO_CHAR (p_time_revd, 'D')),
                                 0,
                                 'No',
                                 'Yes'
                              ) = 'Yes'))              --Modified By Mritunjay
                  AND (p_time_revd >=
                            TO_DATE (
                               TO_CHAR (p_time_revd, 'MM/DD/YYYY') || ' '
                               || (CASE
                                      WHEN FLOOR (NVL (start_time, 0) / 3600) <
                                              10
                                      THEN
                                         '0'
                                         || FLOOR (NVL (start_time, 0) / 3600)
                                      ELSE
                                         TO_CHAR (
                                            FLOOR (NVL (start_time, 0) / 3600)
                                         )
                                   END
                                   || ':'
                                   || CASE
                                         WHEN MOD (FLOOR(NVL (start_time, 0)/60), 60) <
                                                 10
                                         THEN
                                            '0'
                                            || MOD (FLOOR(NVL (start_time, 0)/60), 60)
                                         ELSE
                                            TO_CHAR (
                                               MOD (FLOOR(NVL (start_time, 0)/60), 60)
                                            )
                                      END
                                   || ':00'),
                               'MM/DD/YYYY HH24:MI:SS'
                            )
                         AND p_time_revd <=
                               TO_DATE (
                                  TO_CHAR (p_time_revd, 'MM/DD/YYYY') || ' '
                                  || (CASE
                                         WHEN FLOOR (NVL (end_time, 86399) / 3600) <
                                                 10
                                         THEN
                                            '0'
                                            || FLOOR (
                                                  NVL (end_time, 86399) / 3600
                                               )
                                         ELSE
                                            TO_CHAR(FLOOR(NVL (end_time, 86399)
                                                          / 3600))
                                      END
                                      || ':'
                                      || CASE
                                            WHEN MOD (FLOOR(NVL (end_time, 86399)/60), 60) <
                                                    10
                                            THEN
                                               '0'
                                               || MOD (FLOOR(NVL (end_time, 86399)/60),
                                                       60)
                                            ELSE
                                               TO_CHAR(MOD (
                                                          FLOOR(NVL (end_time, 86399)/60),
                                                          60
                                                       ))
                                         END
                                      || ':59'),
                                  'MM/DD/YYYY HH24:MI:SS'
                               ));

      v_include_found   VARCHAR2 (100);
      v_site_found      VARCHAR2 (100);
   BEGIN
      v_include_found := NULL;
      v_site_found := NULL;
      o_include_flag := 'X';

      FOR c_inc_found IN rc_include_found
      LOOP
         IF c_inc_found.c1 = 'INCLUDE FOUND'
         THEN
            v_include_found := c_inc_found.c1;
            o_include_msg := c_inc_found.MESSAGE;
         ELSIF c_inc_found.c1 = 'SITE FOUND'
         THEN
            v_site_found := c_inc_found.c1;
            o_include_msg := c_inc_found.MESSAGE;
            o_cb_include_action := c_inc_found.action1; -- chandra apr,12,2012 CB maintenance
         END IF;
      END LOOP;

      DBMS_OUTPUT.put_line (v_include_found);
      DBMS_OUTPUT.put_line (v_site_found);

      IF v_include_found IS NULL AND v_site_found IS NOT NULL
      THEN
         DBMS_OUTPUT.put_line ('Include Match Not Found, So Disregard this Alarm ');

           g_info_msz :='Include Match Not Found, So Disregard this Alarm  '||p_alarm_id;

          WRITE_LOG_FILE(file_name => v_file_name
                              ,info     => g_info_msz
                              ,o_return_status  => v_return_status
                     ) ;

         o_include_flag := 'N';
      END IF;

      o_return_status := 'S';
   EXCEPTION
      WHEN OTHERS
      THEN
         o_return_status := 'E';
         o_include_flag := 'X';
        DBMS_OUTPUT.put_line ('INFO :- check_include_found' || SUBSTR(SQLERRM,1,1600));
     g_info_msz :=   'INFO :- check_include_found' || SUBSTR(SQLERRM,1,1600);

          WRITE_LOG_FILE(file_name => v_file_name
                              ,info     => g_info_msz
                              ,o_return_status  => v_return_status
                     ) ;
                      SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'Auto processed PL/SQL',
          p_error_message => 'INFO :- check_include_found' || SUBSTR(SQLERRM,1,1600),
              p_error_code=>'20008'
          );

   END check_include_found;

   --Modified By Mritunjay
   --Threshhold
   --
   /* PROCEDURE check_threshhold (p_alarm_id            IN     NUMBER,
                                p_cust_id             IN     NUMBER,
                                p_site_id             IN     NUMBER,
                                p_time_revd           IN     DATE,
                                o_threshold_msg          OUT VARCHAR2,
                                o_threshold_reached      OUT VARCHAR2,
                                o_return_status          OUT VARCHAR2)
    IS
       --
       CURSOR alm_thresh_msg
       IS
            SELECT   COUNT (mmc.MESSAGE) threshold_value, mmc.MESSAGE
              FROM   sf_message_config mmc,
                     mss_message_type_ref mmt,
                     sf_norm_alarm mna
             WHERE       mmc.cust_id = p_cust_id
                     AND NVL (mmc.site_id, -9999) = NVL (p_site_id, -9999)
                     AND mmt.message_type_code = 'THRESHOLD'
                     AND mmt.message_type_id = mmc.message_type_id
                     -- AND UPPER (mmc.approved_ind) NOT IN ('N','R')                                                           --As per CR 22123,Added By Mritunjay on

21-August-2012
                     AND mmc.cust_id = mna.cust_id
                     AND NVL (mmc.site_id, -9999) = NVL (mna.site_id, -9999)
                     AND mna.alarm_id < p_alarm_id
                     AND p_time_revd BETWEEN get_timezones (mmc.start_date)
                                         AND  get_timezones(NVL (
                                                               mmc.end_date,
                                                               (p_time_revd + 1)
                                                            ))
                     AND TRUNC (get_timezones (mna.time_received)) BETWEEN TRUNC(get_timezones(mmc.start_date))
                                                                       AND  TRUNC(get_timezones(mmc.end_date))
          GROUP BY   mmc.MESSAGE;

       --
       v_threshold_count      NUMBER;
       v_curr_threshold       NUMBER;
       mod_threshold_count    NUMBER;
       v_threshold_setup_yn   VARCHAR2 (2);
    --
    BEGIN
         --
         SELECT   mmc.MESSAGE threshold_value
           INTO   v_threshold_count
           FROM   sf_message_config mmc, mss_message_type_ref mmt
          WHERE       mmc.cust_id = p_cust_id
                  AND NVL (mmc.site_id, p_site_id) = NVL (p_site_id, -9999)
                  AND mmt.message_type_code = 'THRESHOLD'
                  AND mmt.message_type_id = mmc.message_type_id
                  AND ROWNUM < 2
       ORDER BY   mmc.created_on DESC;

       --
       DBMS_OUTPUT.put_line(   'Threshold_value configured for the Site'
                            || p_site_id
                            || 'is'
                            || v_threshold_count);
       --
       v_threshold_setup_yn := 'N';

       IF (v_threshold_count IS NULL)
       THEN
          DBMS_OUTPUT.put_line (
             'Threshold is not configured for the Site 123'
          );
          o_threshold_reached := 'X';
       ELSE
          DBMS_OUTPUT.put_line ('inside if');
          DBMS_OUTPUT.put_line ('alarm_count' || p_alarm_id);

          FOR c_alm_thresh_rec IN alm_thresh_msg
          LOOP
             v_curr_threshold := c_alm_thresh_rec.threshold_value;
             o_threshold_msg := c_alm_thresh_rec.MESSAGE;
             v_threshold_setup_yn := 'Y';
          --dbms_output.put_line('Value of alarm_id, cust_id, site_id are '||p_alarm_id||','||p_cust_id||','||p_site_id);
          END LOOP;
       END IF;

       --
       IF v_threshold_setup_yn = 'Y'
       THEN
          mod_threshold_count := MOD (v_curr_threshold, v_threshold_count);

          --dbms_output.put_line('Value of mod_threshold_count is '||mod_threshold_count);
          IF mod_threshold_count = 0
          THEN
             o_threshold_reached := 'Y';
          ELSE
             o_threshold_reached := 'N';
          END IF;
       -- dbms_output.put_line('Value of o_threshold_reached is '||o_threshold_reached);
       ELSE
          o_threshold_reached := 'X'; -- Meaning there is no threshold setup exists in MSS_Message_Config
       END IF;

       o_return_status := 'S';
    EXCEPTION
       WHEN NO_DATA_FOUND
       THEN
          DBMS_OUTPUT.put_line (
             'Threshold is not configured for the Site 123'
          );
          o_threshold_reached := 'X';
       WHEN OTHERS
       THEN
          o_return_status := 'E';
          DBMS_OUTPUT.put_line (
             'Error in Check_Threshold procedure. Error is ' || SQLERRM
          );
    END check_threshhold;*/

   --
   --Added the Check_RTN_Alarms Procedure on 03-April-2012 By Mritunjay Sinha
   --chandra sep,2018 salesforce update
   PROCEDURE check_rtn_alarms (p_alarm_id        IN     NUMBER,
                               p_cust_id         IN     VARCHAR2,
                               p_site_id         IN     VARCHAR2,
                               p_desc            IN     VARCHAR2,
                               p_source          IN     VARCHAR2,
                               p_controller_ins  IN     VARCHAR2,
                               p_rtn_date        IN     DATE,
                               p_time_ocurd      IN     DATE,
                               p_time_revd       IN     DATE,
                               p_point_name      IN     VARCHAR2,
                               o_rtn_msg            OUT VARCHAR2,
                               o_rtn_flag           OUT VARCHAR2,
                               o_return_status      OUT VARCHAR2)
      IS
   v_repeat_cnt  NUMBER(3):=0;
   BATCHNUMBER NUMBER;
   v_alarm_pending_status VARCHAR2 (10);
   v_alarm_sholding_status VARCHAR2 (15);
   CURSOR c_rtn_alm
      IS
      SELECT   mna1.alarm_id,
                         mna1.processed_flag,
                         mna1.current_status,
                         mna1.TIME_RECEIVED,
                         mna1.RTN_DATE,
                         mna1.SR_REFERENCE,
                         mmc1.MESSAGE,
                         mmc1.repeat_duplicate_timer,
                         mmc1.rtn_repeat_count,
                         mmc1.rtn_pending_alarm_flag
                  FROM
                         (select * from (
                            select MESSAGE,
                                   repeat_duplicate_timer,
                                   rtn_repeat_count,
                                   rtn_pending_alarm_flag,
                                   sf_cust_id,
                                   sf_site_id,
                                   ROW_NUMBER() OVER(ORDER BY
                                   CASE WHEN sf_site_id IS NOT NULL
                                   THEN 1
                                   WHEN sf_CUST_LEVEL_ID IS NOT NULL
                                   THEN 2
                                   ELSE
                                        3
                                   END
                                   ASC) rn
                            from
                            sf_message_config
                            where
                            message_type = g_rtn_message_type
                            AND sf_cust_id=p_cust_id
                            AND NVL(sf_site_id,p_site_id)=p_site_id
                             /* chandra, oct,2018 salesforce commented , needs to be reviewed
                            AND NVL(sf_CUST_LEVEL_ID,-2) IN
                                (
                                 SELECT CUST_LEVEL_ID
                                 FROM MSS_CUST_LEVEL_SITE
                                 WHERE SITE_ID=p_site_id
                                 UNION
                                 SELECT -2 AS CUST_LEVEL_ID
                                 FROM DUAL
                                )
                                chandra oct,2018 salesforce end */
                            AND p_time_revd BETWEEN
                                                    start_date

                                             AND  NVL (
                                                                   end_date,
                                                                   (p_time_revd
                                                                    + 1)
                                                                )
                            )
                           where rn<2
                          ) mmc1,
                         sf_norm_alarm mna1
                 WHERE
                         mmc1.sf_cust_id = mna1.sf_cust_id
                         AND NVL (mmc1.sf_site_id, mna1.sf_site_id) = mna1.sf_site_id
                         AND mna1.sf_site_id = p_site_id
                         AND UPPER (NVL (mna1.descr, 'XXYYZZ')) =
                               UPPER (NVL (p_desc, 'XXYYZZ'))
                         AND UPPER (NVL (mna1.SOURCE, 'XXYYRR')) =
                               UPPER (NVL (p_source, 'XXYYRR'))
                         AND UPPER (NVL (mna1.CONTROLLER_INSTANCE, 'XXYYSS')) =
                               UPPER (NVL (p_controller_ins, 'XXYYSS'))
                         AND UPPER (NVL (mna1.FIELD5, 'XXYYTT')) =
                               UPPER (NVL (p_point_name, 'XXYYTT'))
                         AND mna1.TIME_OCCURRED=p_time_ocurd
                         AND mna1.processed_flag IN ('P','N','PQ')
                         AND mna1.alarm_id < p_alarm_id
                         AND NVL(mna1.current_status,'RAWALARM') <> 'Resolved';
   --Auto process Original Alarm
   PROCEDURE auto_process_org_alm(p_alarm_id IN NUMBER,
                               p_rtn_alm_id IN NUMBER,
                               batch_num    IN NUMBER
                               )
     IS
     v_alarm_action_id NUMBER(38);
     BEGIN
       --write log
       INSERT INTO   SF_ALARM_ACTION_STATUS_NOTES (        sf_alarm_action_id,
       							    techn_level_id,
                                                            lifecycle_number,
                                                            sf_action_id,
                                                            sf_status_id,
                                                            sf_site_contact_id,
                                                            notes,
                                                            created_by,
                                                            created_on,
                                                            modified_by,
                                                            modified_on,
                                                            program_app_id,
                                                            request_id,
                                                            version_number,
                                                            alarm_id,
                                                            log_type
                                                      )
                       VALUES(SF_ALARM_ACT_STAT_NOTES_SQ.nextval,
                       	      1,
                              1,
                              g_rtn_disregard_action_id,
                              g_rtn_disregard_status_id,
                              NULL,
                              'RTN Disregarded for the alarm_id ='||p_rtn_alm_id,
                              g_created_by,
                              get_timezones_SERVER_TO_GMT (SYSDATE),
                              g_modified_by,
                              get_timezones_SERVER_TO_GMT (SYSDATE),
                              1,
                              1,
                              1,
                              p_alarm_id,
                              1)
                 RETURNING   sf_alarm_action_id INTO   v_alarm_action_id;

       --update alarm status
       UPDATE sf_norm_alarm
       SET current_status='Resolved',
           modified_by=g_modified_by,
           SR_REFERENCE=null, --chandra, oct,2018 salesforce DECODE(SR_REFERENCE,NULL,batch_num,SR_REFERENCE),
           modified_on=get_timezones_server_to_gmt (SYSDATE)
           --chandra sep,2018 salesforce update
           --current_alarm_action_id=v_alarm_action_id,
           --last_alarm_action_id= v_alarm_action_id
       WHERE alarm_id=p_alarm_id;
       COMMIT;
       EXCEPTION
       WHEN OTHERS
       THEN
          DBMS_OUTPUT.put_line('INFO :- Auto process Original Alarm :-  '|| SUBSTR(SQLERRM,1,1600));
          g_info_msz :='In check_rtn_alarms procedure Info is :-'|| SUBSTR(SQLERRM,1,1600) ;
          WRITE_LOG_FILE(file_name => v_file_name
                              ,info     => g_info_msz
                              ,o_return_status  => v_return_status
                     ) ;
     END auto_process_org_alm;
    --Present alarm with RTN alarm color coding
    PROCEDURE p_present_org_alm_rtnDate(p_alarm_id NUMBER,
                                        p_rtn_date DATE
                                        )
     IS
     BEGIN
       update sf_norm_alarm set rtn_date=p_rtn_date where alarm_id=p_alarm_id;
       COMMIT;
     EXCEPTION
     WHEN OTHERS
     THEN
       DBMS_OUTPUT.put_line('INFO :- Present alarm with RTN alarm color coding :-  '|| SUBSTR(SQLERRM,1,1600));
       g_info_msz :='In check_rtn_alarms procedure Info is :-'|| SUBSTR(SQLERRM,1,1600) ;
       WRITE_LOG_FILE(file_name => v_file_name
                              ,info     => g_info_msz
                              ,o_return_status  => v_return_status
                     ) ;
     END p_present_org_alm_rtnDate;

    FUNCTION GET_BATCHNUMBER(
    USER_NAME VARCHAR2)
    RETURN NUMBER
    IS
      BATCH_ID NUMBER;
    BEGIN
      INSERT
      INTO
        MSS_SERVICE_REQUEST
        (
          created_by,
          CREATED_ON,
          MODIFIED_BY,
          MODIFIED_ON
        )
        VALUES
        (
          USER_NAME,
          GET_TIMEZONES(SYSDATE),
          USER_NAME,
          GET_TIMEZONES(SYSDATE)
        )
      RETURNING
        SERVICE_REQUEST_ID
      INTO
        BATCH_ID;
      RETURN batch_id;
   END;

   BEGIN
      -- The RTN alarm always is resolved
      o_rtn_flag := 'R';
      o_return_status := 'S';
	    o_rtn_msg := 'Auto Processed - RTN';

      v_alarm_pending_status := 'Pending';
      v_alarm_sholding_status := 'System Holding';
      --Handle original alarms
      FOR r_rtn_rec IN c_rtn_alm
      LOOP
         --If the Original alarm's status is unassigned
         IF r_rtn_rec.current_status = 'Unassigned' THEN
           --within time window
           IF r_rtn_rec.TIME_RECEIVED BETWEEN (p_time_revd- (r_rtn_rec.repeat_duplicate_timer/ 86400)) AND p_time_revd THEN
                IF v_repeat_cnt = 0 THEN
                   BEGIN
                     SELECT   decode(count(distinct mna.TIME_OCCURRED),NULL,0,count(distinct mna.TIME_OCCURRED)) into v_repeat_cnt
                     FROM  sf_norm_alarm mna
                     WHERE mna.sf_site_id = p_site_id  -- chandra sep,2018 salesforce update
                           AND UPPER (NVL (mna.descr, 'XXYYZZ')) =
                                 UPPER (NVL (p_desc, 'XXYYZZ'))
                           AND UPPER (NVL (mna.SOURCE, 'XXYYRR')) =
                                 UPPER (NVL (p_source, 'XXYYRR'))
                           AND UPPER (NVL (mna.CONTROLLER_INSTANCE, 'XXYYRR')) =
                               UPPER (NVL (p_controller_ins, 'XXYYRR'))
                           AND UPPER (NVL (mna.FIELD5, 'XXYYTT')) =
                               UPPER (NVL (p_point_name, 'XXYYTT'))
                           AND mna.processed_flag IN ('P','N','PQ')
                           AND mna.alarm_id < p_alarm_id
                           AND mna.TIME_RECEIVED BETWEEN (p_time_revd
                                                           - (r_rtn_rec.repeat_duplicate_timer
                                                              / 86400))
                                                      AND  p_time_revd;
                       EXCEPTION
                       WHEN NO_DATA_FOUND
                       THEN
                         DBMS_OUTPUT.put_line('INFO :- Present alarm with RTN alarm color coding :-  '|| SUBSTR(SQLERRM,1,1600));
                         g_info_msz :='In check_rtn_alarms procedure Info is :-'|| SUBSTR(SQLERRM,1,1600) ;
                         WRITE_LOG_FILE(file_name => v_file_name
                                                ,info     => g_info_msz
                                                ,o_return_status  => v_return_status
                                       ) ;
                       WHEN OTHERS
                       THEN
                         DBMS_OUTPUT.put_line('INFO :- Present alarm with RTN alarm color coding :-  '|| SUBSTR(SQLERRM,1,1600));
                         g_info_msz :='In check_rtn_alarms procedure Info is :-'|| SUBSTR(SQLERRM,1,1600) ;
                         WRITE_LOG_FILE(file_name => v_file_name
                                                ,info     => g_info_msz
                                                ,o_return_status  => v_return_status
                                       ) ;
                       END;
                END IF;
              --less than repeat count
              IF v_repeat_cnt <= r_rtn_rec.rtn_repeat_count THEN
                 IF r_rtn_rec.SR_REFERENCE IS NULL THEN
                    BATCHNUMBER := GET_BATCHNUMBER('MSSR');
                 END IF;
                 auto_process_org_alm(p_alarm_id => r_rtn_rec.alarm_id,
                                   p_rtn_alm_id => p_alarm_id,
                                   batch_num    =>BATCHNUMBER
                 );

              ELSE
                 p_present_org_alm_rtnDate(
                                            p_alarm_id =>r_rtn_rec.alarm_id,
                                            p_rtn_date =>p_rtn_date
                                           );
              END IF;
           --outside time window
           ELSE
                IF r_rtn_rec.SR_REFERENCE IS NULL THEN
                    BATCHNUMBER := GET_BATCHNUMBER('MSSR');
                END IF;
                auto_process_org_alm(p_alarm_id => r_rtn_rec.alarm_id,
                                   p_rtn_alm_id => p_alarm_id  ,
                                   batch_num    =>BATCHNUMBER
                 );
           END IF;
         END IF;

         --If the Original alarm's status is pending
         IF r_rtn_rec.current_status IN (v_alarm_pending_status,v_alarm_sholding_status)
         THEN
           IF NVL(r_rtn_rec.rtn_pending_alarm_flag,'Y') = 'Y' THEN
                auto_process_org_alm(p_alarm_id => r_rtn_rec.alarm_id,
                                   p_rtn_alm_id => p_alarm_id,
                                   batch_num    =>BATCHNUMBER
                 );
           ELSE
              p_present_org_alm_rtnDate(
                                            p_alarm_id =>r_rtn_rec.alarm_id,
                                            p_rtn_date =>p_rtn_date
                                           );
           END IF;
         END IF;

          --If the Original alarm's status is assigned
         IF r_rtn_rec.current_status = 'Assigned'
         THEN
           p_present_org_alm_rtnDate(
                                            p_alarm_id =>r_rtn_rec.alarm_id,
                                            p_rtn_date =>p_rtn_date
                                           );
         END IF;

          --If the Original alarm's status is null
         IF r_rtn_rec.current_status IS NULL
         THEN
           p_present_org_alm_rtnDate(
                                            p_alarm_id =>r_rtn_rec.alarm_id,
                                            p_rtn_date =>p_rtn_date
                                           );
         END IF;

      END LOOP;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
        DBMS_OUTPUT.put_line('Original Alarm is not found, hence treating the alarm '
                              || p_alarm_id
                              || 'as a Autoprocessed alarm');

          g_info_msz :='Original Alarm is not found, hence treating the alarm '
                              || p_alarm_id
                              || 'as a Autoprocessed alarm';

          WRITE_LOG_FILE(file_name => v_file_name
                              ,info     => g_info_msz
                              ,o_return_status  => v_return_status
                     ) ;
      WHEN OTHERS
      THEN
         --
      DBMS_OUTPUT.put_line('INFO :- In check_rtn_alarms procedure Info is :-  '|| SUBSTR(SQLERRM,1,1600));

          g_info_msz :='In check_rtn_alarms procedure Info is :-'|| SUBSTR(SQLERRM,1,1600) ;

          WRITE_LOG_FILE(file_name => v_file_name
                              ,info     => g_info_msz
                              ,o_return_status  => v_return_status
                     ) ;

         DBMS_OUTPUT.put_line('INFO : Details of RTN Procedure for Alarm ID  '|| p_alarm_id);
      g_info_msz :=       'INFO : Details of RTN Procedure for Alarm ID  '|| p_alarm_id ;

          WRITE_LOG_FILE(file_name => v_file_name
                              ,info     => g_info_msz
                              ,o_return_status  => v_return_status
                     ) ;
         o_return_status := 'E';

          SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'Auto processed PL/SQL',
          p_error_message => 'INFO : Details of RTN Procedure for Alarm ID  '|| p_alarm_id,
              p_error_code=>'20007'
          );
   --
   END check_rtn_alarms;

--Gary Sun 2023-06-19 check auto email rules
PROCEDURE check_auto_email_alarms (p_alm_id            IN     NUMBER,
                                     p_cust_id           IN     VARCHAR2,
                                     p_site_id         IN     VARCHAR2,
                                     p_desc              IN     VARCHAR2,
                                     p_source            IN     VARCHAR2,
                                     p_created_on        IN     DATE,
                                     p_time_received	 IN 	DATE,
                                     o_return_status     OUT VARCHAR2,
                                     o_auto_email_flag     OUT VARCHAR2,
                                     o_auto_email_cdm_cust OUT VARCHAR2
                                     )
   IS
    CURSOR rc_auto_email_rules
       IS
       SELECT AUTO_EMAIL_DESCRIPTION,AUTO_EMAIL_SOURCE,AUTO_EMAIL_CRITERIA
       FROM sf_message_config mmc
       WHERE mmc.sf_cust_id = p_cust_id
             AND mmc.message_type = g_auto_email_Message_Type
             AND p_time_received BETWEEN mmc.start_date
             AND  NVL (mmc.end_date,(p_time_received + 1))
             AND mmc.AUTO_EMAIL_CRITERIA IS NOT NULL
             AND NVL(mmc.sf_site_id,p_site_id) =p_site_id
        ORDER BY DECODE(AUTO_EMAIL_CRITERIA,g_cariteria_description,1,g_cariteria_source,2,g_cariteria_sourceDescr,3,4) ASC;

    CURSOR rc_auto_email_specific_rules
      IS
        SELECT AUTO_EMAIL_DESCRIPTION,AUTO_EMAIL_SOURCE,AUTO_EMAIL_CRITERIA
       FROM sf_message_config mmc
       WHERE mmc.sf_cust_id = p_cust_id
             AND mmc.message_type = g_auto_email_Message_Type
             AND p_time_received BETWEEN mmc.start_date
             AND  NVL (mmc.end_date,(p_time_received + 1))
             AND mmc.AUTO_EMAIL_CRITERIA IS NULL
             AND NVL(mmc.sf_site_id,p_site_id) =p_site_id
             AND AUTO_EMAIL_DESCRIPTION IS NULL
             AND AUTO_EMAIL_SOURCE IS NULL;

    CURSOR c_emailduplicate is
      SELECT
        max(repeat_duplicate_timer) duptimer
    from
      SF_MESSAGE_CONFIG smc
    where
               sf_cust_id = p_cust_id
               and message_type = 'EmailDuplicate'
               and is_exclude = 'true'
    and UPPER (approved_ind) =g_rule_approved_status
               AND get_timezones_GMT_TO_SERVER(p_created_on) BETWEEN smc.start_date AND  NVL ( smc.end_date, (p_created_on + 1) );

     v_dupflag VARCHAR2(1);

     v_delimiter VARCHAR2(2) :=';;';
   BEGIN
    o_auto_email_flag :='N';
    o_auto_email_cdm_cust :='N';
    FOR cdm_cust in (select SYS_CONFIG_VALUE from mss_sys_config msc,sf_customer sc where msc.SYS_CONFIG_VALUE=sc.sf_cust_name and sc.sf_cust_id= p_cust_id
                     and SYS_CONFIG_TYPE_CD='AutoEmailCDM' and SYS_CONFIG_CD='SUPPORT_CUSTOMER')
    LOOP
      o_auto_email_cdm_cust:='Y';
    END LOOP;
    --alarm will be auto email when there is none value of criteria and both source and description are blank.
    FOR r_auto_email_s_rule IN rc_auto_email_specific_rules LOOP
      o_auto_email_flag :='Y';
    END LOOP;
    IF o_auto_email_flag = 'N' THEN
      FOR r_auto_email_rule IN rc_auto_email_rules LOOP
        --check description only
        IF r_auto_email_rule.AUTO_EMAIL_CRITERIA=g_cariteria_description THEN
            FOR include_desc in (
              SELECT
              regexp_substr(r_auto_email_rule.AUTO_EMAIL_DESCRIPTION, '[^'
                                                         || v_delimiter
                                                         || ']+', 1, level) AS ex_d
              FROM
              dual
              CONNECT BY
              regexp_substr(r_auto_email_rule.AUTO_EMAIL_DESCRIPTION, '[^'
                                                         || v_delimiter
                                                         || ']+', 1, level) IS NOT NULL
              )
            LOOP
              IF include_desc.ex_d IS NULL OR instr(UPPER(p_desc),UPPER(include_desc.ex_d))>0 THEN
                o_auto_email_flag :='Y';
              END IF;
              EXIT WHEN o_auto_email_flag = 'Y';
            END LOOP;
        END IF;
        --check source only
        IF o_auto_email_flag ='N' AND r_auto_email_rule.AUTO_EMAIL_CRITERIA=g_cariteria_source THEN
            FOR include_source in (
              SELECT
              regexp_substr(r_auto_email_rule.AUTO_EMAIL_SOURCE, '[^'
                                                         || v_delimiter
                                                         || ']+', 1, level) AS ex_d
              FROM
              dual
              CONNECT BY
              regexp_substr(r_auto_email_rule.AUTO_EMAIL_SOURCE, '[^'
                                                         || v_delimiter
                                                         || ']+', 1, level) IS NOT NULL
              )
            LOOP
              IF include_source.ex_d IS NULL OR instr(UPPER(p_source),UPPER(include_source.ex_d))>0 THEN
                o_auto_email_flag :='Y';
              END IF;
              EXIT WHEN o_auto_email_flag = 'Y';
            END LOOP;
        END IF;
        --check source+description
        IF o_auto_email_flag ='N' AND r_auto_email_rule.AUTO_EMAIL_CRITERIA=g_cariteria_sourceDescr THEN
          IF (r_auto_email_rule.AUTO_EMAIL_SOURCE IS NULL AND r_auto_email_rule.AUTO_EMAIL_DESCRIPTION IS NULL) OR (instr(UPPER(p_source),UPPER(NVL(r_auto_email_rule.AUTO_EMAIL_SOURCE,'XXXYYYZZZZ')))>0 AND instr(UPPER(p_desc),UPPER(NVL(r_auto_email_rule.AUTO_EMAIL_DESCRIPTION,'XXXYYYZZZZ')))>0) THEN
            o_auto_email_flag :='Y';
          END IF;
      END IF;
        EXIT WHEN o_auto_email_flag = 'Y';
      END LOOP;
    END IF;

   IF o_auto_email_flag = 'Y' THEN
        v_dupflag := 'N';
        FOR emd_rec in c_emailduplicate LOOP

           BEGIN

             SELECT
               'Y'
             INTO
               v_dupflag
             FROM
             JAM.SF_NORM_ALARM N
             WHERE
              n.sf_cust_id = p_cust_id
              and n.sf_site_id = p_site_id
              and n.DESCR = p_desc
              and n.source = p_source
              and n.processed_flag in ( 'PQ','YQ','P','Y','PML','PMLQ','ADM_HOLDING','ADM_PENDING')
              and n.auto_email_flag in ('YQ','Y')
              and n.alarm_id < p_alm_id
              and n.created_on > (sysdate-2)
              and rownum < 2
              AND ( (p_time_received - n.time_received) * 86400) < emd_rec.duptimer;

              o_auto_email_flag:='YD';

           EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                          v_dupflag := 'N';
                        WHEN OTHERS THEN
                                        v_dupflag := 'N';
                                        WRITE_LOG_FILE(file_name => v_file_name
                                              ,info     => ' There is no auto email duplicate alarms '
                                              ,o_return_status  => v_return_status
                             ) ;
           END;
        END LOOP;
    END IF;


   o_return_status := 'S';
   EXCEPTION
      WHEN OTHERS
      THEN
        o_return_status := 'E';
        DBMS_OUTPUT.put_line ('INFO :- check_auto_emial_alarm' || SUBSTR(SQLERRM,1,1600));
     g_info_msz :=   'INFO :- check_emial_alarm' || SUBSTR(SQLERRM,1,1600);

          WRITE_LOG_FILE(file_name => v_file_name
                              ,info     => g_info_msz
                              ,o_return_status  => v_return_status
                     ) ;
END check_auto_email_alarms;

   --Gary Sun Oct,9 check email rules
   --chandra, sep 2018 salesforce update
  PROCEDURE check_email_alarms (p_alm_id            IN     NUMBER,
                                     p_cust_id           IN     VARCHAR2,
                                     p_site_id           IN     VARCHAR2,
                                     p_source            IN     VARCHAR2,
                                     p_desc              IN     VARCHAR2,
                                     p_created_on        IN     DATE,
                                     p_time_received	 IN 	DATE,
                                     o_return_status     OUT VARCHAR2,
                                     o_email_alm_flag     OUT VARCHAR2,
                                     o_email_address     OUT VARCHAR2
                                     )
   IS
   CURSOR rc_email_alm
   IS
   SELECT   'EMAIL FOUND' AS c1,maed.EMAIL_DELIVERY_ID
           FROM   sf_alarm_email_delivery maed
          WHERE       maed.sf_cust_id = p_cust_id
                  AND NVL (maed.sf_site_id, p_site_id) = p_site_id
                  AND UPPER (NVL (maed.raw_source, NVL (p_source, 'ABXBYCZ'))) =
                        UPPER (NVL (p_source, 'ABXBYCZ'))
                  AND UPPER (NVL (maed.raw_desc, NVL (p_desc, 'ABXBYCZ'))) =
                        UPPER (NVL (p_desc, 'ABXBYCZ'))
                  AND UPPER (maed.approved_ind) =g_rule_approved_status
                  AND get_timezones_GMT_TO_SERVER(p_created_on) BETWEEN maed.start_date
                                      AND  NVL (
                                                            maed.end_date,
                                                            (p_created_on + 1)
                                                         );
    cursor c_emailduplicate is
    select
    	max(repeat_duplicate_timer) duptimer
    from
	JAM.SF_MESSAGE_CONFIG smc
    where
 	sf_cust_id = p_cust_id
 	and message_type = 'EmailDuplicate'
 	and is_exclude = 'true'
    and UPPER (approved_ind) =g_rule_approved_status
 	AND get_timezones_GMT_TO_SERVER(p_created_on) BETWEEN smc.start_date AND  NVL ( smc.end_date, (p_created_on + 1) );


   --o_email_alm_flag varchar2(2);
     v_dupflag VARCHAR2(1);
   BEGIN
   	o_email_alm_flag:='N';
   	FOR c_email_alm in rc_email_alm LOOP
    		o_email_alm_flag:='Y';
    		o_email_address:=c_email_alm.EMAIL_DELIVERY_ID;
    		DBMS_OUTPUT.put_line('Email rule is configured:'||p_alm_id);
   	END LOOP;

   	IF o_email_alm_flag = 'Y' THEN
		--chandra May 21 2020 check for email duplicate rule
		v_dupflag := 'N';
		FOR emd_rec in c_emailduplicate LOOP

		   BEGIN

		     SELECT
		       'Y'
		     INTO
		       v_dupflag
		     FROM
		     JAM.SF_NORM_ALARM N
		     WHERE
		      n.sf_cust_id = p_cust_id
		      and n.sf_site_id = p_site_id
		      and n.DESCR = p_desc
		      and n.source = p_source
		      and n.processed_flag in ( 'PQ','YQ','P','Y')
		      and n.EMAIL_ALERT_ID is not null
		      and n.alarm_id < p_alm_id
		      and n.created_on > (sysdate-2)
		      and rownum < 2
		      AND ( (p_time_received - n.time_received) * 86400) < emd_rec.duptimer;

    		      o_email_alm_flag:='N';
    		      o_email_address:=NULL;

		   EXCEPTION
			WHEN NO_DATA_FOUND THEN
			  v_dupflag := 'N';
			WHEN OTHERS THEN
				v_dupflag := 'N';
				WRITE_LOG_FILE(file_name => v_file_name
				      ,info     => ' Error getting email duplicate alarms ' || SUBSTR(SQLERRM,1,1600)
				      ,o_return_status  => v_return_status
			     ) ;
		   END;

		END LOOP;
	END IF;


   	o_return_status := 'S';
   EXCEPTION
      WHEN OTHERS
      THEN
        o_return_status := 'E';
        DBMS_OUTPUT.put_line ('INFO :- check_emial_alarm' || SUBSTR(SQLERRM,1,1600));
     g_info_msz :=   'INFO :- check_emial_alarm' || SUBSTR(SQLERRM,1,1600);

          WRITE_LOG_FILE(file_name => v_file_name
                              ,info     => g_info_msz
                              ,o_return_status  => v_return_status
                     ) ;
   END check_email_alarms;

PROCEDURE check_mobile_alarms (  p_alm_id            IN     NUMBER,
                                     p_cust_id           IN     NUMBER,
                                     p_site_id           IN     NUMBER,
                                     p_source            IN     VARCHAR2,
                                     p_desc              IN     VARCHAR2,
                                     p_time_revd         IN     DATE,
                                     p_created_on        IN     DATE,
                                     o_mobile_flag       OUT     VARCHAR2,
                                     o_mobile_alert_only OUT VARCHAR2,
                                     o_mobile_alert_only_inactive OUT VARCHAR2,
                                     o_no_mobile_rule_flag OUT VARCHAR2,
                                     o_no_active_mobile_rule_flag OUT VARCHAR2,
                                     o_mobile_service_level       OUT     VARCHAR2,
                                     o_return_status     OUT VARCHAR2
                                     )
   IS
      TYPE g_mobile_rule_TAB IS TABLE OF VARCHAR2(2) INDEX BY BINARY_INTEGER;
      g_mobile_rule_s_ls g_mobile_rule_TAB;
      g_mobile_rule_d_ls g_mobile_rule_TAB;
      v_source_count  NUMBER(3);
      v_desc_count  NUMBER(3);
      v_mobile_exit_flag   VARCHAR2(2);
      v_mobile_rule_flag   VARCHAR2(50);
      v_service_level       VARCHAR2(50);
      v_service_level_exist VARCHAR2(50);
      v_monitoring_service_exist VARCHAR2(50);
      v_mobile_site_rule_exist VARCHAR2(50);

      v_mobile_rule_s_flag VARCHAR2(2);
      v_mobile_rule_d_flag VARCHAR2(2);

      v_mobile_rule_s_cnt NUMBER(2);
      v_mobile_rule_d_cnt NUMBER(2);

      v_mobile_rule_s_fnd varchar2(2);
      v_mobile_rule_d_fnd varchar2(2);

      o_records NUMBER(2);

      --Get active mobile service by site
      CURSOR rc_mobile_service_active
      IS
      select ssc.service_cntrct_name,ss.site_id
      from mss_site_service ss,
      mss_site_service_cntrct ssc,
      mss_monitor_service_site mss,
      cmn_service cs
      where ss.site_service_cntrct_id     =ssc.site_service_cntrct_id
      and ss.site_id                      =mss.site_id
      and mss.service_id                  =cs.service_id
      and ssc.service_cntrct_name        in ('Mobile Alert','Mobile Resolution')
      and nvl(ss.site_service_status_cd,1)=1
      and cs.service_name                 ='Mobile'
      and get_timezones_GMT_TO_SERVER(nvl(site_start_dt,sysdate+5))    <sysdate
      and get_timezones_GMT_TO_SERVER(nvl(site_stop_dt,sysdate -5))    >sysdate
      and ss.site_id                      =p_site_id;

      --Check if the mobile service is exist for this site
      CURSOR rc_mobile_service_exist
      IS
      select ssc.service_cntrct_name,ss.site_id
      from mss_site_service ss,
      mss_site_service_cntrct ssc,
      mss_monitor_service_site mss,
      cmn_service cs
      where ss.site_service_cntrct_id     =ssc.site_service_cntrct_id
      and ss.site_id                      =mss.site_id
      and mss.service_id                  =cs.service_id
      and ssc.service_cntrct_name        in ('Mobile Alert','Mobile Resolution')
      and nvl(ss.site_service_status_cd,1)=1
      and cs.service_name                 ='Mobile'
      and ss.site_id                      =p_site_id;

      --Check if monitor service exist for this site
      CURSOR rc_monitoring_service
      IS
      select ss.site_id  from mss_site_service ss,
      mss_monitor_service_site mss,
      cmn_service cs
      where
      ss.site_id                      =mss.site_id
      and mss.service_id                  =cs.service_id
      and ss.site_id=p_site_id
      and cs.service_name = 'Monitoring';

      --Get active mobile rule for this site
      CURSOR rc_mobile_site_rule
      IS
         select sf_message_id,
         EXCLUDE_SRC_FLAG,
         EXCLUDE_DESCR_FLAG
         from sf_message_config
         where sf_message_id IN -- chandra sep 2018 salesforce update
         (
          select sf_message_id
          from
          sf_message_config mmc
          where
          mmc.message_type = 'MOBILE'
          and mmc.sf_cust_id = p_cust_id
          and mmc.sf_site_Id = P_Site_Id
          AND p_time_revd BETWEEN mmc.start_date
          AND NVL (mmc.end_date,(p_time_revd + 1)));

      --Get active mobile rule for this customer
      CURSOR rc_mobile_cust_rule
        IS
        SELECT sf_message_id,
            EXCLUDE_SRC_FLAG,
            EXCLUDE_DESCR_FLAG
            from sf_message_config where sf_message_id IN
            (
            SELECT sf_message_id from
            sf_message_config mmc
            where
            mmc.message_type = 'MOBILE'
            AND mmc.sf_cust_id = p_cust_id
            AND mmc.sf_site_id is null
            AND p_time_revd BETWEEN mmc.start_date
            AND NVL (mmc.end_date,(p_time_revd+ 1)));

      --Check if the mobile rule is exist for site or cust
      CURSOR rc_mobile_rule_exist
      IS
      select sf_message_id,
             EXCLUDE_SRC_FLAG,
             EXCLUDE_DESCR_FLAG
             from sf_message_config where sf_message_id =(
      select case when (select 1
      from
      sf_message_config mmc
      where
      mmc.message_type = 'MOBILE'
      and mmc.sf_cust_id = p_cust_id
      and mmc.sf_site_Id = P_Site_Id
      and rownum=1) is null then
      (
      select sf_message_id from
      (select mmc.sf_message_id, ROW_NUMBER() OVER(ORDER BY mmc.MODIFIED_ON DESC) rn
      from
      sf_message_config mmc,
      mss_message_type_ref mmtr
      where
      mmc.message_type = 'MOBILE'
      and mmc.sf_cust_id = p_cust_id
      and mmc.sf_site_id is null
      ) where rn<2
      )else(
      select sf_message_id from
      (select mmc.sf_message_id, ROW_NUMBER() OVER(ORDER BY mmc.MODIFIED_ON DESC) rn
      from
      sf_message_config mmc,
      mss_message_type_ref mmtr
      where
      mmc.message_type = 'MOBILE'
      and mmc.sf_cust_id = p_cust_id
      and mmc.sf_site_Id = P_Site_Id
      ) where rn<2
      ) end message_id from dual
      );

    --Get mobile rule of souece
    CURSOR rc_mobile_rule_s(p_message_id NUMBER)
    IS
    select MMC.sf_MESSAGE_ID,
       MMC.EXCLUDE_SRC_FLAG,
           MMRSD.SRC_OR_DESCR_NAME
    from
      sf_MESSAGE_CONFIG MMC,
      MSS_MOBILE_RULE_SRC_DESCR MMRSD
    where
      MMC.sf_MESSAGE_ID=MMRSD.MESSAGE_ID
      and MMRSD.SRC_OR_DESCR_FLAG='S'
      and nvl(MMRSD.SRC_OR_DESCR_NAME,'YYYZZZZ')=p_source
      and MMC.sf_MESSAGE_ID=p_message_id;

    --Get mobile rule of description
    CURSOR rc_mobile_rule_d(p_message_id NUMBER)
    IS
    select MMC.sf_MESSAGE_ID,
       MMC.EXCLUDE_DESCR_FLAG,
           MMRSD.SRC_OR_DESCR_NAME
    from
      sf_MESSAGE_CONFIG MMC,
      MSS_MOBILE_RULE_SRC_DESCR MMRSD
    where
      MMC.sf_MESSAGE_ID=MMRSD.MESSAGE_ID
      and MMRSD.SRC_OR_DESCR_FLAG='D'
      and nvl(MMRSD.SRC_OR_DESCR_NAME,'ZZZZXXXYY')=p_desc
      and MMC.sf_MESSAGE_ID=p_message_id;

   BEGIN
      g_mobile_rule_s_ls.delete;
      g_mobile_rule_d_ls.delete;
      v_mobile_site_rule_exist :='N';


      FOR c_mobile_service IN rc_mobile_service_active LOOP
         v_service_level:=c_mobile_service.service_cntrct_name;
         o_mobile_service_level :=c_mobile_service.service_cntrct_name;
      END LOOP;

      FOR c_mobile_service_active IN rc_mobile_service_exist LOOP
         v_service_level_exist:=c_mobile_service_active.service_cntrct_name;
      END LOOP;

      FOR c_monitoring_service IN rc_monitoring_service LOOP
         v_monitoring_service_exist := 'Y';
      END LOOP;

      IF v_service_level IS NULL THEN
         o_mobile_flag :='N';
      ELSE
         --Check mobile rule for site
         v_source_count :=0;
         v_desc_count :=0;
         FOR c_mobile_rule IN rc_mobile_site_rule LOOP
            v_source_count :=v_source_count+1;
            v_desc_count :=v_desc_count+1;
            g_mobile_rule_s_ls(v_source_count):='N';
            g_mobile_rule_d_ls(v_desc_count):='N';
            v_mobile_rule_s_flag:='N';
            v_mobile_rule_d_flag:='N';
            v_mobile_site_rule_exist :='Y';
            v_mobile_rule_flag :='Y';
            v_mobile_rule_s_cnt:=0;
            v_mobile_rule_d_cnt:=0;
            v_mobile_exit_flag :='N';
            v_mobile_rule_s_fnd :='N';
            v_mobile_rule_d_fnd :='N';

            --source rule
                    SELECT count(SRC_OR_DESCR_NAME) into v_mobile_rule_s_cnt
                    from MSS_MOBILE_RULE_SRC_DESCR MMRSD
                    where MMRSD.SRC_OR_DESCR_FLAG='S'
                          and MMRSD.message_id=c_mobile_rule.sf_message_id;
                    IF v_mobile_rule_s_cnt > 0 THEN
                        FOR c_mobile_rule_s IN rc_mobile_rule_s(c_mobile_rule.sf_MESSAGE_ID) LOOP
                            v_mobile_rule_s_fnd:='Y';
                            IF c_mobile_rule_s.EXCLUDE_SRC_FLAG ='Y' THEN
                               g_mobile_rule_s_ls(v_source_count):='N';
                               v_mobile_exit_flag :='Y';
                            ELSE
                            g_mobile_rule_s_ls(v_source_count):='Y';
                            END IF;
                        END LOOP;
                        -- NOT FOUND, INCLUDE  NOT MOBILE RULE
                        -- NOT FOUND, EXCLUDE  IS MOBILE RULE
                        IF v_mobile_rule_s_fnd ='N' THEN
                            IF NVL(c_mobile_rule.EXCLUDE_SRC_FLAG,'O') = 'Y' THEN
                                g_mobile_rule_s_ls(v_source_count):='Y';
                            ELSE
                                g_mobile_rule_s_ls(v_source_count):='N';
                            END IF;
                        END IF;

                    ELSE
                        g_mobile_rule_s_ls(v_source_count):='Y';
                    END IF;

            --description rule
                    SELECT count(SRC_OR_DESCR_NAME) into v_mobile_rule_d_cnt
                    from MSS_MOBILE_RULE_SRC_DESCR MMRSD
                    where MMRSD.SRC_OR_DESCR_FLAG='D'
                          and MMRSD.message_id=c_mobile_rule.sf_message_id;

                    -- query rule with desc and message_id,have rule
                    IF v_mobile_rule_d_cnt >0 THEN
                        -- need to check include or exclude
                        FOR c_mobile_rule_d IN rc_mobile_rule_d(c_mobile_rule.sf_MESSAGE_ID) LOOP
                            v_mobile_rule_d_fnd :='Y';
                            IF c_mobile_rule_d.EXCLUDE_DESCR_FLAG ='Y' THEN
                               --exclude and find rule
                               g_mobile_rule_d_ls(v_desc_count):='N';
                               v_mobile_exit_flag :='Y';
                            ELSE
                                --include and find desc rule
                                g_mobile_rule_d_ls(v_desc_count):='Y';
                            END IF;
                        END LOOP;

                        -- NOT FOUND, INCLUDE  NOT MOBILE RULE
                        -- NOT FOUND, EXCLUDE  IS MOBILE RULE
                        IF v_mobile_rule_d_fnd ='N' THEN
                            IF NVL(c_mobile_rule.EXCLUDE_DESCR_FLAG,'O') = 'Y' THEN
                                g_mobile_rule_d_ls(v_desc_count):='Y';
                            ELSE
                                g_mobile_rule_d_ls(v_desc_count):='N';
                            END IF;
                        END IF;
                    -- query rule with source and message_id,no rule, is mobile rule
                    ELSE
                        g_mobile_rule_d_ls(v_desc_count):='Y';
                    END IF;
             --source null
                    IF p_source is null THEN
                        g_mobile_rule_s_ls(v_source_count):='Y';
                    END IF;
             --desc null
                    IF p_desc is null THEN
                        g_mobile_rule_d_ls(v_desc_count):='Y';
                    END IF;
            -- exclude exit loop
                    IF v_mobile_exit_flag ='Y' THEN
                        o_mobile_flag :='N';
                        EXIT;
                    END IF;

          END LOOP;

          IF v_mobile_exit_flag ='N' THEN
              FOR j IN g_mobile_rule_s_ls.FIRST .. g_mobile_rule_s_ls.LAST LOOP
                  IF g_mobile_rule_s_ls(j) ='Y' AND g_mobile_rule_d_ls(j)='Y' THEN
                      o_mobile_flag :='Y';
                      EXIT;
                  END IF;
              END LOOP;
          END IF;


          --If mobile rule for site is not exist,need to check the cust level rule
          IF v_mobile_site_rule_exist = 'N' THEN
              FOR c_mobile_rule IN rc_mobile_cust_rule LOOP
                  v_source_count :=v_source_count+1;
                  v_desc_count :=v_desc_count+1;
                  g_mobile_rule_s_ls(v_source_count):='N';
                  g_mobile_rule_d_ls(v_desc_count):='N';
                  v_mobile_rule_s_flag:='N';
                  v_mobile_rule_d_flag:='N';
                  v_mobile_rule_flag :='Y';
                  v_mobile_rule_s_cnt:=0;
                  v_mobile_rule_d_cnt:=0;
                  v_mobile_exit_flag :='N';
                  v_mobile_rule_s_fnd :='N';
                  v_mobile_rule_d_fnd :='N';

              --source rule
                      SELECT count(SRC_OR_DESCR_NAME) into v_mobile_rule_s_cnt
                      from MSS_MOBILE_RULE_SRC_DESCR MMRSD
                      where MMRSD.SRC_OR_DESCR_FLAG='S'
                            and MMRSD.message_id=c_mobile_rule.sf_message_id;
                      IF v_mobile_rule_s_cnt > 0 THEN
                          FOR c_mobile_rule_s IN rc_mobile_rule_s(c_mobile_rule.sf_MESSAGE_ID) LOOP
                              v_mobile_rule_s_fnd:='Y';
                              IF c_mobile_rule_s.EXCLUDE_SRC_FLAG ='Y' THEN
                                  g_mobile_rule_s_ls(v_source_count):='N';
                                  v_mobile_exit_flag :='Y';
                              ELSE
                                  g_mobile_rule_s_ls(v_source_count):='Y';
                                  EXIT;
                              END IF;
                          END LOOP;
                          -- NOT FOUND, INCLUDE  NOT MOBILE RULE
                          -- NOT FOUND, EXCLUDE  IS MOBILE RULE
                          IF v_mobile_rule_s_fnd ='N' THEN
                              IF NVL(c_mobile_rule.EXCLUDE_SRC_FLAG,'O') = 'Y' THEN
                                  g_mobile_rule_s_ls(v_source_count):='Y';
                              ELSE
                                  g_mobile_rule_s_ls(v_source_count):='N';
                              END IF;
                          END IF;
                      ELSE
                          g_mobile_rule_s_ls(v_source_count):='Y';
                      END IF;

              --description rule
                      SELECT count(SRC_OR_DESCR_NAME) into v_mobile_rule_d_cnt
                      from MSS_MOBILE_RULE_SRC_DESCR MMRSD
                      where MMRSD.SRC_OR_DESCR_FLAG='D'
                            and MMRSD.message_id=c_mobile_rule.sf_message_id;
                      IF v_mobile_rule_d_cnt >0 THEN
                          FOR c_mobile_rule_d IN rc_mobile_rule_d(c_mobile_rule.sf_MESSAGE_ID) LOOP
                              v_mobile_rule_d_fnd:='Y';
                              IF c_mobile_rule_d.EXCLUDE_DESCR_FLAG ='Y' THEN
                                 --v_mobile_rule_d_flag:='N';
                                 g_mobile_rule_d_ls(v_desc_count):='N';
                                 v_mobile_exit_flag :='Y';
                              ELSE
                                 g_mobile_rule_d_ls(v_desc_count):='Y';
                              END IF;
                          END LOOP;
                          -- NOT FOUND, INCLUDE  NOT MOBILE RULE
                          -- NOT FOUND, EXCLUDE  IS MOBILE RULE
                          IF v_mobile_rule_d_fnd ='N' THEN
                            IF NVL(c_mobile_rule.EXCLUDE_DESCR_FLAG,'O') = 'Y' THEN
                                g_mobile_rule_d_ls(v_desc_count):='Y';
                            ELSE
                                g_mobile_rule_d_ls(v_desc_count):='N';
                            END IF;
                          END IF;
                      ELSE
                          g_mobile_rule_d_ls(v_desc_count):='Y';
                      END IF;
              -- source is null
                  IF p_source is null THEN
                      g_mobile_rule_s_ls(v_source_count):='Y';
                  END IF;
              -- desc is null
                  IF p_desc is null THEN
                      g_mobile_rule_d_ls(v_desc_count):='Y';
                  END IF;
              -- exclude exit loop
                  IF v_mobile_exit_flag ='Y' THEN
                      o_mobile_flag :='N';
                      EXIT;
                  END IF;

            END LOOP;

              IF v_mobile_exit_flag ='N' THEN
                  FOR j IN g_mobile_rule_s_ls.FIRST .. g_mobile_rule_s_ls.LAST LOOP
                      IF g_mobile_rule_s_ls(j) ='Y' AND g_mobile_rule_d_ls(j)='Y' THEN
                          o_mobile_flag :='Y';
                          EXIT;
                      END IF;
                  END LOOP;
              END IF;

        END IF;
        --Can not find mobile rule for the site or customer
        IF v_mobile_rule_flag IS NULL THEN
             o_mobile_flag :='N';
        END IF;
  END IF;
  --NO rule ,but service level is not null ,active status
  IF v_mobile_rule_flag IS NULL AND v_service_level IS NOT NULL THEN
     FOR c_mobile_rule_exist IN rc_mobile_rule_exist LOOP
         o_no_active_mobile_rule_flag := 'Y';
     END LOOP;
     --no mobile rule for the active status
     IF o_no_active_mobile_rule_flag IS NULL THEN
         o_no_mobile_rule_flag := 'Y';
     END IF;
  END IF;

   -- Mobile alart,active status , no monitor service
   IF v_service_level ='Mobile Alert' AND v_monitoring_service_exist IS NULL THEN
      o_mobile_alert_only := 'Y';
   END IF;
   -- inactive mobile rule,no monitor service ,mobile alert
   IF v_service_level IS NULL AND v_monitoring_service_exist IS NULL AND v_service_level_exist ='Mobile Alert' THEN
      o_mobile_alert_only_inactive := 'Y';
   END IF;
   o_return_status := 'S';
   EXCEPTION
      WHEN OTHERS
      THEN
        o_return_status := 'E';
        DBMS_OUTPUT.put_line ('INFO :- check_mobile_alarm' || SUBSTR(SQLERRM,1,1600));
        g_info_msz :=   'INFO :- check_mobile_alarm' || SUBSTR(SQLERRM,1,1600);

          WRITE_LOG_FILE(file_name => v_file_name
                              ,info     => g_info_msz
                              ,o_return_status  => v_return_status
                     ) ;
   END check_mobile_alarms;

   --Gary Oct,2021 ADM
   PROCEDURE chk_adm_alarms (p_alarm_id              IN     NUMBER,
                                   p_cust_id               IN     VARCHAR2,
                                   p_site_id               IN     VARCHAR2,
                                   p_time_revd             IN     DATE,
                                   p_desc                  IN     VARCHAR2,
                                   p_source                IN     VARCHAR2,
                                   p_controller            IN     VARCHAR2,
                                   o_adm_flag                 OUT VARCHAR2,
                                   o_adm_auto_disregard_flag  OUT VARCHAR2,
                                   o_return_status            OUT VARCHAR2
                                   )
   IS
       CURSOR rc_adm_rules
       IS
       SELECT SOURCE_EXCLUDE,DESCRIPTION_EXCLUDE,sf_message_id
       FROM sf_message_config mmc
       WHERE mmc.sf_cust_id = p_cust_id
             And NVL(Mmc.sf_site_Id,P_Site_Id) = P_Site_Id
             AND mmc.message_type = g_adm_Message_Type
             AND p_time_revd BETWEEN mmc.start_date
             AND  NVL (mmc.end_date,(p_time_revd + 1));

	   --ADM auto disregard rules
	   CURSOR rc_adm_ad_rules
       IS
       SELECT ADM_AD_SOURCE_INCLUDE,ADM_AD_DESCRIPTION_INCLUDE,sf_message_id
       FROM sf_message_config mmc
       WHERE mmc.sf_cust_id = p_cust_id
             And NVL(Mmc.sf_site_Id,P_Site_Id) = P_Site_Id
             AND mmc.message_type = g_adm_ad_Message_Type
             AND p_time_revd BETWEEN mmc.start_date
             AND NVL (mmc.end_date,(p_time_revd + 1));

      v_delimiter VARCHAR2(2) :=';;';
      v_source_adm_flag VARCHAR2(2);
      v_descr_adm_flag VARCHAR2(2);

	  v_source_adm_ad_flag VARCHAR2(2);
      v_descr_adm_ad_flag VARCHAR2(2);

	  v_source_adm_ad_null_flag VARCHAR2(2);
      v_descr_adm_ad_null_flag VARCHAR2(2);

      v_adm_config_swtich VARCHAR2(2);
      v_adm_ml_support VARCHAR2(2);
      v_adm_support_controller VARCHAR2(2);
   BEGIN
       DBMS_OUTPUT.put_line ('starting procedure chk_adm_alarms');
       g_info_msz :=  'starting procedure chk_adm_alarms';

       WRITE_LOG_FILE(file_name => v_file_name
                       ,info     => g_info_msz
                       ,o_return_status  => v_return_status
                     ) ;
      o_adm_flag :='N';
      v_adm_support_controller:='N';
      --check if ADM model support this controller
      IF instr(UPPER(g_adm_support_controller),UPPER(p_controller))>0 OR p_controller IS NULL THEN
        v_adm_support_controller :='Y';
      END IF;
      IF v_adm_support_controller = 'Y' THEN
        FOR r_adm_rule IN rc_adm_rules
      LOOP
        DBMS_OUTPUT.put_line ('sf_message_id:'||r_adm_rule.sf_message_id);
        o_adm_flag :='Y';
        v_source_adm_flag := NULL;
        v_descr_adm_flag := NULL;
        --check if the source of current alarm is excluded
        FOR exclude_source in (
                SELECT
                  regexp_substr(r_adm_rule.source_exclude, '[^'
                                                 || v_delimiter
                                                 || ']+', 1, level) AS ex_s
                FROM
                  dual
                CONNECT BY
                  regexp_substr(r_adm_rule.source_exclude, '[^'
                                                 || v_delimiter
                                                 || ']+', 1, level) IS NOT NULL
            )LOOP
              IF instr(UPPER(p_source),UPPER(exclude_source.ex_s))>0 THEN
                v_source_adm_flag :='N';
                EXIT WHEN v_source_adm_flag = 'N';
              END IF;
            END LOOP;
        DBMS_OUTPUT.put_line ('starting procedure chk_adm_alarms-1'||v_source_adm_flag);

        --check if the description of current alarm is excluded
        FOR exclude_description in (
            SELECT
              regexp_substr(r_adm_rule.DESCRIPTION_EXCLUDE, '[^'
                                             || v_delimiter
                                             || ']+', 1, level) AS ex_d
            FROM
              dual
            CONNECT BY
              regexp_substr(r_adm_rule.DESCRIPTION_EXCLUDE, '[^'
                                             || v_delimiter
                                             || ']+', 1, level) IS NOT NULL
        )LOOP
          IF instr(UPPER(p_desc),UPPER(exclude_description.ex_d))>0 THEN
            v_descr_adm_flag :='N';
            EXIT WHEN v_descr_adm_flag = 'N';
          END IF;
        END LOOP;
        DBMS_OUTPUT.put_line ('starting procedure chk_adm_alarms-2'||v_descr_adm_flag);
        IF (v_source_adm_flag = 'N' AND v_descr_adm_flag = 'N') OR (v_source_adm_flag = 'N' AND r_adm_rule.DESCRIPTION_EXCLUDE IS NULL) OR (v_descr_adm_flag = 'N' AND r_adm_rule.source_exclude IS NULL) THEN
          o_adm_flag := 'N';
        END IF;
        EXIT WHEN o_adm_flag = 'N';
      END LOOP;
        DBMS_OUTPUT.put_line ('starting procedure chk_adm_alarms-3'||o_adm_flag);
        --ADM ML MODEL SUPPORT CASE HANDLER
         IF o_adm_flag = 'Y' THEN
        FOR ADM_CONFIG_SWITCH IN (SELECT SYS_CONFIG_VALUE FROM MSS_SYS_CONFIG WHERE SYS_CONFIG_TYPE_CD='ADM' AND SYS_CONFIG_CD='RULE_SWITCH' AND SYS_CONFIG_VALUE='ON')
        LOOP
          v_adm_config_swtich :='Y';
        END LOOP;
        IF v_adm_config_swtich ='Y' THEN
          FOR ADM_CONFIG_SUPPORT_DESC IN (SELECT SYS_CONFIG_VALUE FROM MSS_SYS_CONFIG WHERE SYS_CONFIG_TYPE_CD='ADM' AND SYS_CONFIG_CD='SUPPORTDESCR')
          LOOP

            FOR support_description in (
                SELECT
                  regexp_substr(ADM_CONFIG_SUPPORT_DESC.SYS_CONFIG_VALUE, '[^'
                                                 || v_delimiter
                                                 || ']+', 1, level) AS in_d
                FROM
                  dual
                CONNECT BY
                  regexp_substr(ADM_CONFIG_SUPPORT_DESC.SYS_CONFIG_VALUE, '[^'
                                                 || v_delimiter
                                                 || ']+', 1, level) IS NOT NULL
            )LOOP
              IF instr(UPPER(p_desc),UPPER(support_description.in_d)) > 0 OR REGEXP_LIKE(p_desc, support_description.in_d) THEN
                v_adm_ml_support:='Y';
              END IF;
            END LOOP;
          END LOOP;
        END IF;
      END IF;

          IF v_adm_ml_support IS NULL AND v_adm_config_swtich = 'Y' THEN
            o_adm_flag := 'N';
          END IF;
      END IF;

      --check ADM auto disregard rule

      o_adm_auto_disregard_flag := 'N';
      IF o_adm_flag = 'Y' THEN
		FOR r_adm_ad_rule IN rc_adm_ad_rules
		  LOOP
			v_source_adm_ad_flag := NULL;
			v_descr_adm_ad_flag := NULL;

			--check if the source of current alarm is included
        IF r_adm_ad_rule.ADM_AD_SOURCE_INCLUDE IS NOT NULL THEN
			FOR include_source in (
					SELECT
					  regexp_substr(r_adm_ad_rule.ADM_AD_SOURCE_INCLUDE, '[^'
													 || v_delimiter
													 || ']+', 1, level) AS inx_s
					FROM
					  dual
					CONNECT BY
					  regexp_substr(r_adm_ad_rule.ADM_AD_SOURCE_INCLUDE, '[^'
													 || v_delimiter
													 || ']+', 1, level) IS NOT NULL
				)LOOP
				  v_source_adm_ad_flag := 'N';
                  DBMS_OUTPUT.put_line ('starting procedure chk_adm_AD_alarms-1234'||v_source_adm_ad_flag);
				  IF instr(UPPER(p_source),UPPER(include_source.inx_s))>0 THEN
					v_source_adm_ad_flag :='Y';
					EXIT WHEN v_source_adm_ad_flag = 'Y';
				  END IF;
				END LOOP;
         END IF;
        DBMS_OUTPUT.put_line ('starting procedure chk_adm_AD_alarms-1'||v_source_adm_ad_flag);
          DBMS_OUTPUT.put_line ('starting procedure chk_adm_AD_alarms-1678'||v_source_adm_ad_flag);
			--check if the description of current alarm is included
        IF r_adm_ad_rule.ADM_AD_DESCRIPTION_INCLUDE IS NOT NULL THEN
			FOR include_description in (
				SELECT
				  regexp_substr(r_adm_ad_rule.ADM_AD_DESCRIPTION_INCLUDE, '[^'
												 || v_delimiter
												 || ']+', 1, level) AS inx_d
				FROM
				  dual
				CONNECT BY
				  regexp_substr(r_adm_ad_rule.ADM_AD_DESCRIPTION_INCLUDE, '[^'
												 || v_delimiter
												 || ']+', 1, level) IS NOT NULL
			)LOOP
			  v_descr_adm_ad_flag :='N';
			  IF instr(UPPER(p_desc),UPPER(include_description.inx_d))>0 THEN
				v_descr_adm_ad_flag :='Y';
				EXIT WHEN v_descr_adm_ad_flag = 'Y';
			  END IF;
			END LOOP;
        END IF;
      DBMS_OUTPUT.put_line ('starting procedure chk_adm_AD_alarms-2'||v_descr_adm_ad_null_flag);

            --In a single rule, both Description and soure include, auto disregard
			IF v_source_adm_ad_flag = 'Y' AND v_descr_adm_ad_flag = 'Y' THEN
			  o_adm_auto_disregard_flag := 'Y';
			END IF;

             --Source is null and description include, auto disregard
            IF v_source_adm_ad_flag IS NULL AND v_descr_adm_ad_flag = 'Y' THEN
              o_adm_auto_disregard_flag := 'Y';
            END IF;

            --Description is null and soure include, auto disregard
            IF v_descr_adm_ad_flag IS NULL AND v_source_adm_ad_flag = 'Y' THEN
              o_adm_auto_disregard_flag := 'Y';
            END IF;

            --Both Source and description are null, auto disregard
            IF v_source_adm_ad_flag IS NULL AND v_descr_adm_ad_flag IS NULL THEN
			  o_adm_auto_disregard_flag := 'Y';
			END IF;



			EXIT WHEN o_adm_auto_disregard_flag = 'Y';
      DBMS_OUTPUT.put_line ('starting procedure chk_adm_AD_alarms-4'||o_adm_auto_disregard_flag);
		  END LOOP;
      DBMS_OUTPUT.put_line ('starting procedure chk_adm_AD_alarms-3'||o_adm_auto_disregard_flag);
	  END IF;


    o_return_status := 'S';
    EXCEPTION
      WHEN OTHERS
      THEN
         --
         o_return_status := 'E';
         DBMS_OUTPUT.put_line ( sqlerrm );
         DBMS_OUTPUT.put_line ( 'INFO:-  Process ADM alarm error and the Alarm ID '||p_alarm_id);
          g_info_msz :=    ' Process ADM alarm error and the Alarm ID '||p_alarm_id;

          WRITE_LOG_FILE(file_name => v_file_name
                              ,info     => g_info_msz
                              ,o_return_status  => v_return_status
                     ) ;

         o_return_status := 'E';
           SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'SF Auto processed PL/SQL',
          p_error_message => ' Process ADM alarm error and the Alarm ID '||p_alarm_id,
              p_error_code=>'20012'
          );
   --
   END chk_adm_alarms;

   --procedure for validating wheather snooze rule is defined for a customer/sites
   --chandra sep,2018 salesforce update
   PROCEDURE snooze_alarms (p_alarm_id          IN     NUMBER,
                            p_cust_id           IN     VARCHAR2,
                            p_descr             IN     VARCHAR2,
                            o_snooze_time_min      OUT NUMBER,
                            o_return_status        OUT VARCHAR2)
   IS
      CURSOR c_snooze_alm
      IS
         SELECT   mt.timer_value
           FROM   mss_timer mt, mss_timer_assgn mta, mss_timer_type_ref mttr
          WHERE       mt.timer_id = mta.timer_id
                  AND mt.timer_type_id = mttr.timer_type_id
                  AND mt.descr = p_descr
                  AND mta.cust_id = p_cust_id
                  AND mttr.timer_type_name = 'Timer for Event Description';
   --
   BEGIN
      DBMS_OUTPUT.put_line('Before Execution in Snooze_alarms Procedure for Alarm ID  '|| p_alarm_id);

       g_info_msz :='Before Execution in Snooze_alarms Procedure for Alarm ID  '|| p_alarm_id ;

          WRITE_LOG_FILE(file_name => v_file_name
                              ,info     => g_info_msz
                              ,o_return_status  => v_return_status
                     ) ;

      FOR c_snooze_rec IN c_snooze_alm
      LOOP
         --
         o_snooze_time_min := ( (c_snooze_rec.timer_value) / 1440);
         --
         o_return_status := 'S';
      END LOOP;
   --

   EXCEPTION
      WHEN NO_DATA_FOUND
    THEN
          DBMS_OUTPUT.put_line('Event Description is not defined for customer/Description');

          g_info_msz :='Event Description is not defined for customer/Description' ;

          WRITE_LOG_FILE(file_name => v_file_name
                              ,info     => g_info_msz
                              ,o_return_status  => v_return_status
                     ) ;

            o_snooze_time_min := 0;
         o_return_status := 'S';
      --
      WHEN OTHERS
      THEN
         --
        DBMS_OUTPUT.put_line('INFO :- In snooze_alarms Procedure Info is :-  '|| SUBSTR(SQLERRM,1,1600));

         g_info_msz := 'In snooze_alarms Procedure Info is :-  '|| SUBSTR(SQLERRM,1,1600) ;

          WRITE_LOG_FILE(file_name => v_file_name
                        ,info     => g_info_msz
                        ,o_return_status  => v_return_status
                     ) ;

         o_return_status := 'E';
                  SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'Auto processed PL/SQL',
          p_error_message => 'In snooze_alarms Procedure Info is :-  '|| SUBSTR(SQLERRM,1,1600),
              p_error_code=>'20005'
          );
   --
   END snooze_alarms;

   --
   -- Procedure for validating Normalization for the source and description, -- srinivas, 26,apr,2012
   --chandra sep, 2018 salesforce update
      PROCEDURE manage_raw_uard (p_alarm_id     IN NUMBER,
                              p_raw_source   IN VARCHAR2,
                              p_raw_desc     IN VARCHAR2,
                              p_cust_id      IN VARCHAR2
                              )
   IS
      --Cursor declaration to validate whether the raw source and raw descripion is normalised or not
      CURSOR c_norm_source
      IS
           SELECT   mns.norm_source_id norm_source_id,
                    rnsd.norm_desc_id norm_desc_id,
                    mns.norm_source norm_source,
                    nd.norm_desc norm_desc,
                    rsd.cust_id cust_id,
                    rsd.raw_source_desc_id raw_source_desc_id,
                    NVL (mns.approved_flag, 'N') mns_approved_flag,
                    NVL (nd.approved_flag, 'N') nd_approved_flag,
                    NVL (rnsd.approved_flag, 'N') rnsd_approved_flag
             FROM   mss_raw_src_desc rsd,
                    mss_norm_src mns,
                    mss_raw_norm_src_desc rnsd,
                    mss_norm_desc nd,
                    sf_customer c -- chandra sep,2018 salesforce update
            WHERE       rnsd.norm_desc_id = nd.norm_desc_id(+)
                    AND rnsd.norm_source_id = mns.norm_source_id(+)
                    AND rnsd.raw_source_desc_id(+) = rsd.raw_source_desc_id
                    AND UPPER (NVL (rsd.raw_source, 'XYZXYS')) =
                          UPPER (NVL (p_raw_source, 'XYZXYS'))
                    AND UPPER (NVL (rsd.raw_desc, 'XYZXYD')) =
						  UPPER (NVL (SUBSTR(p_raw_desc,1,50), 'XYZXYD'))
                    AND UPPER (NVL (rsd.ignore_flag, 'N')) = 'N'
                    AND rsd.cust_id = c.mss_cust_id
                    and c.sf_cust_id = p_cust_id
         ORDER BY   cust_id ASC;

      --
      v_norm_source_id       NUMBER (15);
      v_norm_desc_id         NUMBER (15);
      v_norm_source          VARCHAR2 (255);
      v_norm_desc            VARCHAR2 (255);
      v_status               VARCHAR2 (1);
      v_raw_source_id        NUMBER (15);
      v_raw_desc_id          NUMBER (15);
      v_ns_approved_flag     VARCHAR2 (1);
      v_nd_approved_flag     VARCHAR2 (1);
      v_rnsd_approved_flag   VARCHAR2 (1);
   BEGIN
      /*
         logic to get normalized source id, if the norm source id does not exist
         then we will add this raw source into an sql table, alter commit to the
         raw source table
       */
      v_norm_source_id := NULL;
      v_norm_desc_id := NULL;
      v_norm_source :=NULL;
      v_norm_desc   :=NULL;
      v_raw_source_id := NULL;
      v_raw_desc_id := NULL;
      v_ns_approved_flag := NULL;
      v_nd_approved_flag := NULL;
      v_rnsd_approved_flag := NULL;

      FOR norm_src_rec IN c_norm_source
      LOOP
         v_norm_source_id := norm_src_rec.norm_source_id;
         v_raw_source_id := norm_src_rec.raw_source_desc_id;
         v_norm_source :=norm_src_rec.norm_source;
         v_norm_desc_id := norm_src_rec.norm_desc_id;
         v_norm_desc :=norm_src_rec.norm_desc;
         v_ns_approved_flag := norm_src_rec.mns_approved_flag;
         v_nd_approved_flag := norm_src_rec.nd_approved_flag;
         v_rnsd_approved_flag := norm_src_rec.rnsd_approved_flag;
      END LOOP;

      DBMS_OUTPUT.put_line (' Norm Source ID =>  ' || v_norm_source_id
                              || chr(13) || chr(10)||
                              ' Raw Source ID  =>  ' || v_raw_source_id);

       g_info_msz := ' Norm Source ID =>  ' || v_norm_source_id
                              || chr(13) || chr(10)||
                              ' Raw Source ID  =>  ' || v_raw_source_id ;

          WRITE_LOG_FILE(file_name => v_file_name
                        ,info     => g_info_msz
                        ,o_return_status  => v_return_status
                     ) ;

      IF (v_raw_source_id IS NULL)
      THEN
          DBMS_OUTPUT.put_line('SUBSTR (p_raw_desc, 1, 50)'|| SUBSTR (p_raw_desc, 1, 50));
         INSERT INTO mss_raw_src_desc (raw_source_desc_id,
                                       raw_source,
                                       raw_desc,
                                       original_raw_desc,
                                       cust_id,
                                       mapped_to_standard_flag,
                                       created_by,
                                       created_on,
                                       modified_by,
                                       modified_on)
           VALUES   (mss_raw_src_desc_sq.NEXTVAL,
                     p_raw_source,
                     SUBSTR (p_raw_desc, 1, 50),
                     p_raw_desc,
                     p_cust_id,
                     'N',
                     'Site Sync',
                     SYSDATE,
                     'Site Sync',
                     SYSDATE);
      END IF;

      IF     v_ns_approved_flag = 'Y'
         AND v_nd_approved_flag = 'Y'
         AND v_rnsd_approved_flag = 'Y'
      THEN
         UPDATE   sf_norm_alarm
            SET   norm_desc_id = v_norm_desc_id,
                  norm_desc = v_norm_desc,
                  norm_source_id = v_norm_source_id,
                  norm_source = v_norm_source
          WHERE   alarm_id = p_alarm_id;
      END IF;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         DBMS_OUTPUT.put_line('INFO :- In manage_raw_uard Procedure Info is  '|| SUBSTR(SQLERRM,1,1600));

          g_info_msz := 'In manage_raw_uard Procedure Info is  '|| SUBSTR(SQLERRM,1,1600) ;

          WRITE_LOG_FILE(file_name => v_file_name
                        ,info     => g_info_msz
                        ,o_return_status  => v_return_status
                     ) ;
      SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'SF Auto processed PL/SQL',
          p_error_message => 'In manage_raw_uard Procedure Info is  '|| SUBSTR(SQLERRM,1,1000),
              p_error_code=>'20004'
          );

   END manage_raw_uard;

  PROCEDURE update_alarms_statuses_temp (p_alarm_id IN NUMBER,
                                         p_current_status IN VARCHAR2,
                                         p_processed_flag IN VARCHAR2
                                         )
   IS
   BEGIN
   UPDATE sf_norm_alarm
   SET
   	current_status=p_current_status,
   	processed_flag=p_processed_flag||'Q'
   WHERE alarm_id=p_alarm_id;
   commit;
   END;

   PROCEDURE update_alarms_statuses (o_return_status OUT VARCHAR2)
   IS
   BEGIN
      --Updating MSS_NORM_ALRAM with the statuses
      --That are already stored in the global pl/sql tables
      FORALL i IN 1 .. g_alarm_count

         UPDATE   sf_norm_alarm mna
            SET   mna.request_id = -100,
                  mna.processed_flag = decode(g_processed_flag (i),'M','P',g_processed_flag (i)) ||'Q',
                  mna.ADM_AUTO_DISREGARD_FLAG = g_auto_disregard_flag(i),
                  mna.mobile_alarm_flag = decode(g_processed_flag (i),'M','MN','N'),
                  alarm_count_24_hrs = g_24hrs_count (i),
                  alarm_count_7_days = g_7days_count (i),
                  modified_on = get_timezones_SERVER_TO_GMT (SYSDATE),
                  ADM_STATUS_TIMEOUT_FLAG='N',
                  ADM_QUEUE_START_TIME = current_date,
                  modified_by = g_modified_by,
                  last_updated_login = -1,
                  time_available_process =
                  DECODE (g_processed_flag (i),'P', g_time_available_process (i),time_available_process),
                  sf_routing_group = g_routing_group (i),
                  email_processed=DECODE(g_email_alarm_flag(i),'Y','Y','N'),
                  auto_email_flag=DECODE(g_processed_flag (i)||g_auto_email_alarm_flag(i)||g_auto_email_cdm_cust(i),'PYN','YQ','PMLYN','YQ','MYN','YQ','YAEN','YQ','PYY','Y','PMLYY','Y','MYY','Y','YAEY','Y','N'),
                  --YH means this alarm will send to ADM model where decide if will send auto email
                  auto_email_cdm_flag=DECODE(g_processed_flag (i)||g_auto_email_alarm_flag(i)||g_auto_email_cdm_cust(i),'PYY','YQ','PMLYY','YH','MYY','YQ','YAEY','YQ','N'),
                  EMAIL_ALERT_ID=g_email_alarm_address(i),
                  LAST_ACTION_NAME =DECODE(g_processed_flag(i),'Y',g_last_action_name(i),null),
                  LAST_ACTION_COMMENTS=DECODE(g_processed_flag(i),'Y',g_last_action_comments(i),null),
                  LAST_ACTION_TIME=DECODE(g_processed_flag(i),'Y',get_timezones_SERVER_TO_GMT (SYSDATE),null)
          WHERE   mna.alarm_id = g_alm_id (i);

          dbms_output.put_line( ' Rows update ' || SQL%ROWCOUNT );


      FOR i IN 1 .. g_almactstat_count
      LOOP
         UPDATE   sf_norm_alarm mna
            SET   current_status = g_ins_current_status (i)
          WHERE   mna.alarm_id = g_almactstat_alm_id (i);
      END LOOP;

      FOR i IN 1 .. g_alarm_count
      LOOP
         g_alm_id (i) := NULL;
         g_processed_flag (i) := NULL;
         g_auto_disregard_flag (i) :=NULL;
         g_24hrs_count (i) := NULL;
         g_7days_count (i) := NULL;
         g_email_processed (i) := NULL;
         g_alert_id (i) := NULL;
         g_almactstat_alm_id (i) := NULL;
         g_time_available_process (i) := NULL;
         g_routing_group (i)      := NULL;
         g_email_alarm_flag(i)     := NULL;
         g_auto_email_alarm_flag(i)     := NULL;
         g_auto_email_cdm_cust(i) :=NULL;
         g_email_alarm_address(i)  :=NULL;
      END LOOP;

      g_alarm_count := 0;
      g_almactstat_count := 0;
      o_return_status := 'S';
    EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line('INFO :- In update_alarms_statuses Info is  '|| SUBSTR(SQLERRM,1,1600));

          g_info_msz := 'In update_alarms_statuses Info is '|| SUBSTR(SQLERRM,1,1600) ;

          WRITE_LOG_FILE(file_name => v_file_name
                        ,info     => g_info_msz
                        ,o_return_status  => v_return_status
                     ) ;

         o_return_status := 'E';
         SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'Auto processed PL/SQL',
          p_error_message => 'In update_alarms_statuses Info is '|| SUBSTR(SQLERRM,1,1600),
              p_error_code=>'20003'
          );
   END update_alarms_statuses;

   PROCEDURE validate_alarm_proc (admFlag IN VARCHAR2, errbuf OUT VARCHAR2, retcode OUT VARCHAR2)
   IS
      /*===========================================================================
      || PARAMETER DESCRIPTION:
      ||   P_RETURN_CODE VALUES ARE AS FOLLOWS - 0 = NORMAL COMPLETION
      ||                                         1 = WARNING
      ||                                         2 = ERROR OCCURRED
      ||
      \===========================================================================*/

      --
      v_log_name       VARCHAR2 (50);

      cur_alm_data SYS_REFCURSOR;
      v_alarm_query_str        VARCHAR2 (2000);
      v_commit_count           NUMBER := 0;
      v_return_status          VARCHAR2 (1);
      v_return_stat            VARCHAR2 (1);
      v_email_address_flag     VARCHAR2 (2);
      v_processed_alarms       NUMBER := 0;
      v_test_call              VARCHAR2 (2) := 'N';
      v_auto_process_flag      BOOLEAN := FALSE;
      v_prod_alarm_flag        BOOLEAN := FALSE;
      v_threshold_flag         BOOLEAN := FALSE;
      v_repeat_flag            VARCHAR2 (2);
      v_adm_flag               VARCHAR2 (2);
      v_adm_auto_disregard_flag               VARCHAR2 (2);

      --chandra sep,2018 salesforce update
      v_site_contact_id        VARCHAR2(18);

      v_get_site_cont_status   VARCHAR2 (2);
      v_return_stats           VARCHAR2 (2);
      v_disregard_flag         VARCHAR2 (2);
      v_thresh_flag            VARCHAR2 (2);
      v_status                 VARCHAR2 (2);
      v_duplicate_flag         VARCHAR2 (3);
      --v_global_msg                  VARCHAR2(150);
      v_include_flag           VARCHAR2 (2);
      v_mobile_flag            VARCHAR2 (2);
      v_mobile_alert_only      VARCHAR2(2);
      v_mobile_alert_only_inactive      VARCHAR2(2);
      v_no_mobile_rule_flag    VARCHAR (2);
      v_mobile_rule_send_mail_flag VARCHAR2 (2);
      v_mobile_rule_mail_subject VARCHAR2 (100);
      v_mobile_rule_mail_body  VARCHAR2 (1000);
      v_no_active_mobile_rule_flag      VARCHAR2 (2);
      v_mobile_service_level   VARCHAR2 (50);
      v_threshold_reached      VARCHAR2 (2);
      v_testcall_action_id     NUMBER;
      v_testcall_status_id     NUMBER;
      v_testcall_notes         VARCHAR2 (2000);
      v_disregard_action_id    NUMBER;
      v_disregard_status_id    NUMBER;
      v_disregard_msg          VARCHAR2 (2000);
      v_repeat_action_id       NUMBER;
      v_repeat_status_id       NUMBER;
      v_repeat_msg             VARCHAR2 (2000);
      v_dup_action_id          NUMBER;
      v_dup_status_id          NUMBER;
      v_dup_msg                VARCHAR2 (2000);
      v_threshold_action_id    NUMBER;
      v_threshold_status_id    NUMBER;
      v_threshold_msg          VARCHAR2 (2000);
      v_include_action_id      NUMBER;
      v_include_status_id      NUMBER;
      v_include_msg            VARCHAR2 (2000);
      --chandra, oct,21,2018 salesforce update
      v_email_alert_id         VARCHAR2(18);
      v_ret_stat               VARCHAR2 (2);
      g_test_call_cnt          NUMBER;
      v_manage_alm_exc EXCEPTION;
      --chandra, aug,21
      v_err_msg                VARCHAR2 (2000);
      --
      --Mritunjay, April,3
      v_rtn_date               DATE;
      v_rtn_msg                VARCHAR2 (50);
      v_rtn_flag               VARCHAR2 (10);
      v_rtn_status             VARCHAR2 (10);
      --
      --Mritunjay june 21,2012
      v_snooze_time_min        NUMBER;
      v_snooze_msg             VARCHAR2 (50);
      v_snooze_flag            VARCHAR2 (10);
      --Mritunjay August 28,2012
      v_cb_disregard_id        VARCHAR2(200);
      v_cb_include_action      VARCHAR2(200);
      v_min_flag               CHAR(1) := 'Y';
      v_min_alarm              NUMBER  := 0;
      v_max_alarm              NUMBER  := 0;
      v_email_alarm_flag       VARCHAR2 (10);
      v_auto_email_alarm_flag  VARCHAR2 (10);
      v_auto_email_cdm_cust    VARCHAR2 (10);
      v_sc_alarm_flag          VARCHAR2 (10);
      v_email_address          VARCHAR2 (2000);

      --autoprocess mobile alert email address
      v_emailAdd_mobilealert    VARCHAR2 (2000);
      v_site_name               VARCHAR2 (200);

      --chandra sep,2018 salesforce update
      TYPE alarm_id_tab IS TABLE OF sf_norm_alarm.alarm_id%TYPE INDEX BY BINARY_INTEGER;
      TYPE receiver_tab IS TABLE OF sf_norm_alarm.receiver%TYPE INDEX BY BINARY_INTEGER;
      TYPE descr_tab IS TABLE OF sf_norm_alarm.descr%TYPE INDEX BY BINARY_INTEGER;
      TYPE source_tab IS TABLE OF sf_norm_alarm.source%TYPE INDEX BY BINARY_INTEGER;
      TYPE time_received_tab IS TABLE OF sf_norm_alarm.time_received%TYPE INDEX BY BINARY_INTEGER;
      TYPE time_occurred_tab IS TABLE OF sf_norm_alarm.time_occurred%TYPE INDEX BY BINARY_INTEGER;
      TYPE controller_instance_tab IS TABLE OF sf_norm_alarm.controller_instance%TYPE INDEX BY BINARY_INTEGER;
      TYPE controller_tab IS TABLE OF sf_norm_alarm.controller%TYPE INDEX BY BINARY_INTEGER;
      TYPE created_on_tab IS TABLE OF sf_norm_alarm.created_on%TYPE INDEX BY BINARY_INTEGER;
      TYPE norm_desc_id_tab IS TABLE OF sf_norm_alarm.norm_desc_id%TYPE INDEX BY BINARY_INTEGER;
      TYPE norm_source_id_tab IS TABLE OF sf_norm_alarm.norm_source_id%TYPE INDEX BY BINARY_INTEGER;

      --chandra sep,2018 salesforce update
      TYPE sf_site_id_tab IS TABLE OF sf_norm_alarm.sf_site_id%TYPE INDEX BY BINARY_INTEGER;
      TYPE sf_cust_id_tab IS TABLE OF sf_norm_alarm.sf_cust_id%TYPE INDEX BY BINARY_INTEGER;
      TYPE sf_routing_group_tab IS TABLE OF sf_norm_alarm.sf_routing_group%TYPE INDEX BY BINARY_INTEGER;

      TYPE rtn_date_tab IS TABLE OF sf_norm_alarm.rtn_date%TYPE INDEX BY BINARY_INTEGER;
      TYPE field5_tab IS TABLE OF sf_norm_alarm.field5%TYPE INDEX BY BINARY_INTEGER;
      TYPE field6_tab IS TABLE OF sf_norm_alarm.field6%TYPE INDEX BY BINARY_INTEGER;
      TYPE field8_tab IS TABLE OF sf_norm_alarm.field8%TYPE INDEX BY BINARY_INTEGER;
      TYPE field9_tab IS TABLE OF sf_norm_alarm.field9%TYPE INDEX BY BINARY_INTEGER;
      TYPE field14_tab IS TABLE OF sf_norm_alarm.field14%TYPE INDEX BY BINARY_INTEGER;

      lt_alarm_id alarm_id_tab;
      lt_receiver receiver_tab;
      lt_descr descr_tab;
      lt_source source_tab;
      lt_time_received time_received_tab;
      lt_time_occurred time_occurred_tab;
      lt_controller_instance controller_instance_tab;
      lt_controller controller_tab;
      lt_created_on created_on_tab;
      lt_norm_desc_id norm_desc_id_tab;
      lt_norm_source_id norm_source_id_tab;

      --chandra sep,2018 salesforce update
      lt_site_id sf_site_id_tab;
      lt_cust_id sf_cust_id_tab;
      lt_routing_group sf_routing_group_tab;

      lt_rtn_date rtn_date_tab;
      lt_field5 field5_tab;
      lt_field6 field6_tab;
      lt_field8 field8_tab;
      lt_field9 field9_tab;
      lt_field14 field14_tab;

      t1 INTEGER;
      t2 INTEGER;
   BEGIN
        t1 := DBMS_UTILITY.get_time;
        IF admFlag = 'Y' THEN
          v_log_name:='SF_AUTOPROC_ADM';
        ELSE
          v_log_name:='SF_AUTOPROC_MAIN';
        END IF;
        LOG_FILE (directory_name   => g_directory
             ,log_name         =>v_log_name
             ,file_name        => v_file_name
             ,o_return_status  => v_return_status);

          DBMS_OUTPUT.put_line('----------------------------------------------------------------------------------'||
                      chr(13) || chr(10)||'SF Auto Process Package Execution Started @ '||to_char(sysdate,'MM/DD/YYYY HH24:MI:SS')||
                      chr(13) || chr(10)||'----------------------------------------------------------------------------------'
                      );

        g_info_msz := '----------------------------------------------------------------------------------'||
                      chr(13) || chr(10)||'SF Auto Process Package Execution Started @ '||to_char(sysdate,'MM/DD/YYYY HH24:MI:SS')||
                      chr(13) || chr(10)||'----------------------------------------------------------------------------------';

         WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;

      --Cache the required values using following procedure
      cache_values (o_return_status => v_return_status);

      select SYS_CONFIG_VALUE into v_emailAdd_mobilealert from MSS_SYS_CONFIG where SYS_CONFIG_TYPE_CD='MobileAlertOnly';

      --Generate a query to fetch adm customer alarms or non-adm customer alarms
      generate_alarm_query(p_adm_flag =>admFlag,o_query_string =>v_alarm_query_str);

      OPEN cur_alm_data FOR v_alarm_query_str;
      LOOP
      FETCH cur_alm_data BULK COLLECT INTO
            lt_alarm_id,
      lt_receiver,
      lt_descr,
      lt_source,
      lt_time_received,
      lt_time_occurred,
      lt_controller_instance,
      lt_controller,
      lt_created_on,
      lt_norm_desc_id,
      lt_norm_source_id,
      lt_site_id,
      lt_cust_id,
      lt_routing_group,
      lt_rtn_date,
      lt_field5,
      lt_field6,
      lt_field8,
      lt_field9,
      lt_field14
      LIMIT 100;

      EXIT WHEN lt_alarm_id.COUNT=0;

    FOR i IN lt_alarm_id.FIRST..lt_alarm_id.LAST LOOP
         v_auto_process_flag := FALSE;
         v_prod_alarm_flag := FALSE;
         v_threshold_flag := FALSE;
         v_email_alert_id := NULL;
         v_email_address_flag := NULL;
         v_site_contact_id := NULL;
         v_rtn_date := lt_rtn_date(i);

         IF v_min_flag = 'Y' THEN
         v_min_alarm:=lt_alarm_id(i);
         END IF;

         get_site_contact_id (
            p_site_id                => lt_site_id(i),
            o_site_contact_id        => v_site_contact_id,
            o_get_site_cont_status   => v_get_site_cont_status,
            o_err_msg                => v_err_msg
         );

         IF v_get_site_cont_status = 'E'
         THEN
            log_error_table (i_alm_id       => lt_alarm_id(i),
                             i_error_name   => 'Get Site Contact',
                             i_error        => v_err_msg);
         END IF;

         --Calling the MAnage_raw_pkg
         /* Verifying the raw source description whether alarm is normalized or not  -- srinivas, 26,apr,2012*/
         manage_raw_uard (p_alarm_id     => lt_alarm_id(i),
                          p_raw_source   => lt_source(i),
                          p_raw_desc     => lt_descr(i),
                          p_cust_id      => lt_cust_id(i)  -- chandra sep,2018 salesforce udpate
                         );

         chk_testcall_alarms (p_alarm_id      => lt_alarm_id(i),
                              p_descr         => lt_descr(i),
                              o_return_stat   => v_return_stat,
                              o_err_msg       => v_err_msg);

         -- return s means it is test call no need process any other rules
         IF v_return_stat = 'S'
         THEN
            v_auto_process_flag := TRUE;
            store_alarm_status_new (
               i_alm_id            => lt_alarm_id(i),
               i_cust_id           => lt_cust_id(i),
               i_site_id           => lt_site_id(i),
               i_description       => lt_descr(i),
               i_controller_inst   => lt_controller_instance(i),
               i_source            => lt_source(i),
               i_request_id        => NULL,
               i_processed_flag    => 'Y',
               i_adm_auto_disregard => 'N',
               i_email_processed   => NULL,
               i_email_alert_id    => NULL --store alarm action, status, notes
                                          ,
               i_action_id         => g_testcall_action_id,
               i_status_id         => g_testcall_status_id,
               i_site_contact_id   => v_site_contact_id,
               i_notes             => g_testcall_notes     -- v_testcall_notes
                                                      ,
               i_current_status    => 'Resolved' --Added by Mritunjay on 27feb2012
                                                ,
               i_time_available    => lt_created_on(i),
               i_routing_group     => lt_routing_group(i),
               --Added by Mritunjay Sinha on 28-August-2012 as implementing CB maintenace functionlity in MSSR
               o_return_status     => v_return_stat
            );
         END IF;

         IF NOT v_auto_process_flag
         THEN
            chk_disregard_alarms (
               p_alarm_id              => lt_alarm_id(i),
               p_cust_id               => lt_cust_id(i),
               p_site_id               => lt_site_id(i),
               p_desc                  => lt_descr(i),
               p_source                => lt_source(i),
               p_controller_instance   => lt_controller_instance(i),
               p_cont_name             => lt_field9(i),
               p_time_revd             => lt_time_received(i),
               p_adv_type              => lt_field8(i),
               p_adv_value             => lt_field6(i),
			   p_prop_name             => lt_field5(i),
               p_field14               => lt_field14(i),
               o_disregard_msg         => v_disregard_msg,
               o_disregard_flag        => v_disregard_flag,
               o_thresh_flag           => v_thresh_flag,
               o_threshold_reached     => v_threshold_reached,
               o_return_status         => v_return_stat,
               o_cb_disregard_id       => v_cb_disregard_id
            );
            IF v_return_stat = 'E'
            THEN
               log_error_table (i_alm_id       => lt_alarm_id(i),
                                i_error_name   => 'Check Disregard Alarms',
                                i_error        => v_err_msg);
            END IF;

            IF     v_return_stat <> 'E'
               AND v_disregard_flag = 'D'
            THEN                             -- Alarm is a Disregard alarm --3
               v_auto_process_flag := TRUE;
               store_alarm_status_new (
                  i_alm_id            => lt_alarm_id(i),
                  i_cust_id           => lt_cust_id(i),
                  i_site_id           => lt_site_id(i),
                  i_description       => lt_descr(i),
                  i_controller_inst   => lt_controller_instance(i),
                  i_source            => lt_source(i),
                  i_request_id        => NULL,
                  i_processed_flag    => 'Y',
                  i_adm_auto_disregard => 'N',
                  i_email_processed   => v_email_address_flag,
                  i_email_alert_id    => v_email_alert_id,
                  --store alarm action, status, notes
                  i_action_id         => NVL (v_cb_disregard_id,
                                              g_disregard_action_id),
                  i_status_id         => g_disregard_status_id,
                  i_site_contact_id   => v_site_contact_id,
                  i_notes             => v_disregard_msg,
                  i_current_status    => 'Resolved',
                  i_time_available    => lt_created_on(i),
                  i_routing_group     => lt_routing_group(i),
                  o_return_status     => v_return_stat
               );
         END IF;
       END IF;

	DBMS_OUTPUT.PUT_LINE ( ' End of Disregard Alarm ' || lt_alarm_id(i) );
         -- END of Disregard

         /*---------------------------------------------------------------------------/
         / Modified as per CR 20801, Now RTN validation will take place               /
         /before validation of REPEAT and DUPLICATE(as per Randy Confirmation)        /
         / Modified By Mritunjay Sinha on 27,Sep 2012                                 /
         /---------------------------------------------------------------------------*/
         --Calling "Check_RTN_alarms" procedure for RTN Rule

	 DBMS_OUTPUT.PUT_LINE ( ' Begnining of rtn ' || lt_alarm_id(i) );

         IF NOT v_auto_process_flag AND v_rtn_date IS NOT NULL
         THEN
            check_rtn_alarms (p_alarm_id        => lt_alarm_id(i),
                              p_cust_id         => lt_cust_id(i),
                              p_site_id         => lt_site_id(i),
                              p_desc            => lt_descr(i),
                              p_source          => lt_source(i),
                              p_controller_ins  => lt_controller_instance(i),
                              p_rtn_date        => lt_rtn_date(i),
                              p_time_ocurd      => lt_time_occurred(i),
                              p_time_revd       => lt_time_received(i),
                              p_point_name      => lt_field5(i),
                              o_rtn_msg         => v_rtn_msg,
                              o_rtn_flag        => v_rtn_flag,
                              o_return_status   => v_rtn_status);

            --
            IF v_return_status = 'E'
            THEN
               log_error_table (i_alm_id       => lt_alarm_id(i),
                                i_error_name   => 'Check RTN Found',
                                i_error        => v_err_msg);
            END IF;

            --
            IF v_rtn_flag = 'R'
            THEN     --RTN Rule is define for the alarm so Auto Process Alarm.
               v_auto_process_flag := TRUE;
               store_alarm_status_new (
                  i_alm_id            => lt_alarm_id(i),
                  i_cust_id           => lt_cust_id(i),
                  i_site_id           => lt_site_id(i),
                  i_description       => lt_descr(i),
                  i_controller_inst   => lt_controller_instance(i),
                  i_source            => lt_source(i),
                  i_request_id        => NULL,
                  i_processed_flag    => 'Y',
                  i_adm_auto_disregard => 'N',
                  i_email_processed   => v_email_address_flag,
                  i_email_alert_id    => v_email_alert_id --store alarm action, status, notes
                                                         ,
                  i_action_id         => g_rtn_action_id,
                  i_status_id         => g_rtn_status_id,
                  i_site_contact_id   => v_site_contact_id,
                  i_notes             => v_rtn_msg,
                  i_current_status    => 'Resolved',
                  i_time_available    => lt_created_on(i),
                  i_routing_group     => lt_routing_group(i),
                  --Added by Mritunjay Sinha on 28-August-2012 as implementing CB maintenace functionlity in MSSR
                  o_return_status     => v_rtn_status
               );
            END IF;
         --
         END IF;                                            -- End of RTN Rule

	 DBMS_OUTPUT.PUT_LINE ( ' Begnining of Repeat ' || lt_alarm_id(i) );
         --Begin of Check Repeat Alarms
         IF NOT v_auto_process_flag
         THEN         --Begin of Check Repeat Alarms (not disregard alarm) --4
            --dbms_output.put_line(' inside Repeat alarms ');

            --check for repeat alarms
            check_repeat_alarms (
               p_alarm_id              => lt_alarm_id(i),
               p_cust_id               => lt_cust_id(i),
               p_site_id               => lt_site_id(i),
               p_desc                  => lt_descr(i),
               p_source                => lt_source(i),
               p_controller_instance   => lt_controller_instance(i),
               p_cont_name             => lt_field9(i),
               p_time_revd             => lt_time_received(i),
               p_time_occr             => lt_time_occurred(i),
               o_repeat_msg            => v_repeat_msg,
               o_repeat_flag           => v_repeat_flag,
               o_return_status         => v_return_stat
            );

            IF v_return_stat = 'E'
            THEN
               log_error_table (i_alm_id       => lt_alarm_id(i),
                                i_error_name   => 'Check Repeat Alarms',
                                i_error        => v_err_msg);
            END IF;

            IF v_return_stat <> 'E' AND v_repeat_flag = 'R'
            THEN                              -- Alarm is a repeat alarm   --5
               --
               v_auto_process_flag := TRUE;
               --chandra, Aug,21
               store_alarm_status_new (
                  i_alm_id            => lt_alarm_id(i),
                  i_cust_id           => lt_cust_id(i),
                  i_site_id           => lt_site_id(i),
                  i_description       => lt_descr(i),
                  i_controller_inst   => lt_controller_instance(i),
                  i_source            => lt_source(i),
                  i_request_id        => NULL,
                  i_processed_flag    => 'Y',
                  i_adm_auto_disregard => 'N',
                  i_email_processed   => v_email_address_flag,
                  i_email_alert_id    => v_email_alert_id --store alarm action, status, notes
                                                         ,
                  i_action_id         => g_repeat_action_id,
                  i_status_id         => g_repeat_status_id,
                  i_site_contact_id   => v_site_contact_id,
                  i_notes             => 'Auto Processed - Repeat',
                  i_current_status    => 'Resolved' --Added by Mritunjay on 27feb2012
                                                   ,
                  i_time_available    => lt_created_on(i),
                  i_routing_group     => lt_routing_group(i),
                  --Added by Mritunjay Sinha on 28-August-2012 as implementing CB maintenace functionlity in MSSR
                  o_return_status     => v_return_stat
               );
               g_repeat_alm_cnt := g_repeat_alm_cnt + 1;
               /*
                update_alarms_statuses_temp(p_alarm_id=>lt_alarm_id(i),
                                            p_current_status=>'Resolved',
                                            p_processed_flag=>'Y'
                                                                    );
                                                                    */
            END IF;
         END IF;                                        -- End of Repeat check

	 DBMS_OUTPUT.PUT_LINE ( ' Begnining of Duplicate ' || lt_alarm_id(i) );
         IF NOT v_auto_process_flag
         THEN        --Begin of Check Duplicate Alarms (not repeat alarms) --6
            -- dbms_output.put_line(' Inside Duplicate ');

            --check for duplicate alarms
            check_duplicate_alarms (
               p_alarm_id              => lt_alarm_id(i),
               p_cust_id               => lt_cust_id(i),
               p_site_id               => lt_site_id(i),
               p_desc                  => lt_descr(i),
               p_source                => lt_source(i),
               p_controller_instance   => lt_controller_instance(i),
               p_cont_name             => lt_field9(i),
               p_time_revd             => lt_time_received(i),
               p_time_occr             => lt_time_occurred(i),
               o_duplicate_msg         => v_dup_msg,
               o_duplicate_flag        => v_duplicate_flag,
               o_return_status         => v_return_stat
            );

            IF v_return_stat = 'E'
            THEN
               log_error_table (i_alm_id       => lt_alarm_id(i),
                                i_error_name   => 'Check Duplicate Alarms',
                                i_error        => v_err_msg);
            END IF;

            IF v_return_stat <> 'E' AND v_duplicate_flag = 'DUP'
            THEN                             -- Alarm is a duplicate alarm --7
               v_auto_process_flag := TRUE;
               --chandra, Aug,21
               store_alarm_status_new (
                  i_alm_id            => lt_alarm_id(i),
                  i_cust_id           => lt_cust_id(i),
                  i_site_id           => lt_site_id(i),
                  i_description       => lt_descr(i),
                  i_controller_inst   => lt_controller_instance(i),
                  i_source            => lt_source(i),
                  i_request_id        => NULL,
                  i_processed_flag    => 'Y',
                  i_adm_auto_disregard => 'N',
                  i_email_processed   => v_email_address_flag,
                  i_current_status    => 'Resolved', --Added by Mritunjay on 27feb2012
                  i_time_available    => lt_time_received(i),
                  i_email_alert_id    => v_email_alert_id --store alarm action, status, notes
                                                         ,
                  i_action_id         => g_dup_action_id,
                  i_status_id         => g_dup_status_id,
                  i_site_contact_id   => v_site_contact_id,
                  i_notes             => v_dup_msg,
                  i_routing_group     => lt_routing_group(i),
                  --Added by Mritunjay Sinha on 28-August-2012 as implementing CB maintenace functionlity in MSSR
                  o_return_status     => v_return_stat
               );
               g_duplicate_alm_cnt := g_duplicate_alm_cnt + 1;
			      /*
                  update_alarms_statuses_temp(p_alarm_id=>lt_alarm_id(i),
                                            p_current_status=>'Resolved',
                                            p_processed_flag=>'Y'
                                                                    );
																	*/
            END IF;
         END IF;                                      --End of Duplicate check

	 DBMS_OUTPUT.PUT_LINE ( ' Begnining of Include ' || lt_alarm_id(i) );

         IF NOT v_auto_process_flag
         THEN                                                --  Include found
            -- dbms_output.put_line(' Inside Include ');

            --Check Include found.
            check_include_found (
               p_alarm_id              => lt_alarm_id(i),
               p_cust_id               => lt_cust_id(i),
               p_site_id               => lt_site_id(i),
               p_time_revd             => lt_time_received(i),
               p_controller_instance   => lt_controller_instance(i),
               p_controller            => lt_field9(i),
               p_source                => lt_source(i),
               p_desc                  => lt_descr(i),
               p_adv_type              => lt_field8(i),
               p_adv_value             => lt_field6(i),
               o_include_msg           => v_include_msg,
               o_include_flag          => v_include_flag,
               o_return_status         => v_return_status,
               o_cb_include_action     => v_cb_include_action
            --Added by Mritunjay Sinha on 28-August-2012 as implementing CB maintenace functionlity in MSSR
            );

            -- dbms_output.put_line('INSIDE CHK INCLUDE FOUND o_include_msg is '|| v_include_msg);
            --dbms_output.put_line('INSIDE CHK INCLUDE FOUND o_include_flag is '|| v_include_flag);
            IF v_return_status = 'E'
            THEN
               log_error_table (i_alm_id       => lt_alarm_id(i),
                                i_error_name   => 'Check Include Found',
                                i_error        => v_err_msg);
            END IF;

            IF v_include_flag = 'N'
            THEN --If include is not found means site is found so Auto Process Alarm.
               v_auto_process_flag := TRUE;
               --chandra, Aug,21
               store_alarm_status_new (
                  i_alm_id            => lt_alarm_id(i),
                  i_cust_id           => lt_cust_id(i),
                  i_site_id           => lt_site_id(i),
                  i_description       => lt_descr(i),
                  i_controller_inst   => lt_controller_instance(i),
                  i_source            => lt_source(i),
                  i_request_id        => NULL,
                  i_processed_flag    => 'Y',
                  i_adm_auto_disregard => 'N',
                  i_email_processed   => v_email_address_flag,
                  i_email_alert_id    => v_email_alert_id --store alarm action, status, notes
                                                         ,
                  i_action_id         => NVL (v_cb_include_action,
                                              g_include_action_id),
                  -- Mritunjay August,28,2012 CB maintenance
                  i_status_id         => g_include_status_id,
                  i_site_contact_id   => v_site_contact_id,
                  i_notes             => v_include_msg,
                  i_current_status    => 'Resolved' --Added by Mritunjay on 27feb2012
                                                   ,
                  i_time_available    => lt_created_on(i),
                  i_routing_group     => lt_routing_group(i),
                  --Added by Mritunjay Sinha on 28-August-2012 as implementing CB maintenace functionlity in MSSR
                  o_return_status     => v_return_stat
               );
            END IF;
         END IF;


     DBMS_OUTPUT.PUT_LINE ( ' Begnining of ADM ' || lt_alarm_id(i) );
         --Begin of Check ADM Alarms
         IF NOT v_auto_process_flag AND admFlag = 'Y'
         THEN
            --check for ADM alarms
            chk_adm_alarms (
               p_alarm_id              => lt_alarm_id(i),
               p_cust_id               => lt_cust_id(i),
               p_site_id               => lt_site_id(i),
               p_time_revd             => lt_time_received(i),
               p_desc                  => lt_descr(i),
               p_source                => lt_source(i),
               p_controller            => lt_controller(i),
               o_adm_flag              => v_adm_flag,
               o_adm_auto_disregard_flag => v_adm_auto_disregard_flag,
               o_return_status         => v_return_stat
            );

            IF v_return_stat = 'E'
            THEN
               log_error_table (i_alm_id       => lt_alarm_id(i),
                                i_error_name   => 'Check ADM Alarms',
                                i_error        => v_err_msg);
            END IF;

            IF v_return_stat <> 'E' AND v_adm_flag = 'Y'
            THEN
               --
               v_auto_process_flag := TRUE;
               --chandra, Aug,21
               store_alarm_status_new (
                  i_alm_id            => lt_alarm_id(i),
                  i_cust_id           => lt_cust_id(i),
                  i_site_id           => lt_site_id(i),
                  i_description       => lt_descr(i),
                  i_controller_inst   => lt_controller_instance(i),
                  i_source            => lt_source(i),
                  i_request_id        => NULL,
                  i_processed_flag    => 'PML',
                  i_adm_auto_disregard => v_adm_auto_disregard_flag,
                  i_email_processed   => v_email_address_flag,
                  i_email_alert_id    => v_email_alert_id,
                  i_action_id         => 'ADM Consult',
                  i_status_id         => g_eventtoqueue_status_id,
                  i_site_contact_id   => NULL,
                  i_notes             => 'Auto Processed - ADM',
                  i_current_status    => 'Unassigned',
                  i_time_available    => lt_created_on(i),
                  i_routing_group     => lt_routing_group(i),
                  o_return_status     => v_return_stat
               );
               g_repeat_alm_cnt := g_repeat_alm_cnt + 1;
            END IF;
         END IF;


	 DBMS_OUTPUT.PUT_LINE ( ' Begnining of Snooze ' || lt_alarm_id(i) );

          IF NOT v_auto_process_flag AND NOT v_threshold_flag
         THEN

            v_return_status := 'E';

            IF v_return_status = 'S'
            THEN
               store_alarm_status_new (
                  i_alm_id            => lt_alarm_id(i),
                  i_cust_id           => lt_cust_id(i),
                  i_site_id           => lt_site_id(i),
                  i_description       => lt_descr(i),
                  i_controller_inst   => lt_controller_instance(i),
                  i_source            => lt_source(i),
                  i_request_id        => NULL,
                  i_processed_flag    => 'P',
                  i_adm_auto_disregard => 'N',
                  i_email_processed   => v_email_address_flag,
                  i_email_alert_id    => v_email_alert_id --store alarm action, status, notes
                                                         ,
                  i_action_id         => g_eventtoqueue_action_id,
                  i_status_id         => g_eventtoqueue_status_id,
                  i_site_contact_id   => NULL,
                  i_notes             => 'Event to queue',
                  i_current_status    => 'Unassigned' --Added by Mritunjay on 27feb2012
                                             ,
                  i_time_available    => (lt_created_on(i)
                                          + v_snooze_time_min) -- Mritunjay November,07,2012 removed p_timereceived
                                                              ,
                  i_routing_group     => lt_routing_group(i),
                  o_return_status     => v_return_stat
               );
            ELSE
               store_alarm_status_new (
                  i_alm_id            => lt_alarm_id(i),
                  i_cust_id           => lt_cust_id(i),
                  i_site_id           => lt_site_id(i),
                  i_description       => lt_descr(i),
                  i_controller_inst   => lt_controller_instance(i),
                  i_source            => lt_source(i),
                  i_request_id        => NULL,
                  i_processed_flag    => 'P',
                  i_adm_auto_disregard => 'N',
                  i_email_processed   => v_email_address_flag,
                  i_email_alert_id    => v_email_alert_id --store alarm action, status, notes
                                                         ,
                  i_action_id         => g_eventtoqueue_action_id,
                  i_status_id         => g_eventtoqueue_status_id,
                  i_site_contact_id   => NULL,
                  i_notes             => 'Event to queue',
                  i_current_status    => 'Unassigned' --Added by Mritunjay on 27feb2012
                                             ,
                  i_time_available    => lt_created_on(i),
                  i_routing_group     => lt_routing_group(i),
                  --Added by Mritunjay Sinha on 28-August-2012 as implementing CB maintenace functionlity in MSSR
                  o_return_status     => v_return_stat
               );
            END IF;
         END IF;


          IF instr(UPPER(lt_descr(i)),UPPER('Test Call')) = 0 AND instr(UPPER(lt_descr(i)),UPPER('Test Alarm')) = 0 THEN
	  	DBMS_OUTPUT.PUT_LINE ( ' Begnining of Check emails ' || lt_alarm_id(i) );
            --Check Email Rule
            check_email_alarms (
               p_alm_id              => lt_alarm_id(i),
               p_cust_id               => lt_cust_id(i),
               p_site_id               => lt_site_id(i),
               p_source                => lt_source(i),
               p_desc                  => lt_descr(i),
               p_created_on            => lt_created_on(i),
               p_time_received	       => lt_time_received(i),
               o_return_status         => v_return_status,
               o_email_alm_flag        =>v_email_alarm_flag,
               o_email_address         =>v_email_address
            );
            g_email_alarm_flag(g_alarm_count):=v_email_alarm_flag;
            g_email_alarm_address(g_alarm_count):=v_email_address;
            dbms_output.put_line( ' email flag :' || v_email_alarm_flag );
            --check auto email rule
            check_auto_email_alarms (
               p_alm_id              => lt_alarm_id(i),
               p_cust_id               => lt_cust_id(i),
               p_site_id               => lt_site_id(i),
               p_desc                  => lt_descr(i),
               p_source                  => lt_source(i),
               p_created_on            => lt_created_on(i),
               p_time_received	       => lt_time_received(i),
               o_return_status         => v_return_status,
               o_auto_email_flag        => v_auto_email_alarm_flag,
               o_auto_email_cdm_cust    => v_auto_email_cdm_cust
            );
            g_auto_email_alarm_flag(g_alarm_count):=v_auto_email_alarm_flag;
            g_auto_email_cdm_cust(g_alarm_count):=v_auto_email_cdm_cust;

            IF g_processed_flag(g_alarm_count) ='P' AND v_auto_email_alarm_flag ='Y' THEN
               IF (admFlag = 'N') OR (admFlag = 'Y' AND v_adm_flag !='Y') THEN
                  g_processed_flag(g_alarm_count) := 'Y';
                  g_auto_email_alarm_flag(g_alarm_count) :='AE';
                  g_last_action_name(g_alarm_count) := 'AUTO_EMAIL_PROCESS';
                  g_last_action_comments(g_alarm_count) := 'Email Resolved by Auto Email rule';
                  g_ins_current_status(g_alarm_count) := 'Resolved';
                END IF;
            END IF;

            --Resolve duplicated AUTO EMAIL alarm
            IF g_processed_flag(g_alarm_count) ='P' AND v_auto_email_alarm_flag ='YD' THEN
               g_processed_flag(g_alarm_count) := 'Y';
               g_auto_email_alarm_flag(g_alarm_count) :='YD';
               g_last_action_name(g_alarm_count) := 'DUPLICATE_RESOLVED';
               g_last_action_comments(g_alarm_count) := 'Resolved by duplicated Email rule';
               g_ins_current_status(g_alarm_count) := 'Resolved';
            END IF;



            dbms_output.put_line( 'Auto email flag :' || v_email_alarm_flag );
            dbms_output.put_line( 'Auto processed flag==== :' || g_processed_flag (g_alarm_count) );
         dbms_output.put_line( 'Auto email flag==== :' || g_auto_email_alarm_flag (g_alarm_count) );
          dbms_output.put_line( 'Auto email cdm cust==== :' || g_auto_email_cdm_cust (g_alarm_count) );
          ELSE
            g_email_alarm_flag(g_alarm_count):= 'N';
            g_email_alarm_address(g_alarm_count):= NULL;

            g_auto_email_alarm_flag(g_alarm_count):='N';
            g_auto_email_cdm_cust(g_alarm_count):='N';
          END IF;

         v_processed_alarms := v_processed_alarms + 1;
         v_max_alarm := lt_alarm_id(i);
         v_min_flag := 'N';

       END LOOP;

           insert_alm_act_stat_notes (o_return_status => v_return_status);
             IF v_return_status <> 'S'
             THEN
               DBMS_OUTPUT.put_line('Error while inserting Alarm Action Status Notes table. Error is '|| SUBSTR(SQLERRM,1,1600));

               g_info_msz :=        'Error while inserting Alarm Action Status Notes table. Error is '|| SUBSTR(SQLERRM,1,1600);

               WRITE_LOG_FILE(file_name => v_file_name
                  ,info     => g_info_msz
                  ,o_return_status  => v_return_status) ;

            END IF;


            update_alarms_statuses (o_return_status => v_return_status);

            IF v_return_status <> 'S'
             THEN
               DBMS_OUTPUT.put_line ('INFO :- Unable to update Alarm Statuses.' || SUBSTR(SQLERRM,1,1600));
               g_info_msz :=        'Unable to update Alarm Statuses.'|| SUBSTR(SQLERRM,1,1600);

               WRITE_LOG_FILE(file_name => v_file_name
                  ,info     => g_info_msz
                  ,o_return_status  => v_return_status) ;
            END IF;
            COMMIT;

      END LOOP;
      t2 := DBMS_UTILITY.get_time;
      DBMS_OUTPUT.put_line('----------------------------------------------------------------------------------'||
                         chr(13) || chr(10)||'Minimun ALARM ID Processed  '||v_min_alarm||
                         chr(13) || chr(10)||'Maximum ALARM ID Processed  '||v_max_alarm||
                         chr(13) || chr(10)||'Total Alarm processed  '||v_processed_alarms||
                         chr(13) || chr(10)||'Execution time  '||TO_CHAR((t2-t1)/100,'999.999')||
                         chr(13) || chr(10)||'Auto Process Package Execution Completed @  '||to_char(sysdate,'MM/DD/YYYY HH24:MI:SS')||
                         chr(13) || chr(10)||'----------------------------------------------------------------------------------'
                         );

          g_info_msz := '----------------------------------------------------------------------------------'||
                         chr(13) || chr(10)||'Minimun ALARM ID Processed  '||v_min_alarm||
                         chr(13) || chr(10)||'Maximum ALARM ID Processed  '||v_max_alarm||
                         chr(13) || chr(10)||'Total Alarm processed  '||v_processed_alarms||
                         chr(13) || chr(10)||'Execution time  '||TO_CHAR((t2-t1)/100,'999.999')||
                         chr(13) || chr(10)||'Auto Process Package Execution Completed @  '||to_char(sysdate,'MM/DD/YYYY HH24:MI:SS')||
                         chr(13) || chr(10)||'----------------------------------------------------------------------------------'
                         ;

        WRITE_LOG_FILE(file_name => v_file_name
                       ,info     => g_info_msz
                       ,o_return_status  => v_return_status) ;


       CLOSE_LOG_FILE(v_file_name);

   EXCEPTION
      WHEN v_manage_alm_exc
      THEN
         ROLLBACK;
         retcode := 2;
         DBMS_OUTPUT.put_line ('Error while processing alarms');
         CLOSE_LOG_FILE(v_file_name);
             SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'Auto processed PL/SQL',
          p_error_message => 'Error while processing alarms',
              p_error_code=>'20002'
          );
      WHEN OTHERS
      THEN
         ROLLBACK;
         retcode := 2;
         DBMS_OUTPUT.put_line(SUBSTR(SQLERRM,1,2000));
     CLOSE_LOG_FILE(v_file_name);
      SF_SEND_MAIL_PKG.send_error_to_mail
          (
          p_application_name => 'Auto processed PL/SQL',
          p_error_message => SUBSTR(SQLERRM,1,1000),
              p_error_code=>'20001'
          );
   END validate_alarm_proc;
END SF_AUTOPROCESS_ALARMS_ALL_PKG;