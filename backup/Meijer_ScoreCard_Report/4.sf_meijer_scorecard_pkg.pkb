CREATE OR REPLACE PACKAGE BODY sf_meijer_scorecard_pkg AS

    PROCEDURE meijer_scordcard_proc (
        errbuf    OUT   VARCHAR2,
        retcode   OUT   VARCHAR2
    ) AS
    currentGMTTime        DATE;
    BEGIN
        dbms_output.put_line('----------------------------------------------------------------------------------'
                             || chr(13)
                             || chr(10)
                             || 'Insert meijer_scordcard_proc table Started @ '
                             || to_char(sysdate, 'MM/DD/YYYY HH24:MI:SS')
                             || chr(13)
                             || chr(10)
                             || '----------------------------------------------------------------------------------');
        --get current time with GMT timezone
        currentGMTTime := F_Get_Timezone(sysdate);

        MERGE INTO SF_MEIJER_SCORECARD sms
        USING (
        ----------- Start to calculate Meijer scorecard report data ----------------------------
			SELECT
			    ss.division,
				ss.sf_site_name store,
				sna.controller_instance main_system,
				sna.source,
				sna.descr description,
				trunc(FROM_TZ (CAST (TO_DATE (TO_CHAR ( time_received , 'dd/mm/yyyy HH24:MI:SS'),'dd/mm/yyyy HH24:MI:SS') AS TIMESTAMP), tz_offset('US/Eastern') ) AT TIME ZONE nvl(ss.timezone_id,'US/Eastern') ) alarm_date,
				COUNT(alarm_id) alarm_count
			FROM
				sf_norm_alarm  sna,
				sf_site        ss,
				sf_customer   sc
			WHERE
					sna.sf_site_id = ss.sf_site_id
				AND sna.sf_cust_id = sc.sf_cust_id
				AND sc.sf_cust_name ='Meijer HQ'
				AND upper(sna.descr)<>'TEST CALL'
				AND trunc(FROM_TZ (CAST (TO_DATE (TO_CHAR ( time_received , 'dd/mm/yyyy HH24:MI:SS'),'dd/mm/yyyy HH24:MI:SS') AS TIMESTAMP), tz_offset('US/Eastern') ) AT TIME ZONE nvl(ss.timezone_id,'US/Eastern') )>=TRUNC(SYSDATE-7)
				AND trunc(FROM_TZ (CAST (TO_DATE (TO_CHAR ( time_received , 'dd/mm/yyyy HH24:MI:SS'),'dd/mm/yyyy HH24:MI:SS') AS TIMESTAMP), tz_offset('US/Eastern') ) AT TIME ZONE nvl(ss.timezone_id,'US/Eastern') )<TRUNC(SYSDATE)
			GROUP BY
			    ss.division,
				ss.sf_site_name,
				sna.controller_instance,
				sna.source,
				sna.descr,
				trunc(FROM_TZ (CAST (TO_DATE (TO_CHAR ( time_received , 'dd/mm/yyyy HH24:MI:SS'),'dd/mm/yyyy HH24:MI:SS') AS TIMESTAMP), tz_offset('US/Eastern') ) AT TIME ZONE nvl(ss.timezone_id,'US/Eastern') )

              )
        -------------------- end to calculate Meijer scorecard report data --------------------------------
        alarm ON ( sms.store = alarm.store and sms.division = alarm.division and sms.main_system = alarm.main_system and sms.source = alarm.source and sms.description = alarm.description
                   AND sms.alarm_date = alarm.alarm_date )
        WHEN MATCHED THEN UPDATE
        SET sms.TOTAL_ALARMS = alarm.TOTAL_ALARMS,
            modified_on = currentGMTTime
        WHEN NOT MATCHED THEN
        INSERT (
		    DIVISION,
            STORE,
            MAIN_SYSTEM,
            SOURCE,
            DESCRIPTION,
            ALARM_DATE,
            TOTAL_ALARMS,
            created_on,
            created_by,
            modified_on,
            modified_by )
        VALUES
            ( alarm.DIVISION,
              alarm.STORE,
              alarm.MAIN_SYSTEM,
              alarm.SOURCE,
              alarm.DESCRIPTION,
              alarm.ALARM_DATE,
              alarm.TOTAL_ALARMS,
              currentGMTTime,
              'PLSQL',
              currentGMTTime,
              'PLSQL' );
	  commit;
       dbms_output.put_line('----------------------------------------------------------------------------------'
                             || chr(13)
                             || chr(10)
                             || 'Insert meijer_scordcard_proc table End @ '
                             || to_char(sysdate, 'MM/DD/YYYY HH24:MI:SS')
                             || chr(13)
                             || chr(10)
                             || '----------------------------------------------------------------------------------');
    END meijer_scordcard_proc;

    FUNCTION f_get_timezone (
        i_timevalue IN DATE
    ) RETURN DATE IS
        v_updated_date DATE;
    BEGIN
        SELECT
            i_timevalue - ( substr(tz_offset(sessiontimezone), 1, instr(tz_offset(sessiontimezone), ':') - 1) / 24 + substr(tz_offset
            (sessiontimezone), instr(tz_offset(sessiontimezone), ':') + 1, 2) / 1440 )
        INTO v_updated_date
        FROM
            dual;

        RETURN v_updated_date;
          --
    EXCEPTION
        WHEN no_data_found THEN
          --
            dbms_output.put_line('Execution Error in getting the timezone details');
        WHEN OTHERS THEN
          --
            dbms_output.put_line('Unhandled exception in F_Get_Timezone');
            dbms_output.put_line(sqlerrm);
          --
    END f_get_timezone;

END sf_meijer_scorecard_pkg;