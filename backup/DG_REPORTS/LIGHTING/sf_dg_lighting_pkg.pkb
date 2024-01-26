CREATE OR REPLACE PACKAGE BODY sf_dg_lighting_pkg AS

    PROCEDURE dg_lighting_proc (
        errbuf    OUT   VARCHAR2,
        retcode   OUT   VARCHAR2
    ) AS
        currentgmttime DATE;
    BEGIN
        dbms_output.put_line('----------------------------------------------------------------------------------'
                             || chr(13)
                             || chr(10)
                             || 'Insert sf_dg_lighting table Started @ '
                             || to_char(sysdate, 'MM/DD/YYYY HH24:MI:SS')
                             || chr(13)
                             || chr(10)
                             || '----------------------------------------------------------------------------------');
        --get current time with GMT timezone

        currentgmttime := f_get_timezone(sysdate);
        MERGE INTO sf_dg_lighting sdl
        USING (
        ----------- Start to calculate dg lighting report data ----------------------------
                  SELECT DISTINCT
                      TRIM(replace(ss.sf_site_name, 'Dollar General ')) AS store,
                      trunc(sna.time_occurred) AS alarm_monitoring_date,
                      sc.sf_cust_name AS store_chain,
                      COUNT(DISTINCT
                          CASE
                              WHEN(sna.descr = 'Lights on too long'
                                   OR sna.descr = 'Lights on Too Long') THEN
                                  sna.alarm_id
                          END
                      ) lights_on_too_long,
                      COUNT(DISTINCT
                          CASE
                              WHEN(sna.descr = 'Excessive light cycles'
                                   OR sna.descr = 'Excessive Light Cycles') THEN
                                  sna.alarm_id
                          END
                      ) excessive_light_cycles,
                      dbms_lob.substr(regexp_replace(
                          LISTAGG(
                              CASE
                                  WHEN(sna.descr = 'Lights on too long'
                                       OR sna.descr = 'Lights on Too Long') THEN
                                      sna.source
                                  ELSE
                                      NULL
                              END, ',') WITHIN GROUP(
                              ORDER BY
                                  1
                          ), '([^,]+)(,\1)+', '\1'), 4000, 1) lights_on_too_long_apps,
                      dbms_lob.substr(regexp_replace(
                          LISTAGG(
                              CASE
                                  WHEN(sna.descr = 'Excessive light cycles'
                                       OR sna.descr = 'Excessive Light Cycles') THEN
                                      sna.source
                                  ELSE
                                      NULL
                              END, ',') WITHIN GROUP(
                              ORDER BY
                                  1
                          ), '([^,]+)(,\1)+', '\1'), 4000, 1) excessive_light_cycles_apps,
                      COUNT(DISTINCT sna.alarm_id) AS event_count
                  FROM
                      jam.sf_site         ss,
                      jam.sf_customer     sc,
                      jam.sf_norm_alarm   sna
                  WHERE
                      sc.sf_cust_name LIKE 'Dollar General Traditional%'
                      AND sc.sf_cust_id = ss.sf_cust_id
                      AND sna.sf_site_id = ss.sf_site_id
                      AND ( sna.time_occurred ) > trunc(sysdate - 1)
                      AND sna.descr IN (
                          'Lights on too long',
                          'Excessive light cycles',
                          'Lights on Too Long',
                          'Excessive Light Cycles'
                      )
                  GROUP BY
                      ss.sf_site_name,
                      sc.sf_cust_name,
                      trunc(sna.time_occurred)
              )
        -------------------- end to calculate dg lighting report data --------------------------------
        alarm ON ( sdl.store = alarm.store
                   AND sdl.alarm_monitoring_date = alarm.alarm_monitoring_date )
        WHEN MATCHED THEN UPDATE
        SET sdl.lights_on_too_long = alarm.lights_on_too_long,
            sdl.excessive_light_cycles = alarm.excessive_light_cycles,
            sdl.lights_on_too_long_apps = alarm.lights_on_too_long_apps,
            sdl.excessive_light_cycles_apps = alarm.excessive_light_cycles_apps,
            sdl.total_alarms = alarm.event_count,
            modified_on = currentgmttime
        WHEN NOT MATCHED THEN
        INSERT (
            store,
            alarm_monitoring_date,
            store_chain,
            lights_on_too_long,
            excessive_light_cycles,
            lights_on_too_long_apps,
            excessive_light_cycles_apps,
            total_alarms,
            created_on,
            created_by,
            modified_on,
            modified_by )
        VALUES
            ( alarm.store,
              alarm.alarm_monitoring_date,
              alarm.store_chain,
              alarm.lights_on_too_long,
              alarm.excessive_light_cycles,
              alarm.lights_on_too_long_apps,
              alarm.excessive_light_cycles_apps,
              alarm.event_count,
              currentgmttime,
            'PLSQL',
              currentgmttime,
            'PLSQL' );
       commit;
    END dg_lighting_proc;

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

END sf_dg_lighting_pkg;