select
store as "Store",
decode(exceed_55,0,null,exceed_55) as "Count of Below 55F Space Temp Alarms",
decode(exceed_80,0,null,exceed_80) as "Count of Exceeded 80F Space Temp Alarms",
decode(exceed_85,0,null,exceed_85) as "Count of Exceeded 85F Space Temp Alarms",
decode(exceed_setpoint,0,null,exceed_setpoint) as "Count of Space Temp Not Achieved Alarms",
decode(exceed_limit,0,null,exceed_limit) "Count of Running and Not Cooling Units",
exceed_55_unit as "Units Below 55F Space Temp",
exceed_80_unit as "Units Exceeded 80F Space Temp",
exceed_85_unit as "Units Exceeded 85F Space Temp",
exceed_setpoint_unit as "Units Space Temp Not Achieved",
exceed_limit_unit as "Units Running and Not Cooling",
Number_of_Units_Down as "Number of Units Down",
TOTAL_UNITS as "Total Units"
from
(
           SELECT
                      a.*,
                      regexp_count(exceed_limit_unit, ',') + 1 AS number_of_units_down,
                      total_units
                  FROM
                      (
                          SELECT
                              TRIM(replace(ss.sf_site_name, 'Dollar General ')) AS store,
                              trunc(sna.time_occurred) AS alarm_monitoring_date,
                              sc.sf_cust_name AS store_chain,
                              COUNT(DISTINCT
                                  CASE
                                      WHEN (substr(sna.field3, 1, 2) = '55'
                                           AND (sna.descr = 'Occupied Hi Limit Exceeded' or sna.descr like 'Unoccupied Low Limit Exceeded Space Temp %')) THEN    -- add 'Unoccupied Low Limit Exceeded Space Temp %' for SS
                                          sna.alarm_id
                                  END
                              ) exceed_55,  
                              COUNT(DISTINCT
                                  CASE
                                      WHEN(substr(sna.field3, 1, 2) = '80'
                                           AND (sna.descr = 'High Limit Alarm' or sna.descr like 'Occupied Hi Limit Exceeded Space Temp %')) THEN   -- add 'Occupied Hi Limit Exceeded Space Temp %' for SS
                                          sna.alarm_id
                                  END
                              ) exceed_80,
                              COUNT(DISTINCT
                                  CASE
                                      WHEN(substr(sna.field3, 1, 2) = '85'
                                           AND (sna.descr = 'Occupied Hi Limit Exceeded' )) THEN 
                                          sna.alarm_id
                                  END
                              ) exceed_85,
                              COUNT(DISTINCT
                                  CASE
                                      WHEN(sna.descr = 'Differential Limit Exceeded') THEN
                                          sna.alarm_id
                                  END
                              ) exceed_limit,
                              COUNT(DISTINCT
                                  CASE
                                      WHEN(sna.descr = 'Appl not keeping set point') THEN
                                          sna.alarm_id
                                  END
                              ) exceed_setpoint,
                              dbms_lob.substr(regexp_replace(LISTAGG(
                                  CASE
                                      WHEN(substr(sna.field3, 1, 2) = '55' 
                                           AND sna.descr = 'Occupied Hi Limit Exceeded') THEN
                                          replace(sna.source,'HVAC UNIT #')
                                      WHEN(substr(sna.field3, 1, 2) = '55' 
                                           AND sna.descr like 'Unoccupied Low Limit Exceeded Space Temp %') THEN   -- add 'Unoccupied Low Limit Exceeded Space Temp %' for SS
                                          replace(sna.field5,'Temp')
                                      ELSE
                                          NULL
                                  END
                              ,',') WITHIN GROUP (ORDER BY 1),'([^,]+)(,\1)+', '\1'), 4000, 1) exceed_55_unit,
                              dbms_lob.substr(regexp_replace(LISTAGG(
                                  CASE
                                      WHEN(substr(sna.field3, 1, 2) = '80'
                                           AND sna.descr = 'High Limit Alarm') THEN
                                          replace(sna.source,'HVAC UNIT #')
                                      WHEN(substr(sna.field3, 1, 2) = '80'
                                           AND sna.descr like 'Occupied Hi Limit Exceeded Space Temp %') THEN    -- add 'Occupied Hi Limit Exceeded Space Temp %' for SS
                                          replace(sna.field5,'Temp')
                                      ELSE
                                          NULL
                                  END
                              ,',') WITHIN GROUP (ORDER BY 1),'([^,]+)(,\1)+', '\1'), 4000, 1) exceed_80_unit,
                              dbms_lob.substr(regexp_replace(LISTAGG(
                                  CASE
                                      WHEN(substr(sna.field3, 1, 2) = '85'
                                           AND sna.descr = 'Occupied Hi Limit Exceeded') THEN
                                          replace(sna.source,'HVAC UNIT #')
                                      
                                      ELSE
                                          NULL
                                  END
                              ,',') WITHIN GROUP (ORDER BY 1),'([^,]+)(,\1)+', '\1'), 4000, 1) exceed_85_unit,
                              dbms_lob.substr(replace(regexp_replace(LISTAGG(
                                  CASE
                                      WHEN(sna.descr = 'Differential Limit Exceeded') THEN
                                          sna.source
                                      ELSE
                                          NULL
                                  END
                              ,',') WITHIN GROUP (ORDER BY 1),'([^,]+)(,\1)+', '\1'), 'HVAC UNIT #'), 4000, 1) exceed_limit_unit,
                              dbms_lob.substr(replace(regexp_replace(LISTAGG(
                                  CASE
                                      WHEN(sna.descr = 'Appl not keeping set point') THEN
                                          sna.source
                                      ELSE
                                          NULL
                                  END
                              ,',') WITHIN GROUP (ORDER BY 1),'([^,]+)(,\1)+', '\1'), 'HVAC UNIT #'), 4000, 1) exceed_setpoint_unit,
                              COUNT(DISTINCT(
                                  CASE
                                      WHEN(sna.descr = 'Appl not keeping set point') THEN
                                          sna.alarm_id
                                      WHEN(sna.descr = 'Differential Limit Exceeded') THEN
                                          sna.alarm_id
                                      WHEN(substr(sna.field3, 1, 2) = '55'
                                           AND (sna.descr = 'Occupied Hi Limit Exceeded') or sna.descr like 'Unoccupied Low Limit Exceeded Space Temp %') THEN    -- add 'Unoccupied Low Limit Exceeded Space Temp %' for SS
                                          sna.alarm_id
                                      WHEN(substr(sna.field3, 1, 2) = '85'
                                           AND (sna.descr = 'Occupied Hi Limit Exceeded') ) THEN
                                          sna.alarm_id
                                      WHEN(substr(sna.field3, 1, 2) = '80'
                                           AND (sna.descr = 'High Limit Alarm' or sna.descr like 'Occupied Hi Limit Exceeded Space Temp %')) THEN       -- add 'Occupied Hi Limit Exceeded Space Temp %' for SS
                                          sna.alarm_id
                                      ELSE
                                          NULL
                                  END
                              )) event_count,
							  ss.state
                          FROM
                              sf_site         ss,
                              sf_customer     sc,
                              sf_norm_alarm   sna
                          WHERE
                              ss.sf_cust_id = sc.sf_cust_id
                              AND sc.sf_cust_name LIKE 'Dollar General Traditional%'
                              AND ss.sf_site_id = sna.sf_site_id
                              AND ( sna.time_occurred ) > trunc(sysdate - 1)
                              AND ( sna.time_occurred ) < trunc(sysdate+1)
                              AND (sna.source LIKE 'HVAC UNIT%' or sna.source ='RTU Setpoints')
                              AND sna.field12 = '0'
                              AND sna.descr IN (
                                  'Differential Limit Exceeded',
                                  'Appl not keeping set point',
                                  'High Limit Alarm',
                                  'Occupied Hi Limit Exceeded',
                                  'Occupied Hi Limit Exceeded Space Temp 1',
                                  'Occupied Hi Limit Exceeded Space Temp 2',
                                  'Occupied Hi Limit Exceeded Space Temp 3',
                                  'Occupied Hi Limit Exceeded Space Temp 4',
                                  'Occupied Hi Limit Exceeded Space Temp 5',
                                  'Occupied Hi Limit Exceeded Space Temp 6',
                                  'Occupied Hi Limit Exceeded Space Temp 7',
                                  'Occupied Hi Limit Exceeded Space Temp 8',
                                  'Unoccupied Low Limit Exceeded Space Temp 1',
                                  'Unoccupied Low Limit Exceeded Space Temp 2',
                                  'Unoccupied Low Limit Exceeded Space Temp 3',
                                  'Unoccupied Low Limit Exceeded Space Temp 4',
                                  'Unoccupied Low Limit Exceeded Space Temp 5',
                                  'Unoccupied Low Limit Exceeded Space Temp 6',
                                  'Unoccupied Low Limit Exceeded Space Temp 7',
                                  'Unoccupied Low Limit Exceeded Space Temp 8' -- you can change here like ( sna.descr in ('a1','a2') or sna.descr like 'Occupied Hi Limit Exceeded Space Temp %')
                              )
                          GROUP BY
                              ss.sf_site_name,
                              sc.sf_cust_name,
                              trunc(sna.time_occurred),
							  ss.state
                      ) a,
                      DG_INNOVATION_UNITS_LKP_NEW lkp
                  WHERE
                      a.store = to_char(lkp.store(+))
                      AND event_count <> 0
                      )  order by store