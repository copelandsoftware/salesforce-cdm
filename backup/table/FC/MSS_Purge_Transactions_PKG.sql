create or replace PACKAGE BODY     MSS_Purge_Transactions_PKG
AS
   -- Following are the package body level global variables

   g_commit_record_count     NUMBER      := 20000;
   g_limit_rows              NUMBER      := 1000;
   g_conc_request_id         NUMBER;
   g_org_id                  NUMBER;
   g_user_id                 NUMBER;
   g_login_id                NUMBER;

   TYPE g_char100_type       IS TABLE OF VARCHAR2(100)  INDEX BY BINARY_INTEGER;
   TYPE g_char2000_type      IS TABLE OF VARCHAR2(2000) INDEX BY BINARY_INTEGER;
   TYPE g_number_type        IS TABLE OF NUMBER     INDEX BY BINARY_INTEGER;

   g_table_name              g_char100_type;
   g_link_with               g_char100_type;
   g_link_key                g_char100_type;
   g_where_clause            g_char2000_type;
   g_total_rows            g_number_type;
   g_total_time        g_number_type;

   g_purge_data        g_number_type;
   g_sc_purge_data     g_number_type;
   g_purge_transaction_excep   EXCEPTION;

   v_file_name                  UTL_FILE.FILE_TYPE;
   g_directory                  VARCHAR2(130) := 'MSSPURGEPROC';
   g_info_msz                   VARCHAR2 (2000);
   v_return_status              VARCHAR2 (1);

--Gary Sun Added the Procedure LOG_FILE to create a Dynamic file where we can write the log of the Purge Package
PROCEDURE LOG_FILE (directory_name IN VARCHAR2,file_name OUT UTL_FILE.FILE_TYPE, o_return_status OUT VARCHAR2) AS

log_file UTL_FILE.FILE_TYPE;

BEGIN
log_file := UTL_FILE.FOPEN(directory_name,'MSSR_PURGEPROC'||'.log','a');
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

--Gary Sun Added the Procedure WRITE_LOG_FILE to write the log of the Channeling Package

 PROCEDURE WRITE_LOG_FILE (file_name IN UTL_FILE.FILE_TYPE, info IN VARCHAR2,o_return_status OUT VARCHAR2 ) AS
BEGIN

--UTL_FILE.PUT_LINE(file_name,info);

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

--Gary Sun Added the Procedure CLOSE_LOG_FILE to write the log of the Channeling Package

PROCEDURE CLOSE_LOG_FILE (file_name IN UTL_FILE.FILE_TYPE) AS
BEGIN
 IF utl_file.is_open(file_name) THEN
    utl_file.fclose_all;
 END IF;
END CLOSE_LOG_FILE;

PROCEDURE  Cache_Purge_Tables(
  o_return_status OUT VARCHAR2
  ) IS

    CURSOR C_Purge_Tab IS
  SELECT  table_name,
    link_with,
    link_key,
    where_clause,
    0,
    0
  FROM  MSS_PURGE_TABLES
  WHERE enabled_flag = 'Y'
  ORDER BY  link_with,delete_order;          --Modified by Mritunjay Sinha on 23,August 2012
BEGIN

   OPEN  C_Purge_Tab;
   FETCH C_Purge_Tab BULK COLLECT INTO g_table_name, g_link_with, g_link_key, g_where_clause, g_total_rows, g_total_time;
   CLOSE C_Purge_Tab;

   o_return_status := 'S';

EXCEPTION

    WHEN OTHERS THEN

        IF C_Purge_Tab%ISOPEN THEN
          CLOSE C_Purge_Tab;
        END IF;

        --DBMS_OUTPUT.put_line(    'Unhandled exception in Cache_Purge_Tables');
        DBMS_OUTPUT.put_line('error is =>'|| SQLERRM);
        g_info_msz :='Unhandled exception in Cache_Purge_Tables error is =>'|| SQLERRM;
        WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
        o_return_status := 'U';

END Cache_Purge_Tables;


PROCEDURE  Purge_Tables(  in_purge_type IN  VARCHAR2,
  												o_return_status OUT   VARCHAR2
  										 ) IS

  l_delete_sql  VARCHAR2(3000);
  l_start_time  NUMBER;

BEGIN


   --DBMS_OUTPUT.put_line('Purge_Tables procedure is invoked with rows : '||g_purge_data.COUNT);
   g_info_msz :='Purge_Tables procedure is invoked with rows : '||g_purge_data.COUNT;
   WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;

   FOR i IN 1 .. g_table_name.COUNT LOOP

  	IF g_link_with(i) = in_purge_type THEN

    		l_start_time := DBMS_UTILITY.GET_TIME;

    		IF g_where_clause(i) IS NULL THEN
      			l_delete_sql := 'DELETE FROM '||g_table_name(i)||' WHERE '||g_link_key(i)||' = :b1';
    		ELSE
      			l_delete_sql := 'DELETE FROM '||g_table_name(i)||' WHERE '||g_where_clause(i);
    		END IF;

     		--DBMS_OUTPUT.put_line('Performing the operation on table : '||g_table_name(i));
     		--DBMS_OUTPUT.put_line('SQL : '||l_delete_sql);
        g_info_msz :='Performing the operation on table : '||g_table_name(i)||
                     chr(13) || chr(10)||'SQL : '||l_delete_sql;
        WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;

    		FORALL j IN 1 .. g_purge_data.COUNT
      		EXECUTE IMMEDIATE l_delete_sql USING g_purge_data(j);

      	--DBMS_OUTPUT.put_line('Rows deleted                      : '||NVL(SQL%ROWCOUNT, 0));

        g_info_msz :='Rows deleted                      : '||NVL(SQL%ROWCOUNT, 0);
        WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
    		g_total_rows(i) := g_total_rows(i) + NVL(SQL%ROWCOUNT, 0);

    		g_total_time(i) := g_total_time(i) + ((DBMS_UTILITY.GET_TIME - l_start_time)/100);

  	END IF;

   END LOOP;

   COMMIT;

  /* chandra dec,19,2012
  	IF NVL(SQL%ROWCOUNT, 0) > 0 THEN
      COMMIT;
   	END IF;

	*/
   o_return_status := 'S';

EXCEPTION

  WHEN OTHERS THEN

        --DBMS_OUTPUT.put_line( 'Unhandled exception in Purge_Tables');
        --DBMS_OUTPUT.put_line( 'error is  =>'|| SQLERRM);

        g_info_msz :='Unhandled exception in Purge_Tables'||
                     chr(13) || chr(10)||'error is  =>'|| SQLERRM;
        WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
        o_return_status := 'U';
  			ROLLBACK;

END Purge_Tables;
------------------------------------------
--Purge site condition data
PROCEDURE  Purge_SC_Tables(  in_purge_type IN  VARCHAR2,
  												o_return_status OUT   VARCHAR2
  										 ) IS

  l_delete_sql  VARCHAR2(3000);
  l_start_time  NUMBER;

BEGIN


   --DBMS_OUTPUT.put_line('Purge_SC_Tables procedure is invoked with rows : '||g_sc_purge_data.COUNT);
   g_info_msz :='Purge_SC_Tables procedure is invoked with rows : '||g_sc_purge_data.COUNT;
   WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;

   FOR i IN 1 .. g_table_name.COUNT LOOP

  	IF g_link_with(i) = in_purge_type THEN

    		l_start_time := DBMS_UTILITY.GET_TIME;

    		IF g_where_clause(i) IS NULL THEN
      			l_delete_sql := 'DELETE FROM '||g_table_name(i)||' WHERE '||g_link_key(i)||' = :b1';
    		ELSE
      			l_delete_sql := 'DELETE FROM '||g_table_name(i)||' WHERE '||g_where_clause(i);
    		END IF;

     		--DBMS_OUTPUT.put_line('Performing the operation on table : '||g_table_name(i));
     		--DBMS_OUTPUT.put_line('SQL : '||l_delete_sql);
        g_info_msz :='Performing the operation on table : '||g_table_name(i)||
                     chr(13) || chr(10)||'SQL : '||l_delete_sql;
        WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;

    		FORALL j IN 1 .. g_sc_purge_data.COUNT
      		EXECUTE IMMEDIATE l_delete_sql USING g_sc_purge_data(j);

      	--DBMS_OUTPUT.put_line('Rows deleted                      : '||NVL(SQL%ROWCOUNT, 0));

        g_info_msz :='Rows deleted                      : '||NVL(SQL%ROWCOUNT, 0);
        WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
    		g_total_rows(i) := g_total_rows(i) + NVL(SQL%ROWCOUNT, 0);

    		g_total_time(i) := g_total_time(i) + ((DBMS_UTILITY.GET_TIME - l_start_time)/100);

  	END IF;

   END LOOP;

   COMMIT;

  /* chandra dec,19,2012
  	IF NVL(SQL%ROWCOUNT, 0) > 0 THEN
      COMMIT;
   	END IF;

	*/
   o_return_status := 'S';

EXCEPTION

  WHEN OTHERS THEN

        --DBMS_OUTPUT.put_line( 'Unhandled exception in Purge_Tables');
        --DBMS_OUTPUT.put_line( 'error is  =>'|| SQLERRM);

        g_info_msz :='Unhandled exception in Purge_Tables'||
                     chr(13) || chr(10)||'error is  =>'|| SQLERRM;
        WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
        o_return_status := 'U';
  			ROLLBACK;

END Purge_SC_Tables;

------------------------------------------

PROCEDURE Purge_Transactions_Data    (in_delete_date    IN     DATE,
                                      in_customer_id    IN     NUMBER,
                                      in_evt_type       IN     VARCHAR2,
                                      o_return_status   OUT VARCHAR2
                                      )  IS

   CURSOR cur_incident_alarm( p_evt_type VARCHAR2)  IS
   SELECT
   		ALARM_ID
   FROM
   		MSS_NORM_ALARM mna
   WHERE
   		mna.processed_flag IN ('P', 'Y')
      AND mna.cust_id = NVL (in_customer_id, cust_id)
      AND UPPER (nvl(mna.event_type,p_evt_type)) =
               UPPER (NVL (in_evt_type, nvl(mna.event_type,p_evt_type)))
      AND UPPER (mna.current_status) = 'RESOLVED'
      AND mna.CREATED_ON < in_delete_date
      AND EXISTS ( 	SELECT
      								mwo.WORK_ORDER_ID
                  	FROM
                  		MSS_WORK_ORDER mwo
                 		WHERE
                 			mwo.wo_status = 'Sys Closed'
                      AND upper(mwo.current_status) = 'RESOLVED'
                      AND MWO.ALARM_ID = MNA.ALARM_ID
                  )
		UNION
		SELECT
				ALARM_ID
  	FROM
  			MSS_NORM_ALARM mna
 		WHERE
 				mna.processed_flag IN ('P', 'Y')
        AND mna.cust_id = NVL (in_customer_id, cust_id)
        AND UPPER (nvl(mna.event_type,p_evt_type)) =
               UPPER (NVL (in_evt_type, nvl(mna.event_type,p_evt_type)))
        AND UPPER (mna.current_status) = 'RESOLVED'
        AND mna.CREATED_ON < in_delete_date
        AND NOT EXISTS (	SELECT
        											mwo.WORK_ORDER_ID
                          FROM
                          	MSS_WORK_ORDER mwo
                          WHERE
                          	MWO.ALARM_ID = MNA.ALARM_ID
                        );


    CURSOR cur_tasks_alarm( p_evt_type VARCHAR2) IS
   	SELECT
   			mwo.work_order_id
   	FROM
   			MSS_WORK_ORDER mwo
   	WHERE
   		UPPER((DECODE(EVENT_TYPE, 'Alarm', 'Alarm','SR-Phone'))) = UPPER(NVL(in_evt_type, DECODE(EVENT_TYPE, 'Alarm', 'Alarm','SR-Phone')))
   		AND        mwo.cust_id               = NVL(in_customer_id, mwo.CUST_ID)
   		AND        mwo.wo_status             = 'Sys Closed'
   		AND        UPPER(mwo.current_status) = 'RESOLVED'
   		AND        mwo.CREATED_ON            < in_delete_date;

      CURSOR cur_site_condition(p_sc_delete_date DATE) IS
      SELECT
        DISTINCT ALARM_ID
      FROM
        MSS_SITE_CONDITION msc
      WHERE
        msc.time_captured<p_sc_delete_date
        OR msc.time_captured>trunc(sysdate)+1;




      v_total_time          NUMBER := 0;
      v_total_rows_purged   NUMBER := 0;
      v_return_status       VARCHAR2 (1);
      v_commit_count        NUMBER := 0;
      --Added By Mrit on 22 Oct 2012
      l_start_time1  NUMBER;
      v_total_time1  NUMBER   := 0;
      v_total_rows  NUMBER    := 0;
      --Added By Mrit on 27 Nov 2012
      l_start_time2   NUMBER      ;
      v_total_time2   NUMBER  := 0;
      v_total_rows1   NUMBER  := 0;

	    --MV Transation log, add by Gary
      mv_delete_date DATE;
      --Site Condition, add by Gary
      sc_delete_date DATE;
      -- MSS_AUDIT_LOG, add by Gary
      al_delete_date DATE;

       --DG HVAC, add by Gary
      dg_delete_date DATE;
      
      --sf_site_LC
      lc_delete_date DATE :=trunc(sysdate)-6;


BEGIN
    mv_delete_date:=in_delete_date;
    sc_delete_date:=in_delete_date;
    al_delete_date:= in_delete_date;
    dg_delete_date:= in_delete_date;

    FOR MSSYSCONFIG IN (
	SELECT TO_NUMBER(SYS_CONFIG_VALUE) AS KEEPDAYS
	FROM MSS_SYS_CONFIG
	WHERE SYS_CONFIG_TYPE_CD='MVTransactionLog' AND SYS_CONFIG_CD='KeepDays'
	)LOOP
	mv_delete_date:=trunc(sysdate)-MSSYSCONFIG.KEEPDAYS+1;
	END LOOP;

    FOR MSSYSCONFIG1 IN (
	SELECT TO_NUMBER(SYS_CONFIG_VALUE) AS KEEPDAYS
	FROM MSS_SYS_CONFIG
	WHERE SYS_CONFIG_TYPE_CD='PurgeSCData' AND SYS_CONFIG_CD='KeepDays'
	)LOOP
	sc_delete_date:=trunc(sysdate)-MSSYSCONFIG1.KEEPDAYS+1;
	END LOOP;

    FOR MSSYSCONFIG2 IN (
	SELECT TO_NUMBER(SYS_CONFIG_VALUE) AS KEEPDAYS
	FROM MSS_SYS_CONFIG
	WHERE SYS_CONFIG_TYPE_CD='PurgeAuditLogData' AND SYS_CONFIG_CD='KeepDays'
	)LOOP
	al_delete_date:=trunc(sysdate)-MSSYSCONFIG2.KEEPDAYS+1;
	END LOOP;

    FOR MSSYSCONFIG3 IN (
	SELECT TO_NUMBER(SYS_CONFIG_VALUE) AS KEEPDAYS
	FROM MSS_SYS_CONFIG
	WHERE SYS_CONFIG_TYPE_CD='DGInnovationData' AND SYS_CONFIG_CD='KeepDays'
	)LOOP
	dg_delete_date:=trunc(sysdate)-MSSYSCONFIG3.KEEPDAYS+1;
	END LOOP;

  	Cache_Purge_Tables(
    									o_return_status => v_return_status
    								);

   	IF v_return_status <> 'S' THEN
  			RAISE g_purge_transaction_excep;
   	END IF;

    --DBMS_OUTPUT.put_line ('-------------------------------------------');
   	--DBMS_OUTPUT.put_line ('Performing the purging operation for Site Condition');
   	--DBMS_OUTPUT.put_line ('-------------------------------------------');
   	--DBMS_OUTPUT.put_line ('Delete Date    : ' || in_delete_date);

    g_info_msz :='-------------------------------------------'||
                     chr(13) || chr(10)||'Performing the purging operation for Site Condition'||
                     chr(13) || chr(10)||'-------------------------------------------';
        WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;

    OPEN cur_site_condition(sc_delete_date);
    LOOP
      FETCH cur_site_condition BULK COLLECT INTO g_sc_purge_data LIMIT g_limit_rows;
              Purge_SC_Tables(  in_purge_type => 'SC',
   	                  o_return_status => v_return_status
   	           		);

      IF v_return_status <> 'S' THEN
         RAISE g_purge_transaction_excep;
      END IF;

      EXIT WHEN cur_site_condition%NOTFOUND;

  	END LOOP;

    CLOSE cur_site_condition;
    ----------------------------------------
   	--DBMS_OUTPUT.put_line ('-------------------------------------------');
   	--DBMS_OUTPUT.put_line ('Performing the purging operation for ALARM');
   	--DBMS_OUTPUT.put_line ('-------------------------------------------');
   	--DBMS_OUTPUT.put_line ('Delete Date    : ' || in_delete_date);

    g_info_msz :='-------------------------------------------'||
                     chr(13) || chr(10)||'Performing the purging operation for ALARM'||
                     chr(13) || chr(10)||'-------------------------------------------';
        WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
   	--Processing the Alarm data
   	OPEN cur_incident_alarm(UPPER(NVL(in_evt_type,'ALARM'))) ;

   	LOOP

   	   FETCH cur_incident_alarm BULK COLLECT INTO g_Purge_data LIMIT g_limit_rows;

					IF g_Purge_data.COUNT > 0 THEN

   		 				FORALL j IN 1 .. g_purge_data.COUNT
   	    				UPDATE  MSS_NORM_ALARM  SET   CURRENT_ALARM_ACTION_ID = NULL ,LAST_ALARM_ACTION_ID = NULL  WHERE  alarm_id = g_Purge_data(j);

   		 				FORALL j IN 1 .. g_purge_data.COUNT
   	    				UPDATE  MSS_WORK_ORDER SET  CURRENT_WO_ACTION_ID = NULL ,LAST_WO_ACTION_ID = NULL WHERE  alarm_id = g_Purge_data(j);

							COMMIT;

					END IF;

   	   Purge_Tables(  in_purge_type => 'ALARM',
   	                  o_return_status => v_return_status
   	           		);


   	   IF v_return_status <> 'S' THEN
         RAISE g_purge_transaction_excep;
   	   END IF;

   	   EXIT WHEN cur_incident_alarm%NOTFOUND;

   	END LOOP;

   	CLOSE cur_incident_alarm;

   	--DBMS_OUTPUT.put_line ('Completed Successfully');
   	--DBMS_OUTPUT.put_line ('-------------------------------------------');
   	--DBMS_OUTPUT.put_line ('Performing the purging operation for SR');
   	--DBMS_OUTPUT.put_line ('Delete Date    : ' || in_delete_date);

    g_info_msz :='Completed Successfully'||
                     chr(13) || chr(10)||'-------------------------------------------'||
                     chr(13) || chr(10)||'Performing the purging operation for SR';
        WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
   	l_start_time1 := DBMS_UTILITY.GET_TIME;

   	DELETE FROM
   			MSS_SERVICE_REQUEST msr
   	WHERE
   		 NOT EXISTS (	SELECT
   	 								NVL (sr_reference, 1)
               		FROM
               			mss_norm_alarm mna
               		WHERE
               			mna.sr_reference=msr.service_request_id
               	);

   	--DBMS_OUTPUT.put_line (  TO_CHAR (SQL%ROWCOUNT) || ' rows deleted from MSS_ MSS_SERVICE_REQUEST'   );
    g_info_msz :=TO_CHAR (SQL%ROWCOUNT) || ' rows deleted from MSS_ MSS_SERVICE_REQUEST';
    WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
		IF TO_CHAR (SQL%ROWCOUNT) > 0 THEN
   		v_total_time1 := v_total_time1 + ( (DBMS_UTILITY.GET_TIME - l_start_time1) / 100);
   		v_total_rows  := v_total_rows + NVL (SQL%ROWCOUNT, 0);

   		DBMS_OUTPUT.put_line ('v_total_time1 ==>' || v_total_time1);
   		DBMS_OUTPUT.put_line ('v_total_rows  ==>' || v_total_rows);
      g_info_msz :='v_total_time1 ==>' || v_total_time1||
                     chr(13) || chr(10)||'v_total_rows  ==>' || v_total_rows;
      WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
		END IF;

  	--DBMS_OUTPUT.put_line ('Completed Successfully');
  	--DBMS_OUTPUT.put_line ('-------------------------------------------');
  	--DBMS_OUTPUT.put_line ('Performing the purging operation for SR-PHONE');
  	--DBMS_OUTPUT.put_line ('Delete Date    : ' || in_delete_date);
    g_info_msz :='Completed Successfully'||
                    chr(13) || chr(10)||'------------------------------------------------'||
                    chr(13) || chr(10)||'Performing the purging operation for SR-PHONE';

    WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;

  	OPEN cur_tasks_alarm(UPPER(NVL(in_evt_type,'SR-PHONE')));

  		LOOP

     		FETCH cur_tasks_alarm  BULK COLLECT INTO   g_Purge_data  LIMIT g_limit_rows;

				IF g_Purge_data.COUNT > 0 THEN

     				FORALL i in  1 .. g_Purge_data.COUNT
       				UPDATE MSS_WORK_ORDER SET CURRENT_WO_ACTION_ID = NULL  ,LAST_WO_ACTION_ID = NULL  WHERE work_order_id = g_Purge_data(i);

        		COMMIT;

        END IF;

       	Purge_Tables( in_purge_type   => 'TASK',
                     	o_return_status => v_return_status
              			);

        IF v_return_status <> 'S' THEN
         		RAISE g_purge_transaction_excep;
        END IF;

	      EXIT WHEN cur_tasks_alarm%NOTFOUND;

   		END LOOP;

   		CLOSE cur_tasks_alarm;

      --DBMS_OUTPUT.put_line ('Completed Successfully');
      --DBMS_OUTPUT.put_line ('-------------------------------------------');
      g_info_msz :='Completed Successfully'||
                     chr(13) || chr(10)||'-------------------------------------------';
      WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
			--Added below piece of code to delete all the records of alarms from MSS_RAW_ALARM table
			--Mritunjay Sinha 27,November 2012
   		l_start_time2 := DBMS_UTILITY.GET_TIME;

      DELETE FROM
   	MSS_RAW_ALARM mra
      WHERE
      	NVL (processed_flag, 'P') = 'P'
        AND     mra.CREATED_ON            < in_delete_date
        AND     NOT EXISTS ( SELECT
         			alarm_id
                             FROM
                               mss_norm_alarm mna
                             WHERE
                               mna.alarm_id = mra.alarm_id
                             );

   		--DBMS_OUTPUT.put_line ( TO_CHAR (SQL%ROWCOUNT) || ' rows deleted from MSS_ MSS_SERVICE_REQUEST' );
      g_info_msz :=TO_CHAR (SQL%ROWCOUNT) || ' rows deleted from MSS_ MSS_SERVICE_REQUEST';
      WRITE_LOG_FILE(file_name => v_file_name,info     => g_info_msz,o_return_status  => v_return_status) ;
      IF TO_CHAR (SQL%ROWCOUNT) > 0 THEN
	v_total_time2 := v_total_time2 + ( (DBMS_UTILITY.GET_TIME - l_start_time2) / 100);
   	v_total_rows1 := v_total_rows1 + NVL (SQL%ROWCOUNT, 0);

   	--DBMS_OUTPUT.put_line ('v_total_time2 ==>' || v_total_time2);
   	--DBMS_OUTPUT.put_line ('v_total_rows1 ==>' || v_total_rows1);
        g_info_msz :='v_total_time2 ==>' || v_total_time2||chr(13) || chr(10)||'v_total_rows1 ==>' || v_total_rows1;
        WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
        END IF;

	--delete SF notes, alarm table
	DELETE FROM JAM.SF_ALARM_ACTION_STATUS_NOTES sna
	WHERE
	 EXISTS (SELECT 1 FROM jam.sf_norm_alarm na WHERE Created_on < in_delete_date
	         AND na.alarm_id = sna.alarm_id);

    	g_info_msz :=TO_CHAR (SQL%ROWCOUNT) || ' rows deleted from SF_ALARM_ACTION_STATUS_NOTES';
    	WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;

	DELETE FROM jam.sf_norm_alarm na
	WHERE Created_on < in_delete_date;

    	g_info_msz :=TO_CHAR (SQL%ROWCOUNT) || ' rows deleted from SF_NORM_ALARM ';
    	WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;

	--DBMS_OUTPUT.put_line ('Completed Successfully');
   	--DBMS_OUTPUT.put_line ('-------------------------------------------');
   	--DBMS_OUTPUT.put_line ('Performing the purging operation for MSS_EXT_COMM_REQ_LOG');
   	--DBMS_OUTPUT.put_line ('Delete Date    : ' || in_delete_date);

    g_info_msz :='Completed Successfully'||
                     chr(13) || chr(10)||'-------------------------------------------'||
                     chr(13) || chr(10)||'Performing the purging operation for MSS_EXT_COMM_REQ_LOG';
        WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
   	l_start_time1 := DBMS_UTILITY.GET_TIME;

   	DELETE FROM
   			MSS_EXT_COMM_REQ_LOG mecrl
   	WHERE   mecrl.CREATED_ON < mv_delete_date;

    --delete SF_SITE_LAST_COMMUNICATION_Q
   	DELETE FROM
   			SF_SITE_LAST_COMMUNICATION_Q sfsitelc
   	WHERE   sfsitelc.CREATED_ON < lc_delete_date and sfsitelc.PROCESSED_FLAG='Y';
    
        
    g_info_msz :='Completed Successfully'||
                     chr(13) || chr(10)||'-------------------------------------------'||
                     chr(13) || chr(10)||'Performing the purging operation for SF_SITE_LAST_COMMUNICATION_Q';
        WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
   	l_start_time1 := DBMS_UTILITY.GET_TIME;
    

   	--DBMS_OUTPUT.put_line (  TO_CHAR (SQL%ROWCOUNT) || ' rows deleted from MSS_EXT_COMM_REQ_LOG'   );
    g_info_msz :=TO_CHAR (SQL%ROWCOUNT) || ' rows deleted from MSS_EXT_COMM_REQ_LOG';
    WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;

    DELETE FROM
   			MSS_AUDIT_LOG mal
   	WHERE   mal.CREATED_ON < al_delete_date;

   	--DBMS_OUTPUT.put_line (  TO_CHAR (SQL%ROWCOUNT) || ' rows deleted from MSS_AUDIT_LOG'   );
    g_info_msz :=TO_CHAR (SQL%ROWCOUNT) || ' rows deleted from MSS_AUDIT_LOG';
    WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;

    DELETE FROM
   			sf_dg_innovations sdi
   	WHERE   sdi.CREATED_ON < dg_delete_date;

   	--DBMS_OUTPUT.put_line (  TO_CHAR (SQL%ROWCOUNT) || ' rows deleted from sf_dg_innovations'   );
    g_info_msz :=TO_CHAR (SQL%ROWCOUNT) || ' rows deleted from sf_dg_innovations';
    WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;

		IF TO_CHAR (SQL%ROWCOUNT) > 0 THEN
   		v_total_time1 := v_total_time1 + ( (DBMS_UTILITY.GET_TIME - l_start_time1) / 100);
   		v_total_rows  := v_total_rows + NVL (SQL%ROWCOUNT, 0);

   		--DBMS_OUTPUT.put_line ('v_total_time1 ==>' || v_total_time1);
   		--DBMS_OUTPUT.put_line ('v_total_rows  ==>' || v_total_rows);
        g_info_msz :='v_total_time1 ==>' || v_total_time1||
                     chr(13) || chr(10)||'v_total_rows  ==>' || v_total_rows;
        WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
		END IF;




   		DBMS_OUTPUT.put_line('---------------------------------------------------------------------------------');
   		DBMS_OUTPUT.put_line('                              PURGE OPERATION REPORT');
   		DBMS_OUTPUT.put_line('---------------------------------------------------------------------------------');
   		DBMS_OUTPUT.put_line(RPAD('Table Name', 40)||RPAD('Total Rows Deleted', 25)||'Time in Minutes');
   		DBMS_OUTPUT.put_line('---------------------------------------------------------------------------------');
      g_info_msz :='---------------------------------------------------------------------------------'||
                    chr(13) || chr(10)||'                              PURGE OPERATION REPORT'||
                    chr(13) || chr(10)||'---------------------------------------------------------------------------------'||
                    chr(13) || chr(10)||RPAD('Table Name', 40)||RPAD('Total Rows Deleted', 25)||'Time in Minutes'||
                    chr(13) || chr(10)||'---------------------------------------------------------------------------------'||
                    chr(13) || chr(10)||'---------------------------------------------------------------------------------';

      WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
   		FOR i IN g_table_name.FIRST .. g_table_name.LAST LOOP
  			DBMS_OUTPUT.put_line(RPAD(g_table_name(i), 40)||RPAD(g_total_rows(i),25)||ROUND(g_total_time(i)/60, 2));
  			v_total_rows_purged := v_total_rows_purged + g_total_rows(i);
  			v_total_time      := v_total_time + g_total_time(i);
   		END LOOP;

  		DBMS_OUTPUT.put_line('MSS_SERVICE_REQUEST                     '||v_total_rows||'                         '||v_total_time1);
  		DBMS_OUTPUT.put_line('MSS_RAW_ALARM                           '||v_total_rows1||'                        '||v_total_time2);

  		DBMS_OUTPUT.put_line('---------------------------------------------------------------------------------');
  	 	DBMS_OUTPUT.put_line(RPAD('Total rows deleted from all tables', 40)||(v_total_rows_purged+v_total_rows+v_total_rows1));
  		DBMS_OUTPUT.put_line(RPAD('Time taken for the whole execution', 65)||ROUND((v_total_time+v_total_time1+v_total_time2)/60, 2));
  		DBMS_OUTPUT.put_line('---------------------------------------------------------------------------------');
  	 	DBMS_OUTPUT.put_line('                                   END OF REPORT');
  	 	DBMS_OUTPUT.put_line('---------------------------------------------------------------------------------');
	    g_info_msz :='MSS_SERVICE_REQUEST                     '||v_total_rows||'                         '||v_total_time1||
                    chr(13) || chr(10)||'MSS_RAW_ALARM                           '||v_total_rows1||'                        '||v_total_time2||
                    chr(13) || chr(10)||'---------------------------------------------------------------------------------'||
                    chr(13) || chr(10)||RPAD('Total rows deleted from all tables', 40)||(v_total_rows_purged+v_total_rows+v_total_rows1)||
                    chr(13) || chr(10)||RPAD('Time taken for the whole execution', 65)||ROUND((v_total_time+v_total_time1+v_total_time2)/60, 2)||
                    chr(13) || chr(10)||'---------------------------------------------------------------------------------'||
                    chr(13) || chr(10)||'                                   END OF REPORT'||
                    chr(13) || chr(10)||'---------------------------------------------------------------------------------';

      WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;

   		UPDATE
   			MSS_PURGE_TABLES
   		SET
   			last_purge_date = SYSDATE
   		WHERE
   			enabled_flag = 'Y';

   		COMMIT;

   		o_return_status := 'S';

EXCEPTION

   WHEN g_purge_transaction_excep THEN

        IF cur_incident_alarm%ISOPEN THEN
          CLOSE cur_incident_alarm;
        END IF;

        IF cur_tasks_alarm%ISOPEN THEN
          CLOSE cur_tasks_alarm;
        END IF;

        DBMS_OUTPUT.put_line('Execution error in Purge_Transactions_Data');
        g_info_msz :='Execution error in Purge_Transactions_Data';
        WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
        o_return_status := 'E';

  			ROLLBACK;

   WHEN OTHERS THEN

        IF cur_incident_alarm%ISOPEN THEN
          CLOSE cur_incident_alarm;
        END IF;

        IF cur_tasks_alarm%ISOPEN THEN
          CLOSE cur_tasks_alarm;
        END IF;

        DBMS_OUTPUT.put_line('Unhandled exception in Purge_Transactions_Data');
        DBMS_OUTPUT.put_line(SQLERRM);
        g_info_msz :='Unhandled exception in Purge_Transactions_Data'||SUBSTR(SQLERRM,1,1600);
        WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
        o_return_status := 'U';

  			ROLLBACK;

END Purge_Transactions_Data;

PROCEDURE Purge_Transactions (errbuf              OUT VARCHAR2,
                              retcode             OUT VARCHAR2,
                              in_delete_date   		IN  DATE,
                              in_customer_id   		IN  NUMBER,
                              in_evt_type      		IN 	VARCHAR2,
                              in_delete_batch			IN 	NUMBER DEFAULT 1000
                             )   IS

     v_return_status   VARCHAR2 (1);

BEGIN

      LOG_FILE (directory_name   => g_directory
             ,file_name        => v_file_name
             ,o_return_status  => v_return_status);
      g_info_msz := '----------------------------------------------------------------------------------'||
                      chr(13) || chr(10)||'Purge Transactions Package Execution Started @ '||to_char(sysdate,'MM/DD/YYYY HH24:MI:SS')||
                      chr(13) || chr(10)||'----------------------------------------------------------------------------------';
   	  WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
      DBMS_OUTPUT.put_line ('Running the program with following parameters');
   		DBMS_OUTPUT.put_line ('------------------------------------------------');
   		DBMS_OUTPUT.put_line ('Delete Date    : ' || in_delete_date);
   		DBMS_OUTPUT.put_line ('Limit Rows     : ' || g_limit_rows);
   		DBMS_OUTPUT.put_line ('SR Type        : ' ||in_evt_type);
   		DBMS_OUTPUT.put_line ('Limit Rows     : ' ||g_limit_rows);
   		DBMS_OUTPUT.put_line('------------------------------------------------');

      g_info_msz :='Running the program with following parameters'||
                    chr(13) || chr(10)||'------------------------------------------------'||
                    chr(13) || chr(10)||'Delete Date    : ' || in_delete_date||
                    chr(13) || chr(10)||'Limit Rows     : ' || g_limit_rows||
                    chr(13) || chr(10)||'SR Type        : ' ||in_evt_type||
                    chr(13) || chr(10)||'------------------------------------------------';

      WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;

      IF in_delete_date >= SYSDATE  THEN
         DBMS_OUTPUT.put_line ('You cannot delete the future transactions');
         g_info_msz :='You cannot delete the future transactions';
         WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
         RAISE g_purge_transaction_excep;
      END IF;

      IF in_delete_date > (SYSDATE - 10)  THEN
         DBMS_OUTPUT.put_line (  'You cannot delete the last 10 days transactions'  );
         g_info_msz :='You cannot delete the last 10 days transactions';
         WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
         RAISE g_purge_transaction_excep;
      END IF;

			IF in_delete_batch IS NOT NULL THEN
				 g_limit_rows := in_delete_batch;
			END IF;

      Purge_Transactions_Data (	in_delete_date    => in_delete_date + 1,
                               	in_customer_id    => in_customer_id,
                               	in_evt_type       => in_evt_type,
                               	o_return_status   => v_return_status
                              );

      IF v_return_status <> 'S' THEN
         DBMS_OUTPUT.put_line ('Unhandled exception in Purge_Tables' || SQLERRM);
         g_info_msz :='Unhandled exception in Purge_Tables';
         WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
         RAISE g_purge_transaction_excep;
      END IF;

      retcode := 0;
      COMMIT WORK;
      g_info_msz := '----------------------------------------------------------------------------------'||
                         chr(13) || chr(10)||'Purge Transactions Package Execution Completed @  '||to_char(sysdate,'MM/DD/YYYY HH24:MI:SS')||
                         chr(13) || chr(10)||'----------------------------------------------------------------------------------'
                         ;

      WRITE_LOG_FILE(file_name => v_file_name
                       ,info     => g_info_msz
                       ,o_return_status  => v_return_status) ;
      CLOSE_LOG_FILE(v_file_name);
   EXCEPTION
      WHEN g_purge_transaction_excep  THEN
         DBMS_OUTPUT.put_line ('Execution error in Purge_Transactions');
         g_info_msz :='Execution error in Purge_Transactions';
         WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
         retcode := 2;
         CLOSE_LOG_FILE(v_file_name);
         ROLLBACK;

      WHEN OTHERS  THEN
         DBMS_OUTPUT.put_line ('Unhandled exception in Purge_Transactions');
         DBMS_OUTPUT.put_line (SQLERRM);
         g_info_msz :='Unhandled exception in Purge_Transactions'||SUBSTR(SQLERRM,1,1600);
         WRITE_LOG_FILE(file_name => v_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
         retcode := 2;
         CLOSE_LOG_FILE(v_file_name);
         ROLLBACK;

   END Purge_Transactions;

END MSS_Purge_Transactions_PKG;