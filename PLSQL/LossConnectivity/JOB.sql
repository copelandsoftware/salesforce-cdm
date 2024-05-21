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