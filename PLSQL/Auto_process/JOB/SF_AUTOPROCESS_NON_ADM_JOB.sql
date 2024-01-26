DECLARE job_exists number;
BEGIN
--DROP IF JOB EXISTS.
	select COUNT(*) INTO job_exists from user_scheduler_jobs 
	    where job_name = 'SF_AUTOPROCESS_NON_ADM_CUST';
	IF job_exists >= 1
	    THEN dbms_scheduler.drop_job(job_name => 'SF_AUTOPROCESS_NON_ADM_CUST');
	END IF;
--CREATE JOB
    dbms_scheduler.create_job
    (job_name => 'SF_AUTOPROCESS_NON_ADM_CUST',
    job_type => 'PLSQL_BLOCK',
    job_action=> 'DECLARE
                    v_errbuf varchar(100);
                    v_retcode varchar(100);
                BEGIN
                    SF_AUTOPROCESS_ALARMS_ALL_PKG.VALIDATE_ALARM_PROC
                    (
					admFlag => ''N'',
					errbuf => v_errbuf,
                    retcode => v_retcode
                    ) ;
                END;',
    start_date=> SYSDATE,
    repeat_interval=> 'FREQ=MINUTELY; INTERVAL=3',
    enabled=>true,
    auto_drop=>false,
    comments=>  'Job start date is ' || to_char(trunc(sysdate),'MM/DD/YYYY') ||', running per day.'
    );
END;
/