DECLARE job_exists number;
BEGIN
--DROP IF JOB EXISTS.
	select COUNT(*) INTO job_exists from user_scheduler_jobs 
	    where job_name = 'SF_AUTOPROCESS_ADM_ALARMS_JOB';
	IF job_exists >= 1
	    THEN dbms_scheduler.drop_job(job_name => 'SF_AUTOPROCESS_ADM_ALARMS_JOB');
	END IF;
--CREATE JOB
    dbms_scheduler.create_job
    (job_name => 'SF_AUTOPROCESS_ADM_ALARMS_JOB',
    job_type => 'PLSQL_BLOCK',
    job_action=> 'DECLARE
					  v_errbuf VARCHAR2(200);
					  v_retcode VARCHAR2(200);
				  BEGIN

					   SF_AUTOPROCESS_ADM_ALARMS_PKG.VALIDATE_ALARM_PROC
                    (errbuf => v_errbuf,
                    retcode => v_retcode
                    ) ;
				  END;',
    start_date=> TRUNC(sysdate-1),
    repeat_interval=> 'FREQ=MINUTELY; INTERVAL=2',
    enabled=>true,
    auto_drop=>false,
    comments=>  'Job start date is ' || to_char(trunc(sysdate),'MM/DD/YYYY') ||', running per day.'
    );
END;
/
