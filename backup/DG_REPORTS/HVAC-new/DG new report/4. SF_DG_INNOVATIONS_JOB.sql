DECLARE job_exists number;
BEGIN
--DROP IF JOB EXISTS.
	select COUNT(*) INTO job_exists from user_scheduler_jobs
	    where job_name = 'SF_DG_INNOVATIONS_NEW_JOB';
	IF job_exists >= 1
	    THEN dbms_scheduler.drop_job(job_name => 'SF_DG_INNOVATIONS_NEW_JOB');
	END IF;
--CREATE JOB
    dbms_scheduler.create_job
    (job_name => 'SF_DG_INNOVATIONS_NEW_JOB',
    job_type => 'PLSQL_BLOCK',
    job_action=> 'DECLARE
					  ERRBUF VARCHAR2(200);
					  RETCODE VARCHAR2(200);
				  BEGIN

					  SF_DG_INNOVATIONS_NEW_PKG.DG_INNOVATIONS_PROC(
						ERRBUF => ERRBUF,
						RETCODE => RETCODE
					  );
				  END;',
    start_date=> TRUNC(SYSDATE +1) + 1/24,
    repeat_interval=> 'FREQ=HOURLY; INTERVAL=24',
    enabled=>true,
    auto_drop=>false,
    comments=>  'Job start date is ' || to_char(trunc(sysdate),'MM/DD/YYYY') ||', running per day.'
    );
END;
/