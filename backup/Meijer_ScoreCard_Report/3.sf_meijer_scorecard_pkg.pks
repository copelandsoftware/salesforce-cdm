create or replace PACKAGE sf_meijer_scorecard_pkg AS 

    PROCEDURE meijer_scordcard_proc (
        errbuf        OUT           VARCHAR2,
        retcode       OUT           VARCHAR2
    );

    FUNCTION F_Get_Timezone(i_timevalue IN DATE) RETURN DATE;

END sf_meijer_scorecard_pkg;