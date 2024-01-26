create or replace PACKAGE sf_dg_lighting_pkg AS 

    PROCEDURE dg_lighting_proc (
        errbuf        OUT           VARCHAR2,
        retcode       OUT           VARCHAR2
    );

    FUNCTION F_Get_Timezone(i_timevalue IN DATE) RETURN DATE;

END sf_dg_lighting_pkg;