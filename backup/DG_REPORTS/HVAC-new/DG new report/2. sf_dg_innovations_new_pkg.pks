create or replace PACKAGE sf_dg_innovations_new_pkg AS 

    PROCEDURE dg_innovations_proc (
        errbuf        OUT           VARCHAR2,
        retcode       OUT           VARCHAR2
    );

    FUNCTION F_Get_Timezone(i_timevalue IN DATE) RETURN DATE;

END sf_dg_innovations_new_pkg;