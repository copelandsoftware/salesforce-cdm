create or replace PACKAGE clean_SF_Work_Order_pkg AS 

    PROCEDURE clean_sf_wo_proc (
        errbuf        OUT           VARCHAR2,
        retcode       OUT           VARCHAR2
    );

END clean_SF_Work_Order_pkg;