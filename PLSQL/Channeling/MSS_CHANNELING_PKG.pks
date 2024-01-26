create or replace PACKAGE MSS_CHANNELING_PKG AS
  PROCEDURE MSS_CHANNELING_PROC( errbuf OUT VARCHAR2,
                                  retcode OUT VARCHAR2
                                 );
  FUNCTION F_Get_Timezone(i_timevalue IN DATE) RETURN DATE;
END MSS_CHANNELING_PKG;