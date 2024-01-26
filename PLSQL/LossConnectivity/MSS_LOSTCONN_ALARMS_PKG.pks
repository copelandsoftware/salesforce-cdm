create or replace PACKAGE     MSS_LOSTCONN_ALARMS_PKG
AS

 PROCEDURE  send_mail(  errbuf          OUT VARCHAR2,
                        retcode         OUT VARCHAR2,
                        p_recipient     IN VARCHAR2,
                        p_subject       IN VARCHAR2
                                );

END MSS_LOSTCONN_ALARMS_PKG;