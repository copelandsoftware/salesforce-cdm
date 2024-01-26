create or replace PACKAGE     MSS_Purge_Transactions_PKG AS

PROCEDURE  Purge_Transactions(   	errbuf             OUT VARCHAR2,
  																retcode            OUT VARCHAR2,
  																in_delete_date     IN  DATE,
  																in_customer_id     IN  NUMBER,
  																in_evt_type        IN VARCHAR2,
                              		in_delete_batch			IN 	NUMBER DEFAULT 1000
  													 );

END MSS_Purge_Transactions_PKG;