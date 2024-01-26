create or replace PACKAGE BODY clean_SF_Work_Order_pkg AS

    PROCEDURE clean_sf_wo_proc (
        errbuf    OUT   VARCHAR2,
        retcode   OUT   VARCHAR2
    ) AS
    BEGIN
        dbms_output.put_line('----------------------------------------------------------------------------------'
                             || chr(13)
                             || chr(10)
                             || 'clean sf_work_order table Started @ '
                             || to_char(sysdate, 'MM/DD/YYYY HH24:MI:SS')
                             || chr(13)
                             || chr(10)
                             || '----------------------------------------------------------------------------------');
      UPDATE sf_work_order SET last_action = 'Cancel WorkOrder'
      WHERE sf_work_order_id
      in (
        SELECT
         sf_work_order_id
        FROM
         sf_work_order
        WHERE
          ext_wo_exception IS NOT NULL
          AND created_on < sysdate - 30
          AND last_action IN ( 'Confirm' )

         UNION
         
        SELECT
          sf_work_order_id
        FROM
          sf_work_order  wo,
          sf_site        ss
        WHERE
           ss.sf_site_id = wo.sf_site_id
           AND created_on < sysdate - 30
           AND (last_action IN ( 'Dispatch', 'Save', 'Dispatch Resolve','') or last_action is null)
           AND ss.sf_cust_id in ('001f400001STyMoAAL','001f4000005i78SAAQ')
       );
       
    END clean_sf_wo_proc;

END clean_SF_Work_Order_pkg;