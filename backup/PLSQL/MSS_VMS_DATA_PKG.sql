create or replace PACKAGE BODY     mss_vms_data_pkg
AS
-- /*******************************************************************************
--                 Copy Rights Reserved Emerson
--                 ERS Project
-- ********************************************************************************
-- Program Name    :  MSSR vms data Extract
-- $Header         :
-- Program Type    :  Procedure
-- File Name       :  mss_vms_data_pkg.sql
-- Reference File  :  MSS-C, xxcs.xxcs_vms_data_pkg
-- Description     : Extraction Program
--                   Called By:
--                   CONCURRENT MANAGER
-- Tables Used     :
--                   Custom Oracle Tables
--                   ----------------------
-- API Used        :
-- Functions       :
-- Packages Used   : --XXCS_SUPERVALU_WO_NOTES_PKG
-- Assumptions     :
-- Caution         :
-- ******************************************************************************
-- REVISION HISTORY
-- Ver     TRS   Date           Author                 Description
-- 1.0          20-Dec-2012     Srini              Initial Creation
-- ******************************************************************************/

function remove_spl(p_notes in varchar2) return varchar2 is
  v_notes varchar2(2000);
begin
  v_notes := null;
  if p_notes is not null then
  for i in 1.. length( p_notes) loop
    if (ascii(substr(p_notes,i,1)) > 64 and  ascii(substr(p_notes,i,1)) < 91 ) or ( ascii(substr(p_notes,i,1)) > 96 and ascii(substr(p_notes,i,1)) < 123) or (ascii(substr(p_notes,i,1)) = 32 ) then
      v_notes := v_notes || substr(p_notes,i,1);
    end if;
   end loop;
   end if;
  return v_notes;
end;

FUNCTION wo_notes( itaskid in number) return varchar2 is
  v_nt varchar2(4000);
begin
  for n_rec in (select notes from JAM.MSS_WO_ACTION_STATUS_NOTES where work_order_id = itaskid order by wo_action_id desc) loop
    v_nt := v_nt || remove_spl(n_rec.notes) ||',';
  end loop;
  return v_nt;
exception when others then
 return v_nt;
end;

FUNCTION wo_notes_long( itaskid in number) return long is
  v_nt long;
begin
  for n_rec in (select notes from JAM.MSS_WO_ACTION_STATUS_NOTES where work_order_id = itaskid order by wo_action_id desc) loop
    v_nt := v_nt ||remove_spl(n_rec.notes) ||',';
  end loop;
  return v_nt;
end;

Procedure Vms_Data_Extract (P_Errbuf   OUT     Varchar2,
                            p_retcode  OUT     NUMBER
                            ,p_cust_name IN varchar2
                            )

   IS
      l_cur_val      VARCHAR2 (2000);
      l_file         UTL_FILE.file_type;
      l_count        NUMBER;
      l_create_path   varchar2(2000);
      l_move_path     varchar2(2000);
      P_Dir       Varchar2(130) := 'SUPERVALUWO';--'SUPERVALUWO';
    --  P_File      Varchar2(130) := 'SVU_EMERSON_INTERFACE_FILE.dat';
    P_File      Varchar2(130);


      p_invalid_filename exception;

CURSOR c_vms_data
    is
-- Legacy SQL:
--    select
--     --wonum,location,onbehalfof, description,ud_dispatchto,trade,longdescription,ud_ems_alarm_msg,failurecode,assetnum,wopriority
--      WONUM AS "WONUM",
--      LOCATION AS "LOCATION",
--      ONBEHALFOF AS "ONBEHALFOF",
--      DESCRIPTION AS "DESCRIPTION",
--      UD_DISPATCHTO AS "UD_DISPATCHTO",
--      TRADE AS "TRADE",
--      LONGDESCRIPTION AS "LONGDESCRIPTION",
--      UD_EMS_ALARM_MSG AS "UD_EMS_ALARM_MSG",
--      FAILURECODE AS "FAILURECODE",
--      ASSETNUM AS "ASSETNUM",
--      WOPRIORITY AS "WOPRIORITY"
--    from
--    (
--        select
--        mwo.work_order_no wonum,
--        substr(trim(r.ref_name),length(trim(r.ref_name))-4,10) location,
--        decode(mwo.event_type,'Alarm',null,mwo.reported_by_first_name|| '.'||mwo.reported_by_last_name) onbehalfof,
--        mss_vms_data_pkg.remove_spl(mwo.work_order_description) description,
--        cspc.contact_note ud_dispatchto,
--        mpt.problem_type_name trade,
--        wo_notes(mwo.work_order_id)  longdescription,
--        mna.source ud_ems_alarm_msg,
--        mtc.trade_cat_name||'.'||me.equip_name failurecode,
--        me.model assetnum,
--        decode(msl.severity_level_cd,'1', 1,'2',2,'3', 3, '4', 4,4)  wopriority,
--        --row_number() over(partition by wn.source_object_id order by wn.jtf_note_id desc) as num
--        row_number() over(partition by mwo.work_order_no order by mwasn.wo_action_id) as num
--    From Mss_Work_Order Mwo,
--         Cmn_Location_Addr A,
--         Mss_Problem_Type Mpt,
--         Mss_wo_Action_Status_Notes mwasn,
--         Mss_Severity_Level Msl,
--         Mss_Trade_Cat Mtc,
--         Mss_Equip Me,
--         Mss_Norm_Alarm Mna,
--         Cmn_Service_Provider_Contact Cspc,
--         Cmn_Cust Cc,
--         JAM.CMN_SITE s,
--	 jam.cmn_reference r,
--	 JAM.CMN_SITE_CONTACT sc,
--	jam.cmn_location_addr la
--   Where
--     r.ref_id = s.site_id
--    and cc.cust_id = s.cust_id
--    and s.site_id = sc.site_id
--    and LA.LOCATION_ADDR_ID = SC.ADDR_ID
--    and mwo.site_id = s.site_id
--    and Mwo.Alarm_Id=Mna.Alarm_Id(+)
--    And   Mwo.WORK_ORDER_ID=mwasn.WORK_ORDER_ID
--    And   Mwo.Equip_Id = Me.Equip_Id
--    And   Mwo.Trade_Cat_Id =  Mtc.Trade_Cat_Id
--    And   Mwo.PRoblem_Type_Id=Mpt.Problem_Type_Id
--    And   Mwo.Severity_Level_Id=Msl.Severity_Level_Id
--    And   Mwo.Service_Provider_Id =Cspc.Service_Provider_Id
--    And   Cspc.Addr_Id = A.Location_Addr_Id(+)
--    And   Mwo.Cust_Id = Cc.Cust_Id
--    and   cc.cust_name = p_cust_name
--    and   CAST((FROM_TZ(CAST(mwo.MODIFIED_ON AS TIMESTAMP),'+00:00') AT TIME ZONE 'US/Eastern') AS DATE) >= trunc(sysdate-1)
--    and   CAST((FROM_TZ(CAST(mwo.MODIFIED_ON AS TIMESTAMP),'+00:00') AT TIME ZONE 'US/Eastern') AS DATE) < trunc(sysdate)
--    );
--    where num = 1;
-- New SQL for SF table
        SELECT
            SFWO.WORK_ORDER_NO AS "WONUM",
            SUBSTR(TRIM(SFWO.SITE_NAME),LENGTH(TRIM(SFWO.SITE_NAME))-4,10) AS "LOCATION",-- SUBSTR(TRIM(SFS.SF_SITE_NAME),LENGTH(TRIM(SFS.SF_SITE_NAME))-4,10) "LOCATION" --
            SFWO.REPORTED_BY_FIRST_NAME || ' ' || SFWO.REPORTED_BY_LAST_NAME AS "ONBEHALFOF", -- DECODE(MWO.EVENT_TYPE,'ALARM',NULL,MWO.REPORTED_BY_FIRST_NAME|| '.'||MWO.REPORTED_BY_LAST_NAME) "ONBEHALFOF",
            SFWO.DESCRIPTION AS "DESCRIPTION", -- MSS_VMS_DATA_PKG.REMOVE_SPL(MWO.WORK_ORDER_DESCRIPTION) "DESCRIPTION",--REMOVE ALL SPECIAL CHARS(ONLY INCLUDING LETTERS & SPACE).
            NULL AS "UD_DISPATCHTO", -- CSPC.CONTACT_NOTE "UD_DISPATCHTO",
            SFWO.PROBLEM_TYPE AS "TRADE", -- MPT.PROBLEM_TYPE_NAME "TRADE", FROM MSS_PROBLEM_TYPE
            REPLACE(REPLACE(SFWO.WO_DETAILS, CHR(10), ' '), CHR(13), ' ') AS "LONGDESCRIPTION", -- WO_NOTES(MWO.WORK_ORDER_ID)  "LONGDESCRIPTION", JOIN ALL NOTES IN: MSS_WO_ACTION_STATUS_NOTES
            SFNA.SOURCE AS "UD_EMS_ALARM_MSG", -- MNA.SOURCE "UD_EMS_ALARM_MSG",
            REPLACE(SFA.CATEGORY, 'SVU ') || '.' || REPLACE(SFA.EQUIPMENT, 'SVU ') AS "FAILURECODE", -- MTC.TRADE_CAT_NAME||'.'||ME.EQUIP_NAME "FAILURECODE",
            NULL AS "ASSETNUM", -- ME.MODEL "ASSETNUM", FROM EQUIPMENT
            DECODE(SFWO.SEVERITY_LEVEL,'1',1,'2',2,'3',3,'4',4,4) AS "WOPRIORITY" -- DECODE(MSL.SEVERITY_LEVEL_CD,'1', 1,'2',2,'3', 3, '4', 4,4)  "WOPRIORITY"
        FROM SF_WORK_ORDER SFWO,
            SF_NORM_ALARM SFNA,
            SF_SITE SFS,
            SF_CUSTOMER SFC,
            SF_ASSET_TEMP SFA
        WHERE SFWO.SF_ALARM_ID = SFNA.SF_ALARM_ID(+)
        AND SFWO.SF_SITE_ID = SFS.SF_SITE_ID
        AND SFS.SF_CUST_ID = SFC.SF_CUST_ID
        AND SFC.SF_CUST_NAME = p_cust_name
        AND SFWO.SF_ASSET_ID = SFA.SF_ASSET_ID(+)
        AND SFWO.LAST_ACTION ='Confirm'
        AND CAST((FROM_TZ(CAST(SFWO.MODIFIED_ON AS TIMESTAMP),'+00:00') AT TIME ZONE 'US/Eastern') AS DATE) >= TRUNC(SYSDATE - 1)
        AND CAST((FROM_TZ(CAST(SFWO.MODIFIED_ON AS TIMESTAMP),'+00:00') AT TIME ZONE 'US/Eastern') AS DATE) < TRUNC(SYSDATE);

BEGIN

   if p_cust_name is null then
      raise p_invalid_filename;
   else
   	P_File := replace(trim(p_cust_name),' ','')|| '.dat';
   	dbms_output.put_line( ' The filename ' || P_File );
   end if;


    BEGIN
      l_file := UTL_FILE.fopen (p_dir, p_file, 'w',32767);
    EXCEPTION

        WHEN UTL_FILE.INVALID_PATH THEN
        dbms_output.put_line( ' Invalid Path ' || SQLERRM ); --FND_FILE.PUT_LINE(FND_FILE.OUTPUT,
        WHEN UTL_FILE.INVALID_MODE THEN
        dbms_output.put_line( ' Invalid Mode ' || SQLERRM );
        WHEN UTL_FILE.INVALID_OPERATION THEN
        dbms_output.put_line( ' Invalid operation ' || SQLERRM );
        WHEN UTL_FILE.INVALID_MAXLINESIZE THEN
        dbms_output.put_line( ' Invalid Max line size ' || SQLERRM );
        WHEN OTHERS THEN
        dbms_output.put_line( ' Unknown Error while fopen ' || SQLERRM );
    END;


        UTL_FILE.put_line (l_file,'WONUM|LOCATION|ONBEHALFOF|DESCRIPTION|UD_DISPATCHTO|TRADE|LONGDESCRIPTION|UD_EMS_ALARM_MSG|FAILURECODE|ASSETNUM|WOPRIORITY');

        FOR c_vms_rec in  c_vms_data LOOP

            BEGIN
                dbms_output.put_line( 'test===================');
                dbms_output.put_line( 'test==================='||c_vms_rec.WONUM||'|'||c_vms_rec.LOCATION||'|'||c_vms_rec.ONBEHALFOF||'|'||c_vms_rec.DESCRIPTION||'|'||c_vms_rec.UD_DISPATCHTO||'|'||c_vms_rec.TRADE||'|'||c_vms_rec.LONGDESCRIPTION||'|'||c_vms_rec.UD_EMS_ALARM_MSG||'|'||c_vms_rec.FAILURECODE||'|'||c_vms_rec.ASSETNUM||'|'||c_vms_rec.WOPRIORITY);
                UTL_FILE.put_line (l_file, c_vms_rec.WONUM||'|'||c_vms_rec.LOCATION||'|'||c_vms_rec.ONBEHALFOF||'|'||c_vms_rec.DESCRIPTION||'|'||c_vms_rec.UD_DISPATCHTO||'|'||c_vms_rec.TRADE||'|'||c_vms_rec.LONGDESCRIPTION||'|'||c_vms_rec.UD_EMS_ALARM_MSG||'|'||c_vms_rec.FAILURECODE||'|'||c_vms_rec.ASSETNUM||'|'||c_vms_rec.WOPRIORITY);
            EXCEPTION WHEN OTHERS THEN
                dbms_output.put_line( 'Error while writing the data to the file ' || SQLERRM);
            END;

            END LOOP;

        UTL_FILE.fclose (l_file);

    EXCEPTION
       WHEN UTL_FILE.INVALID_PATH THEN
           dbms_output.put_line('Invalid File Path- Error'); --fnd_file.put_line (fnd_file.log,
       WHEN UTL_FILE.INVALID_MODE THEN
        dbms_output.put_line('Invalid file open mode ' || SQLERRM); --dbms_output.put_line(
       WHEN UTL_FILE.INVALID_FILEHANDLE THEN
        UTL_FILE.FCLOSE_ALL;
        dbms_output.put_line('Invalid File handle- Error ');
       WHEN UTL_FILE.WRITE_ERROR THEN
        dbms_output.put_line('UTL file write error- Error  ');
       WHEN UTL_FILE.READ_ERROR THEN
        dbms_output.put_line('UTL file read - Error ');
       WHEN UTL_FILE.INTERNAL_ERROR THEN
        dbms_output.put_line('Internal Error- Error ');
       WHEN p_invalid_filename THEN
        dbms_output.put_line(' Invalid filename entered - Error ');
       WHEN OTHERS THEN
        dbms_output.put_line('Error in procedure vms_data_extract  '|| SQLERRM);

	END vms_data_extract;
End Mss_Vms_Data_Pkg;