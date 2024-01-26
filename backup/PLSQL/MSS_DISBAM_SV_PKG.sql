create or replace PACKAGE BODY     MSS_DISBAM_SV_PKG IS
  /*=========================================================================================
  ||  PROJECT NAME        : Monitoring Service Systems Replacement (MSSR)
  ||  APPLICATION NAME    : Customer Support
  ||  SCRIPT NAME         : MSS_DISBAM_SV_PKG
  ||  CREATION INFORMATION
  ||     12/20/2012       : Srini
  ||  Description      : Program to load the supervalu dispatch, provider data
  =========================================================================================== */
     g_debug_flag       VARCHAR2 (1) := 'N';
     g_request_id       NUMBER (1) := 1;
     g_program_app_id   NUMBER (1) := 1;
     g_version_no       NUMBER (1) := 1;
   --g_active_status    NUMBER (1) := 1;
     g_start_date       DATE;
     g_end_date         DATE;

     g_created_by       VARCHAR2 (15) := 'Administrator';
     g_modified_by      VARCHAR2 (15) := 'Administrator';
     g_created_on       DATE :=SYSDATE;
     g_modified_on      DATE  :=SYSDATE;
     g_addr_type_id_loc NUMBER(5);
     g_addr_type_id_phone NUMBER(5);
     g_addr_type_id_email NUMBER(5);

     g_addr_rol_type_id_loc NUMBER(5);
     g_addr_rol_type_id_phone NUMBER(5);
     g_addr_rol_type_id_email NUMBER(5);
     g_addr_rol_type_id_wo_email  NUMBER(5);
     g_str_mng_rol_type_id   NUMBER (3);
     g_spe_contact_type_id  NUMBER (3);
     g_spn_party_rol_type_id NUMBER(5);
     g_srvce_prov_rol_type_id number;
     v_log_type NUMBER := 3;
     g_debug_enabled VARCHAR2(1) := 'Y';
     g_range_id  NUMBER DEFAULT 6;
     g_all_contact_rol_type_id  NUMBER(3);
     g_scpe_contact_type_id  NUMBER(3);
     g_service_provider_group_id NUMBER(19);

     v_file_name                  UTL_FILE.FILE_TYPE;
     g_directory                  VARCHAR2(130) := 'VMSPROC';
     g_info_msz                   VARCHAR2 (2000);
     V_Return_Status              Varchar2 (1);

     T1 Integer;
     T2 Integer;
     T3 Integer;


   --Gary Sun Added the Procedure LOG_FILE to create a Dynamic file where we can write the log of the Channeling Package

PROCEDURE LOG_FILE (directory_name IN VARCHAR2,log_filename IN VARCHAR2, file_name OUT UTL_FILE.FILE_TYPE, o_return_status OUT VARCHAR2) AS

    log_file UTL_FILE.FILE_TYPE;

    BEGIN
    --log_file := UTL_FILE.FOPEN(directory_name,'MSSR_CHANNELINGPROC_'||to_char(sysdate,'MMDDYYYY_HH24MISS')||'.log','w');
    log_file := UTL_FILE.FOPEN(directory_name,'VMSPROC_' || replace(trim(log_filename),' ','')  ||'.log','a');
    file_name := log_file;

    o_return_status := 'S';

    EXCEPTION
    WHEN UTL_FILE.INVALID_PATH THEN
    RAISE_APPLICATION_ERROR(-20100,'Invalid Path');
    o_return_status := 'E';
    WHEN UTL_FILE.INVALID_MODE THEN
    RAISE_APPLICATION_ERROR(-20101,'Invalid Mode');
    o_return_status := 'E';
    WHEN UTL_FILE.INVALID_OPERATION then
    RAISE_APPLICATION_ERROR(-20102,'Invalid Operation');
    o_return_status := 'E';
    WHEN UTL_FILE.INVALID_FILEHANDLE then
    RAISE_APPLICATION_ERROR(-20103,'Invalid Filehandle');
    o_return_status := 'E';
    WHEN UTL_FILE.WRITE_ERROR then
    RAISE_APPLICATION_ERROR(-20104,'Write Error');
    o_return_status := 'E';
    WHEN UTL_FILE.READ_ERROR then
    RAISE_APPLICATION_ERROR(-20105,'Read Error');
    o_return_status := 'E';
    WHEN UTL_FILE.INTERNAL_ERROR then
    RAISE_APPLICATION_ERROR(-20106,'Internal Error');
    o_return_status := 'E';
    WHEN OTHERS THEN
    UTL_FILE.FCLOSE(log_file);
    o_return_status := 'E';
    END LOG_FILE;

    --Gary Sun Added the Procedure WRITE_LOG_FILE to write the log of the Channeling Package

PROCEDURE WRITE_LOG_FILE (file_name IN UTL_FILE.FILE_TYPE, info IN VARCHAR2,o_return_status OUT VARCHAR2 ) AS
      BEGIN

      UTL_FILE.PUT_LINE(file_name,info);

      o_return_status := 'S';

      EXCEPTION
      WHEN UTL_FILE.INVALID_PATH THEN
      RAISE_APPLICATION_ERROR(-20100,'Invalid Path');
      o_return_status := 'E';
      WHEN UTL_FILE.INVALID_MODE THEN
      RAISE_APPLICATION_ERROR(-20101,'Invalid Mode');
      o_return_status := 'E';
      WHEN UTL_FILE.INVALID_OPERATION then
      RAISE_APPLICATION_ERROR(-20102,'Invalid Operation');
      o_return_status := 'E';
      WHEN UTL_FILE.INVALID_FILEHANDLE then
      RAISE_APPLICATION_ERROR(-20103,'Invalid Filehandle');
      o_return_status := 'E';
      WHEN UTL_FILE.WRITE_ERROR then
      RAISE_APPLICATION_ERROR(-20104,'Write Error');
      o_return_status := 'E';
      WHEN UTL_FILE.READ_ERROR then
      RAISE_APPLICATION_ERROR(-20105,'Read Error');
      o_return_status := 'E';
      WHEN UTL_FILE.INTERNAL_ERROR then
      RAISE_APPLICATION_ERROR(-20106,'Internal Error');
      o_return_status := 'E';
      WHEN OTHERS THEN
      o_return_status := 'E';
      END WRITE_LOG_FILE;

    --Gary Sun Added the Procedure CLOSE_LOG_FILE to write the log of the Channeling Package

PROCEDURE CLOSE_LOG_FILE (file_name IN UTL_FILE.FILE_TYPE) AS
    BEGIN
     IF utl_file.is_open(file_name) THEN
        utl_file.fclose_all;
     END IF;
    END CLOSE_LOG_FILE;

PROCEDURE Debug (i_mesg IN VARCHAR2)
   IS
     BEGIN
        IF g_debug_flag = 'Y'
        THEN
		    g_info_msz := ' '||i_mesg;
            WRITE_LOG_FILE(file_name => v_file_name ,info     => g_info_msz ,o_return_status  => v_return_status);
        END IF;
   END Debug;

PROCEDURE Debug_Mesg(i_file_type IN  NUMBER,
                            i_mesg  IN  VARCHAR2 )
   IS

   BEGIN

    /* IF g_debug_enabled = 'Y' --AND i_file_type = FND_FILE.LOG
     THEN
       --FND_FILE.PUT_LINE(i_file_type, i_mesg);
       DBMS_OUTPUT.PUT_LINE(i_mesg);
     END IF;

     IF g_debug_enabled = 'Y' --AND i_file_type = FND_FILE.OUTPUT
     THEN
       --FND_FILE.PUT_LINE(i_file_type, i_mesg);
              DBMS_OUTPUT.PUT_LINE(i_mesg);
              WRITE_LOG_FILE(file_name => v_file_name ,info     => i_mesg,o_return_status  => v_return_status);

     END IF;*/

     IF g_debug_enabled = 'Y' AND i_file_type = 3 THEN
       --FND_FILE.PUT_LINE(i_file_type, i_mesg);
       DBMS_OUTPUT.PUT_LINE(i_mesg);

     END IF;

   END Debug_Mesg;

PROCEDURE CACHE_VALUES (o_return_status OUT VARCHAR2)
  IS

CURSOR c_addr_type_ref
  IS
    select
        addr_type_id,addr_type_descr
    from
        cmn_addr_type_ref
    where
        addr_type_descr IN ( 'location','phone','email');


CURSOR c_addr_rol_type_ref
 IS
    select
        addr_rol_type_id,addr_rol_type_descr
    from
        cmn_addr_rol_type_ref
    where
        addr_rol_type_descr IN ('Corporate Address','Telephone','Email','WorkOrder_Delivery_Email');

CURSOR C_PARTY_ROL_TYPE_REF
 IS
    SELECT
        party_rol_type_id
    FROM
        cmn_party_rol_type_ref
    WHERE
        party_rol_type_descr ='AllContact';

CURSOR C_CONT_TYPE_REF
 IS
    SELECT
        contact_type_id
    FROM
        cmn_contact_type_ref
    WHERE
        contact_type_descr = 'Site_Contact_Phone_Email';

CURSOR C_SP_PARTY_ROL_TYPE_REF
 IS
   SELECT
        party_rol_type_id
    FROM
        cmn_party_rol_type_ref
    WHERE
        party_rol_type_descr = 'ServiceProvider';

CURSOR cur_service_provider_grp
 IS
  SELECT   SERVICE_PROVIDER_GROUP_ID
    FROM   cmn_service_provider_group
   WHERE   UPPER (SERVICE_PROVIDER_GROUP_NAME) LIKE '%DISPATCH%';

BEGIN

    --g_start_date  := get_timezones(sysdate);
    --g_created_on  := get_timezones(SYSDATE);
    --g_modified_on  := get_timezones(SYSDATE);

    FOR addr_type_ref_rec in c_addr_type_ref LOOP
        IF  addr_type_ref_rec.addr_type_descr = 'location' THEN
            g_addr_type_id_loc := addr_type_ref_rec.addr_type_id;
        ELSIF  addr_type_ref_rec.addr_type_descr = 'phone' THEN
            g_addr_type_id_phone := addr_type_ref_rec.addr_type_id;
        ELSIF  addr_type_ref_rec.addr_type_descr = 'email' THEN
            g_addr_type_id_email := addr_type_ref_rec.addr_type_id;
        END IF;
    END LOOP;

    FOR addr_rol_type_ref_rec in c_addr_rol_type_ref LOOP
        IF  addr_rol_type_ref_rec.addr_rol_type_descr ='Corporate Address' THEN
            g_addr_rol_type_id_loc := addr_rol_type_ref_rec.addr_rol_type_id;
        ELSIF    addr_rol_type_ref_rec.addr_rol_type_descr ='Telephone' THEN
            g_addr_rol_type_id_phone := addr_rol_type_ref_rec.addr_rol_type_id;
        ELSIF  addr_rol_type_ref_rec.addr_rol_type_descr ='Email' THEN
            g_addr_rol_type_id_email := addr_rol_type_ref_rec.addr_rol_type_id;
        ELSIF  addr_rol_type_ref_rec.addr_rol_type_descr ='WorkOrder_Delivery_Email' THEN
            g_addr_rol_type_id_wo_email := addr_rol_type_ref_rec.addr_rol_type_id;
        END IF;
    END LOOP;

    FOR party_rol_type_rec in C_PARTY_ROL_TYPE_REF LOOP
        g_all_contact_rol_type_id:= party_rol_type_rec.party_rol_type_id;
    END LOOP;

    FOR sp_party_rol_type_rec in C_SP_PARTY_ROL_TYPE_REF LOOP
         g_spn_party_rol_type_id:= sp_party_rol_type_rec.party_rol_type_id;
    END LOOP;

    FOR cont_type_ref in C_CONT_TYPE_REF LOOP
        g_scpe_contact_type_id := cont_type_ref.contact_type_id;
    END LOOP;

    FOR rec_service_provider_grp in cur_service_provider_grp LOOP
        g_service_provider_group_id := rec_service_provider_grp.service_provider_group_id;
    END LOOP;

END CACHE_VALUES;

PROCEDURE VALIDATE_PARAM( i_customer IN VARCHAR2
                 ,o_cust_id    OUT VARCHAR2
                 ,o_return_status OUT VARCHAR2
                        )
IS

--CURSOR c_cust IS
--  SELECT
--    cust_id
--  FROM
--    cmn_cust cc
--  WHERE
--    cust_name = i_customer;

BEGIN

    o_return_status := 'S';

--    FOR cust_rec IN c_cust LOOP
--        o_cust_id := cust_rec.cust_id;      -- o_cust_id := cust_rec.cust_id;
--    END LOOP;
    SELECT SF_CUST_ID INTO o_cust_id FROM SF_CUSTOMER WHERE SF_CUST_NAME = i_customer;
    IF o_cust_id IS NULL THEN
        Debug_Mesg( i_file_type => v_log_type,  i_mesg         => ' Customer not found in cmn_cust ' || i_customer );
        o_return_status := 'E';
    ELSE
          Debug_Mesg( i_file_type => v_log_type,  i_mesg         => ' Customer ID found in cmn_cust ' || o_cust_id );
    END IF;

EXCEPTION WHEN OTHERS THEN
        o_return_status := 'E';
        Debug_Mesg( i_file_type => v_log_type,   i_mesg         => 'Unhandled exception in validate parameters proc ' || SQLERRM );
END VALIDATE_PARAM;

/************************************************************************************************************
Name: LOAD_ALARM_EMER_CONTACTS()
Param: I_Load_Alarm_Contacts = 'Y'
Desc:
    Synch SiteContact from MSS_DISBAM_SVU_STAGE(by key NAME_FMT) & MSS_DISBAM_SVU_EMR_PROV_STAGE(by key EMER_CONTACT_1_FMT) into SF_CONTACT_TEMP
        1. Merge SF_CONTACT_TEMP.
        2. Remove SF_CONTACT_TEMP.
        3. Merge SF_CONTACT_ASSIGNEE_TEMP.
        4. Remove SF_CONTACT_ASSIGNEE_TEMP.
************************************************************************************************************/
PROCEDURE LOAD_ALARM_EMER_CONTACTS (
   i_cust_id         IN     VARCHAR2,
   o_return_status      OUT VARCHAR2
)
IS

Begin
 G_Info_Msz := 'LOAD_ALARM_EMER_CONTACTS  ------begin-------------------i_cust_id:'||i_cust_id;
 Write_Log_File(File_Name => V_File_Name ,Info => G_Info_Msz,O_Return_Status => V_Return_Status);

--Merge INTO SF_CONTACT_TEMP
MERGE INTO SF_CONTACT_TEMP SFC
    USING (
        SELECT
            LAST_NAME LAST_NAME,
            PHONE PHONE,
            PRIORITY PRIORITY,
            MIN(SF_SITE_ID) SF_SITE_ID
        FROM(
            SELECT DISTINCT
                DECODE(DISBAM.EMER_CONTACT_1,NULL,NULL,DECODE(DISBAM.EMER_TYPE,'EMR1','EMR 1 ','EMR2','EMR 2 ','DM','DM ') || TRIM(BOTH '"'  FROM TRIM(BOTH CHR(13) FROM TRIM(BOTH CHR(32) FROM TRIM(BOTH CHR(9) FROM TRIM(BOTH '"' FROM DISBAM.EMER_CONTACT_1)))))) LAST_NAME,
                NVL(CASE WHEN INSTR(DISBAM.EMER_PHONE_1,',')>0 AND LENGTH(DISBAM.EMER_PHONE_1)>20 THEN SUBSTR(DISBAM.EMER_PHONE_1,0,(INSTR(DISBAM.EMER_PHONE_1,',')-1)) ELSE TRIM(DISBAM.EMER_PHONE_1) END, '000-000-0000') AS PHONE,
                DECODE (DISBAM.EMER_TYPE, 'EMR1', '200', 'EMR2', '300', 'DM', '400', NULL) PRIORITY,
                SFS.SF_SITE_ID SF_SITE_ID
            FROM
                MSS_DISBAM_SVU_EMR_PROV_STAGE DISBAM,
                SF_SITE SFS
            WHERE
                DISBAM.EMER_CONTACT_1_FMT IS NOT NULL
                AND TRIM(DISBAM.BANNER || ' ' || DISBAM.LOCATION) = TRIM(SFS.SF_SITE_NAME)
                AND SFS.SF_CUST_ID = i_cust_id

            UNION

            SELECT DISTINCT
                'FM ' || TRIM(BOTH '"'  FROM TRIM(BOTH CHR(13) FROM TRIM(BOTH CHR(32) FROM TRIM(BOTH CHR(9) FROM TRIM(BOTH '"' FROM DISBAM.NAME))))) AS LAST_NAME,
                NVL(CASE WHEN INSTR(DISBAM.PRIMARYPHONE,',')>0 AND LENGTH(DISBAM.PRIMARYPHONE)>20 THEN SUBSTR(DISBAM.PRIMARYPHONE,0,(INSTR(DISBAM.PRIMARYPHONE,',')-1)) ELSE TRIM(DISBAM.PRIMARYPHONE) END, '000-000-0000') AS PHONE,
                DISBAM.PRIORITY PRIORITY,
                SFS.SF_SITE_ID SF_SITE_ID
            FROM
                MSS_DISBAM_SVU_STAGE DISBAM,
                SF_SITE SFS
            WHERE
                DISBAM.NAME_FMT IS NOT NULL
                AND DISBAM.TRADE = 'DIVMAINT'
                AND TRIM(DISBAM.BANNER || ' ' || DISBAM.LOCATION) = TRIM(SFS.SF_SITE_NAME)
                AND SFS.SF_CUST_ID = i_cust_id
            ) GROUP BY LAST_NAME, PHONE, PRIORITY
    ) MDSS
    ON (SFC.IS_ACTIVE = 'Y' AND SFC.IS_SERVICE_PROVIDER = 'N' AND SFC.LAST_NAME = MDSS.LAST_NAME AND SFC.PHONE = MDSS.PHONE)
    WHEN MATCHED THEN
        UPDATE SET
            SFC.PRIORITY = MDSS.PRIORITY,
            SFC.SF_SITE_ID = MDSS.SF_SITE_ID,
            SFC.SEND_FLAG = 'YQ',
            SFC.MODIFIED_ON = G_MODIFIED_ON,
            SFC.MODIFIED_BY = G_MODIFIED_BY
        WHERE SFC.SF_CUST_ID = i_cust_id
            AND(SFC.PRIORITY <> MDSS.PRIORITY OR SFC.SF_SITE_ID <> MDSS.SF_SITE_ID)
    WHEN NOT MATCHED THEN
        INSERT(
            FIRST_NAME,
            LAST_NAME,
            PHONE,
            PRIORITY,
            SF_SITE_ID,
            EMAIL,
            IS_SERVICE_PROVIDER,
            ADDRESS,
            METHOD_OF_DELIVERY,
            IS_ACTIVE,
            SEND_FLAG,
            SF_CUST_ID,
            CREATED_ON,
            CREATED_BY,
            MODIFIED_ON,
            MODIFIED_BY
        )
        VALUES(
            NULL,
            MDSS.LAST_NAME,
            MDSS.PHONE,
            MDSS.PRIORITY,
            MDSS.SF_SITE_ID,
            NULL,
            'N',
            NULL,
            'P',
            'Y',
            'YQ',
            i_cust_id,
            G_CREATED_ON,
            G_CREATED_BY,
            G_MODIFIED_ON,
            G_MODIFIED_BY
        );
--Remove
UPDATE SF_CONTACT_TEMP SFC
SET IS_ACTIVE = 'N',
    SEND_FLAG = 'YQ',
    MODIFIED_ON = G_MODIFIED_ON,
    MODIFIED_BY = G_MODIFIED_BY
WHERE SF_CUST_ID = i_cust_id
    AND IS_SERVICE_PROVIDER = 'N'
    AND IS_ACTIVE = 'Y'
    AND NOT EXISTS(
        SELECT 1 FROM (
            SELECT DISTINCT
                DECODE(DISBAM.EMER_CONTACT_1,NULL,NULL,DECODE(DISBAM.EMER_TYPE,'EMR1','EMR 1 ','EMR2','EMR 2 ','DM','DM ') || TRIM(BOTH '"'  FROM TRIM(BOTH CHR(13) FROM TRIM(BOTH CHR(32) FROM TRIM(BOTH CHR(9) FROM TRIM(BOTH '"' FROM DISBAM.EMER_CONTACT_1)))))) LAST_NAME,
                NVL(CASE WHEN INSTR(DISBAM.EMER_PHONE_1,',')>0 AND LENGTH(DISBAM.EMER_PHONE_1)>20 THEN SUBSTR(DISBAM.EMER_PHONE_1,0,(INSTR(DISBAM.EMER_PHONE_1,',')-1)) ELSE TRIM(DISBAM.EMER_PHONE_1) END, '000-000-0000') AS PHONE
            FROM
                MSS_DISBAM_SVU_EMR_PROV_STAGE DISBAM,
                SF_SITE SFS
            WHERE
                DISBAM.EMER_CONTACT_1_FMT IS NOT NULL
                AND TRIM(DISBAM.BANNER || ' ' || DISBAM.LOCATION) = TRIM(SFS.SF_SITE_NAME)
                AND SFS.SF_CUST_ID = i_cust_id

            UNION

            SELECT DISTINCT
                'FM ' || TRIM(BOTH '"'  FROM TRIM(BOTH CHR(13) FROM TRIM(BOTH CHR(32) FROM TRIM(BOTH CHR(9) FROM TRIM(BOTH '"' FROM DISBAM.NAME))))) AS LAST_NAME,
                NVL(CASE WHEN INSTR(DISBAM.PRIMARYPHONE,',')>0 AND LENGTH(DISBAM.PRIMARYPHONE)>20 THEN SUBSTR(DISBAM.PRIMARYPHONE,0,(INSTR(DISBAM.PRIMARYPHONE,',')-1)) ELSE TRIM(DISBAM.PRIMARYPHONE) END, '000-000-0000') AS PHONE
            FROM
                MSS_DISBAM_SVU_STAGE DISBAM,
                SF_SITE SFS
            WHERE
                DISBAM.NAME_FMT IS NOT NULL
                AND DISBAM.TRADE = 'DIVMAINT'
                AND TRIM(DISBAM.BANNER || ' ' || DISBAM.LOCATION) = TRIM(SFS.SF_SITE_NAME)
                AND SFS.SF_CUST_ID = i_cust_id
        ) WHERE PHONE = SFC.PHONE
            AND LAST_NAME = SFC.LAST_NAME
    );


--Merge into SF_CONTACT_ASSIGNEE_TEMP
MERGE INTO SF_CONTACT_ASSIGNEE_TEMP SFCA
    USING (
        SELECT DISTINCT
            SFC.ID SF_NATIVE_CONTACT_ID,
            -- DISBAM.PRIORITY AS PRIORITY,
            SFS.SF_SITE_ID SF_SITE_ID
        FROM
            SF_CONTACT_TEMP SFC,
            SF_SITE SFS,
            MSS_DISBAM_SVU_STAGE DISBAM
        WHERE
            SFC.IS_SERVICE_PROVIDER = 'N'
            AND SFC.IS_ACTIVE = 'Y'
            AND SFC.LAST_NAME = 'FM ' || TRIM(BOTH '"'  FROM TRIM(BOTH CHR(13) FROM TRIM(BOTH CHR(32) FROM TRIM(BOTH CHR(9) FROM TRIM(BOTH '"' FROM DISBAM.NAME)))))
            AND SFC.PHONE = NVL(CASE WHEN INSTR(DISBAM.PRIMARYPHONE,',')>0 AND LENGTH(DISBAM.PRIMARYPHONE)>20 THEN SUBSTR(DISBAM.PRIMARYPHONE,0,(INSTR(DISBAM.PRIMARYPHONE,',')-1)) ELSE TRIM(DISBAM.PRIMARYPHONE) END, '000-000-0000')
            AND DISBAM.NAME_FMT IS NOT NULL
            AND TRIM(DISBAM.BANNER || ' ' || DISBAM.LOCATION) = TRIM(SFS.SF_SITE_NAME)
            AND SFS.SF_CUST_ID = i_cust_id

        UNION

        SELECT DISTINCT
            SFC.ID SF_NATIVE_CONTACT_ID,
            -- DECODE (DISBAM.EMER_TYPE, 'EMR1', '200', 'EMR2', '300', 'DM', '400', NULL) PRIORITY,
            SFS.SF_SITE_ID SF_SITE_ID
        FROM
            SF_CONTACT_TEMP SFC,
            SF_SITE SFS,
            MSS_DISBAM_SVU_EMR_PROV_STAGE DISBAM
        WHERE
            SFC.IS_SERVICE_PROVIDER = 'N'
            AND SFC.IS_ACTIVE = 'Y'
            AND SFC.LAST_NAME = DECODE(DISBAM.EMER_CONTACT_1,NULL,NULL,DECODE(DISBAM.EMER_TYPE,'EMR1','EMR 1 ','EMR2','EMR 2 ','DM','DM ') || TRIM(BOTH '"'  FROM TRIM(BOTH CHR(13) FROM TRIM(BOTH CHR(32) FROM TRIM(BOTH CHR(9) FROM TRIM(BOTH '"' FROM DISBAM.EMER_CONTACT_1))))))
            AND SFC.PHONE = NVL(CASE WHEN INSTR(DISBAM.EMER_PHONE_1,',')>0 AND LENGTH(DISBAM.EMER_PHONE_1)>20 THEN SUBSTR(DISBAM.EMER_PHONE_1,0,(INSTR(DISBAM.EMER_PHONE_1,',')-1)) ELSE TRIM(DISBAM.EMER_PHONE_1) END, '000-000-0000')
            AND DISBAM.EMER_CONTACT_1_FMT IS NOT NULL
            AND TRIM(DISBAM.BANNER || ' ' || DISBAM.LOCATION) = TRIM(SFS.SF_SITE_NAME)
            AND SFS.SF_CUST_ID = i_cust_id
    ) MDSS
    ON (
        SFCA.SF_NATIVE_CONTACT_ID = MDSS.SF_NATIVE_CONTACT_ID
        AND SFCA.SF_SITE_ID = MDSS.SF_SITE_ID
        AND SFCA.IS_SERVICE_PROVIDER = 'N'
        AND SFCA.IS_ACTIVE = 'Y'
    )
    -- WHEN MATCHED THEN
    --     UPDATE SET
    --         SFCA.PRIORITY = MDSS.PRIORITY,
    --         SFCA.SEND_FLAG = 'YQ',
    --         SFCA.MODIFIED_ON = G_MODIFIED_ON,
    --         SFCA.MODIFIED_BY = G_MODIFIED_BY
    --     WHERE
    --         SFCA.PRIORITY <> MDSS.PRIORITY
    WHEN NOT MATCHED THEN
        INSERT(
            SF_NATIVE_CONTACT_ID,
            SF_SITE_ID,
            SF_NATIVE_ASSET_ID,
            IS_SERVICE_PROVIDER,
            IS_ACTIVE,
            SEND_FLAG,
            CREATED_ON,
            CREATED_BY,
            MODIFIED_ON,
            MODIFIED_BY
        )
        VALUES(
            MDSS.SF_NATIVE_CONTACT_ID,
            MDSS.SF_SITE_ID,
            NULL,
            'N',
            'Y',
            'YQ',
            G_CREATED_ON,
            G_CREATED_BY,
            G_MODIFIED_ON,
            G_MODIFIED_BY
        );


--Remove not exist from SF_CONTACT_ASSIGNEE_TEMP
UPDATE SF_CONTACT_ASSIGNEE_TEMP SFCA
SET IS_ACTIVE = 'N',
    SEND_FLAG = 'YQ',
    MODIFIED_ON = G_MODIFIED_ON,
    MODIFIED_BY = G_MODIFIED_BY
WHERE IS_SERVICE_PROVIDER = 'N'
    AND IS_ACTIVE = 'Y'
    AND SF_SITE_ID IN (
        SELECT SF_SITE_ID FROM SF_SITE WHERE SF_CUST_ID = i_cust_id
    )
    AND NOT EXISTS(
        SELECT 1 FROM (
            SELECT DISTINCT
                SFC.ID SF_NATIVE_CONTACT_ID,
                SFS.SF_SITE_ID SF_SITE_ID
            FROM
                SF_CONTACT_TEMP SFC,
                SF_SITE SFS,
                MSS_DISBAM_SVU_STAGE DISBAM
            WHERE
                SFC.IS_SERVICE_PROVIDER = 'N'
                AND SFC.IS_ACTIVE = 'Y'
                AND SFC.LAST_NAME = 'FM ' || TRIM(BOTH '"'  FROM TRIM(BOTH CHR(13) FROM TRIM(BOTH CHR(32) FROM TRIM(BOTH CHR(9) FROM TRIM(BOTH '"' FROM DISBAM.NAME)))))
                AND SFC.PHONE = NVL(CASE WHEN INSTR(DISBAM.PRIMARYPHONE,',')>0 AND LENGTH(DISBAM.PRIMARYPHONE)>20 THEN SUBSTR(DISBAM.PRIMARYPHONE,0,(INSTR(DISBAM.PRIMARYPHONE,',')-1)) ELSE TRIM(DISBAM.PRIMARYPHONE) END, '000-000-0000')
                AND DISBAM.NAME_FMT IS NOT NULL
                AND TRIM(DISBAM.BANNER || ' ' || DISBAM.LOCATION) = TRIM(SFS.SF_SITE_NAME)
                AND SFS.SF_CUST_ID = i_cust_id

            UNION

            SELECT DISTINCT
                SFC.ID SF_NATIVE_CONTACT_ID,
                SFS.SF_SITE_ID SF_SITE_ID
            FROM
                SF_CONTACT_TEMP SFC,
                SF_SITE SFS,
                MSS_DISBAM_SVU_EMR_PROV_STAGE DISBAM
            WHERE
                SFC.IS_SERVICE_PROVIDER = 'N'
                AND SFC.IS_ACTIVE = 'Y'
                AND SFC.LAST_NAME = DECODE(DISBAM.EMER_CONTACT_1,NULL,NULL,DECODE(DISBAM.EMER_TYPE,'EMR1','EMR 1 ','EMR2','EMR 2 ','DM','DM ') || TRIM(BOTH '"'  FROM TRIM(BOTH CHR(13) FROM TRIM(BOTH CHR(32) FROM TRIM(BOTH CHR(9) FROM TRIM(BOTH '"' FROM DISBAM.EMER_CONTACT_1))))))
                AND SFC.PHONE = NVL(CASE WHEN INSTR(DISBAM.EMER_PHONE_1,',')>0 AND LENGTH(DISBAM.EMER_PHONE_1)>20 THEN SUBSTR(DISBAM.EMER_PHONE_1,0,(INSTR(DISBAM.EMER_PHONE_1,',')-1)) ELSE TRIM(DISBAM.EMER_PHONE_1) END, '000-000-0000')
                AND DISBAM.EMER_CONTACT_1_FMT IS NOT NULL
                AND TRIM(DISBAM.BANNER || ' ' || DISBAM.LOCATION) = TRIM(SFS.SF_SITE_NAME)
                AND SFS.SF_CUST_ID = i_cust_id
        )
        WHERE SF_NATIVE_CONTACT_ID = SFCA.SF_NATIVE_CONTACT_ID
        AND SF_SITE_ID = SFCA.SF_SITE_ID
    );

      g_info_msz := '   LOAD_ALARM_EMER_CONTACTS Insert SF_CONTACT_ASSIGNEE_TEMP:' || sql%rowcount;
      Write_Log_File(File_Name => V_File_Name ,Info => G_Info_Msz,O_Return_Status => V_Return_Status);
      Debug_Mesg( I_File_Type => V_Log_Type,  I_Mesg  => G_Info_Msz );

  O_Return_Status := 'S';
 EXCEPTION
   WHEN OTHERS
   THEN
      o_return_status := 'E';
      Dbms_Output.Put_Line(' i_file_type   =>'
                           || v_log_type
                           || 'i_mesg        => Unhandled exception in LOAD_ALARM_EMER_CONTACTS proc '
                           || SQLERRM);
            g_info_msz := ' i_file_type   =>'
                           || v_log_type
                           || 'i_mesg        => Unhandled exception in LOAD_ALARM_EMER_CONTACTS proc '
                           || SQLERRM ;

            WRITE_LOG_FILE(file_name => v_file_name
                  ,info     => g_info_msz
                  ,o_return_status  => v_return_status);
      ROLLBACK;
End Load_Alarm_Emer_Contacts;

/************************************************************************************************************
Name: LOAD_EMERGENCY_PROV()
Param: I_Emr_Prov_Upt = 'Y'
Desc:
    This is removed and merged into LOAD_PROV_DATA()
************************************************************************************************************/
PROCEDURE LOAD_EMERGENCY_PROV( i_cust_id IN VARCHAR2, o_return_status OUT VARCHAR2 ) IS
Begin
       -- Debug_Mesg( i_file_type => v_log_type, i_mesg         => ' Started Loading Emergency Providers ' );
        DEBUG_MESG( i_file_type => v_log_type,  i_mesg         => ' LOAD_EMERGENCY_PROV, Merge providers:  ' || sql%rowcount );
   o_return_status := 'S';

EXCEPTION WHEN OTHERS THEN
   o_return_status := 'E';
     --FND_FILE.PUT_LINE(FND_FILE.LOG, ' Unhandled exception in LOAD_EMERGENCY_PROV proc ' || SQLERRM );
     DBMS_OUTPUT.PUT_LINE( ' Unhandled exception in LOAD_EMERGENCY_PROV proc ' || SQLERRM );   -- ms
     g_info_msz := ' Unhandled exception in LOAD_EMERGENCY_PROV proc ' || SQLERRM ;

            WRITE_LOG_FILE(file_name => v_file_name
                  ,info     => g_info_msz
                  ,o_return_status  => v_return_status);
END LOAD_EMERGENCY_PROV;

/************************************************************************************************************
Name: Load_Dispatch_Data()
Param: I_Disbam_Upt = 'Y'
Desc:
Insert from MSS_DISBAM_SVU_STAGE into SF_ASSET_TEMP
    1. Level 1, Trade Category(Root).
        Only insert new ones if category not exist in SF_ASSET_TEMP.
    2. Level 2, Equipment, Location, ProblemType(Grouped list splited by ';')
        Only insert new ones if SF_ASSET_NAME(Cat+Equip+'N/A') not exist in SF_ASSET_TEMP.
************************************************************************************************************/
PROCEDURE LOAD_DISPATCH_DATA( i_cust_id IN VARCHAR2, o_return_status  OUT VARCHAR2 ) IS
BEGIN
    o_return_status :='S';
    -- loading the disbam category
    --Debug_Mesg( I_File_Type => V_Log_Type, I_Mesg => ' Started loading disbam categories ' );
    T1 := Dbms_Utility.Get_Time;

-- Level 1 Insert
    INSERT INTO SF_ASSET_TEMP(
        SF_SITE_ID,
        SF_ASSET_NAME,
        CATEGORY,
        SEND_FLAG,
        START_DATE,
        CREATED_ON,
        CREATED_BY,
        MODIFIED_ON,
        MODIFIED_BY)
    SELECT DISTINCT
        SFS.SF_SITE_ID SF_SITE_ID,
        'SVU ' || SUBSTR(MDSS.FAILURE_CODE, 1, 3) SF_ASSET_NAME,
        'SVU ' || SUBSTR(MDSS.FAILURE_CODE, 1, 3) CATEGORY,
       	'YQ' SEND_FLAG,
       	SYSDATE START_DATE,
       	G_CREATED_ON CREATED_ON,
       	G_CREATED_BY CREATED_BY,
       	G_MODIFIED_ON MODIFIED_ON,
       	G_MODIFIED_BY MODIFIED_BY
    FROM MSS_DISBAM_SVU_STAGE MDSS,
    	SF_SITE SFS
    WHERE MDSS.TRADE <> 'DIVMAINT'
    AND MDSS.FAILURE_CODE IS NOT NULL
    AND TRIM(MDSS.BANNER||' '|| MDSS.LOCATION) = TRIM(SFS.SF_SITE_NAME)
    AND SFS.SF_CUST_ID = I_CUST_ID
    AND NOT EXISTS (
    	SELECT 1 FROM SF_ASSET_TEMP SFA_L1
    	WHERE SFA_L1.SF_SITE_ID = SFS.SF_SITE_ID
    	AND SFA_L1.CATEGORY = 'SVU ' || SUBSTR(MDSS.FAILURE_CODE, 1, 3)
        AND SFA_L1.PID IS NULL
    );

    T2 := Dbms_Utility.Get_Time;
    G_Info_Msz :='  LOAD_DISPATCH_DATA Asset_L1 row:'||SQL%ROWCOUNT||',  Execution time:'||To_Char((T2-T1)/100,'999.999');
      WRITE_LOG_FILE(file_name => v_file_name ,info     => g_info_msz,o_return_status  => v_return_status);

    Debug_Mesg( i_file_type => v_log_type, i_mesg => G_Info_Msz );

    -- loading the disbam equipment
    --Debug_Mesg( i_file_type => v_log_type,i_mesg => ' Started loading disbam equipments ' );
    T1 := Dbms_Utility.Get_Time;
-- Level 2 Insert
    INSERT INTO SF_ASSET_TEMP(
    	PID,
    	SF_SITE_ID,
        SF_ASSET_NAME,
        CATEGORY,
        EQUIPMENT,
        LOCATION,
        PROBLEM_TYPE,
        SEND_FLAG,
        START_DATE,
        CREATED_ON,
        CREATED_BY,
        MODIFIED_ON,
        MODIFIED_BY)
    SELECT PID, SF_SITE_ID, SF_ASSET_NAME, CATEGORY, EQUIPMENT, LOCATION,
    	LISTAGG(PROBLEM_TYPE, ';') WITHIN GROUP (ORDER BY NULL) PROBLEM_TYPE,
    	SEND_FLAG, SYSDATE START_DATE, G_CREATED_ON CREATED_ON, G_CREATED_BY CREATED_BY, G_MODIFIED_ON MODIFIED_ON, G_MODIFIED_BY MODIFIED_BY
    FROM(
        SELECT DISTINCT
            SFA_L1.ID PID,
            SFS.SF_SITE_ID SF_SITE_ID,
            'SVU ' || SUBSTR(MDSS.FAILURE_CODE, 1, 3) || ' + SVU ' || SUBSTR(MDSS.FAILURE_CODE, 5 ,8) || ' + N/A' SF_ASSET_NAME,
            'SVU ' || SUBSTR(MDSS.FAILURE_CODE, 1, 3) CATEGORY,
            'SVU ' || SUBSTR(MDSS.FAILURE_CODE, 5 ,8) EQUIPMENT,
            NULL LOCATION,
            MDSS.TRADE PROBLEM_TYPE,
            'YQ' SEND_FLAG
        FROM MSS_DISBAM_SVU_STAGE MDSS,
            SF_SITE SFS,
            SF_ASSET_TEMP SFA_L1
        WHERE MDSS.TRADE <> 'DIVMAINT'
            AND MDSS.FAILURE_CODE IS NOT NULL
            AND TRIM(MDSS.BANNER || ' ' || MDSS.LOCATION) = TRIM(SFS.SF_SITE_NAME)
            AND SFS.SF_CUST_ID = I_CUST_ID
            AND SFA_L1.PID IS NULL
            AND SFA_L1.SF_SITE_ID = SFS.SF_SITE_ID
            AND SFA_L1.CATEGORY = 'SVU ' || SUBSTR(MDSS.FAILURE_CODE, 1, 3)
            AND NOT EXISTS(
                SELECT 1 FROM SF_ASSET_TEMP SFA_L2
                WHERE SFA_L2.PID = SFA_L1.ID
                AND SFA_L2.SF_ASSET_NAME = 'SVU ' || SUBSTR(MDSS.FAILURE_CODE, 1, 3) || ' + SVU ' || SUBSTR(MDSS.FAILURE_CODE, 5 ,8) || ' + N/A'
            )
    )  GROUP BY PID, SF_SITE_ID, SF_ASSET_NAME, CATEGORY, EQUIPMENT, LOCATION, SEND_FLAG;

    T2 := Dbms_Utility.Get_Time;
    G_Info_Msz :='  LOAD_DISPATCH_DATA Asset_L2 row:'||SQL%ROWCOUNT||',  Execution time:'||To_Char((T2-T1)/100,'999.999');
      Write_Log_File(File_Name => V_File_Name ,Info     => G_Info_Msz,O_Return_Status  => V_Return_Status);
    Debug_Mesg( I_File_Type => V_Log_Type,I_Mesg => G_Info_Msz );

EXCEPTION WHEN OTHERS THEN
    --FND_FILE.PUT_LINE(FND_FILE.LOG, ' Unhandled exception in load_dispatch_data ' || SQLERRM );
    dbms_output.put_line(' Unhandled exception in load_dispatch_data ' || SQLERRM );
     g_info_msz := ' Unhandled exception in load_dispatch_data ' || SQLERRM ;

            WRITE_LOG_FILE(file_name => v_file_name
                  ,info     => g_info_msz
                  ,o_return_status  => v_return_status);
END LOAD_DISPATCH_DATA;

/************************************************************************************************************
Name: LOAD_PROV_DATA()
Param: I_Provider_Upt = 'Y'
Desc:
    Merge ServiceProvider from MSS_DISBAM_SVU_STAGE(by key NAME_FMT) & MSS_DISBAM_SVU_EMR_PROV_STAGE(by key EMER_CONTACT_1_FMT) into SF_CONTACT_TEMP
        1. Remove SF_CONTACT_TEMP
        2. Merge SF_CONTACT_TEMP
************************************************************************************************************/
PROCEDURE LOAD_PROV_DATA( i_cust_id IN VARCHAR2, o_return_status OUT VARCHAR2 )
IS
BEGIN
--Remove
UPDATE SF_CONTACT_TEMP SFC
SET IS_ACTIVE = 'N',
    SEND_FLAG = 'YQ',
    MODIFIED_ON = G_MODIFIED_ON,
    MODIFIED_BY = G_MODIFIED_BY
WHERE IS_SERVICE_PROVIDER = 'Y'
    AND IS_ACTIVE = 'Y'
    AND SF_CUST_ID = i_cust_id
    AND NOT EXISTS(
        SELECT 1 FROM(
            SELECT DISTINCT
                DISBAM.NAME_FMT LAST_NAME,
                DISBAM.ABSERVICEPROVID ADDRESS
            FROM
                MSS_DISBAM_SVU_STAGE DISBAM,
                SF_SITE SFS
            WHERE
                DISBAM.NAME_FMT IS NOT NULL
                AND TRIM(DISBAM.BANNER || ' ' || DISBAM.LOCATION) = TRIM(SFS.SF_SITE_NAME)
                AND SFS.SF_CUST_ID = i_cust_id

            UNION

            SELECT DISTINCT
                DISBAM.EMER_CONTACT_1_FMT LAST_NAME,
                DISBAM.EMER_CONTACT_ID ADDRESS
            FROM
                MSS_DISBAM_SVU_EMR_PROV_STAGE DISBAM,
                SF_SITE SFS
            WHERE
                EMER_CONTACT_1_FMT IS NOT NULL
                AND TRIM(DISBAM.BANNER || ' ' || DISBAM.LOCATION) = TRIM(SFS.SF_SITE_NAME)
                AND SFS.SF_CUST_ID = i_cust_id
        ) WHERE LAST_NAME = SFC.LAST_NAME
            AND ADDRESS = SFC.ADDRESS
    );
-- Merge into SF_CONTACT_TEMP
MERGE INTO SF_CONTACT_TEMP SFC
    USING (
        SELECT DISTINCT
            NULL FIRST_NAME,
            DISBAM.NAME_FMT LAST_NAME,--'N/A' in legacy PL/SQL
            NVL(CASE WHEN INSTR(DISBAM.PRIMARYPHONE,',')>0 AND LENGTH(DISBAM.PRIMARYPHONE)>20 THEN SUBSTR(DISBAM.PRIMARYPHONE,0,(INSTR(DISBAM.PRIMARYPHONE,',')-1)) ELSE TRIM(DISBAM.PRIMARYPHONE) END, '000-000-0000') AS PHONE,
            DISBAM.EMAIL EMAIL,
            'Y' IS_SERVICE_PROVIDER,
            DISBAM.ABSERVICEPROVID ADDRESS,
            DECODE(DISBAM.EMAIL, NULL, 'P', 'E') METHOD_OF_DELIVERY,
            'Y' IS_ACTIVE,
            'YQ' SEND_FLAG,
            G_CREATED_ON CREATED_ON,
            G_CREATED_BY CREATED_BY,
            G_MODIFIED_ON MODIFIED_ON,
            G_MODIFIED_BY MODIFIED_BY
        FROM
            MSS_DISBAM_SVU_STAGE DISBAM,
            SF_SITE SFS
        WHERE
            DISBAM.NAME_FMT IS NOT NULL
            AND TRIM(DISBAM.BANNER || ' ' || DISBAM.LOCATION) = TRIM(SFS.SF_SITE_NAME)
            AND SFS.SF_CUST_ID = i_cust_id

        UNION

        SELECT DISTINCT
            NULL FIRST_NAME,
            DISBAM.EMER_CONTACT_1_FMT LAST_NAME,
            NVL(CASE WHEN INSTR(DISBAM.EMER_PHONE_1,',')>0 AND LENGTH(DISBAM.EMER_PHONE_1)>20 THEN SUBSTR(DISBAM.EMER_PHONE_1,0,(INSTR(DISBAM.EMER_PHONE_1,',')-1)) ELSE TRIM(DISBAM.EMER_PHONE_1) END, '000-000-0000') AS PHONE,
            NULL EMAIL,
            'Y' IS_SERVICE_PROVIDER,
            DISBAM.EMER_CONTACT_ID ADDRESS,
            'P' METHOD_OF_DELIVERY,
            'Y' IS_ACTIVE,
            'YQ' SEND_FLAG,
            G_CREATED_ON CREATED_ON,
            G_CREATED_BY CREATED_BY,
            G_MODIFIED_ON MODIFIED_ON,
            G_MODIFIED_BY MODIFIED_BY
        FROM
            MSS_DISBAM_SVU_EMR_PROV_STAGE DISBAM,
            SF_SITE SFS
        WHERE
            EMER_CONTACT_1_FMT IS NOT NULL
            AND TRIM(DISBAM.BANNER || ' ' || DISBAM.LOCATION) = TRIM(SFS.SF_SITE_NAME)
            AND SFS.SF_CUST_ID = i_cust_id
    ) MDSS
    ON (SFC.LAST_NAME = MDSS.LAST_NAME AND SFC.ADDRESS = MDSS.ADDRESS AND SFC.IS_SERVICE_PROVIDER = 'Y' AND SFC.IS_ACTIVE = 'Y')
    WHEN MATCHED THEN
        UPDATE SET
            SFC.EMAIL = MDSS.EMAIL,
            SFC.PHONE = MDSS.PHONE,
            SFC.METHOD_OF_DELIVERY = MDSS.METHOD_OF_DELIVERY,
            SFC.SEND_FLAG = MDSS.SEND_FLAG,
            SFC.MODIFIED_ON = G_MODIFIED_ON,
            SFC.MODIFIED_BY = G_MODIFIED_BY
        WHERE
            SFC.SF_CUST_ID = i_cust_id
            AND (
                SFC.EMAIL <> MDSS.EMAIL
                OR SFC.PHONE <> MDSS.PHONE
                OR SFC.METHOD_OF_DELIVERY <> MDSS.METHOD_OF_DELIVERY
            )
    WHEN NOT MATCHED THEN
        INSERT(
            FIRST_NAME,
            LAST_NAME,
            PHONE,
            EMAIL,
            IS_SERVICE_PROVIDER,
            ADDRESS,
            METHOD_OF_DELIVERY,
            IS_ACTIVE,
            SEND_FLAG,
            SF_CUST_ID,
            CREATED_ON,
            CREATED_BY,
            MODIFIED_ON,
            MODIFIED_BY
        )
        VALUES(
            MDSS.FIRST_NAME,
            MDSS.LAST_NAME,
            MDSS.PHONE,
            MDSS.EMAIL,
            MDSS.IS_SERVICE_PROVIDER,
            MDSS.ADDRESS,
            MDSS.METHOD_OF_DELIVERY,
            MDSS.IS_ACTIVE,
            MDSS.SEND_FLAG,
            i_cust_id,
            MDSS.CREATED_ON,
            MDSS.CREATED_BY,
            MDSS.MODIFIED_ON,
            MDSS.MODIFIED_BY
        );

        DEBUG_MESG( i_file_type => v_log_type,  i_mesg         => ' LOAD_PROV_DATA, Merge providers:  ' || sql%rowcount );

     o_return_status := 'S';

EXCEPTION WHEN OTHERS THEN
     o_return_status := 'E';
     --FND_FILE.PUT_LINE(FND_FILE.LOG, ' Unhandled exception in LOAD_PROV_DATA proc ' || SQLERRM );
     DBMS_OUTPUT.PUT_LINE( ' Unhandled exception in LOAD_PROV_DATA proc ' || SQLERRM );
     g_info_msz := ' Unhandled exception in LOAD_PROV_DATA proc ' || SQLERRM ;

            WRITE_LOG_FILE(file_name => v_file_name
                  ,info     => g_info_msz
                  ,o_return_status  => v_return_status);
END LOAD_PROV_DATA;

/************************************************************************************************************
Name: ASSIGN_PROVIDERS()
Param: I_Assign_Providers = 'Y'
Desc: Merge into SF_CONTACT_ASSIGNEE_TEMP from MSS_DISBAM_SVU_STAGE & MSS_DISBAM_SVU_EMR_PROV_STAGE
    1. Remove SF_CONTACT_ASSIGNEE_TEMP
    2. Merge SF_CONTACT_ASSIGNEE_TEMP
************************************************************************************************************/
PROCEDURE ASSIGN_PROVIDERS( i_cust_id IN VARCHAR2,
                            o_return_status OUT VARCHAR2 ) IS
BEGIN

--Remove
UPDATE SF_CONTACT_ASSIGNEE_TEMP SFCA
SET IS_ACTIVE = 'N',
    SEND_FLAG = 'YQ',
    MODIFIED_ON = G_MODIFIED_ON,
    MODIFIED_BY = G_MODIFIED_BY
WHERE IS_SERVICE_PROVIDER = 'Y'
    AND IS_ACTIVE = 'Y'
    AND SF_SITE_ID IN (
        SELECT SF_SITE_ID FROM SF_SITE WHERE SF_CUST_ID = i_cust_id
    )
    AND NOT EXISTS(
        SELECT 1 FROM (
            SELECT DISTINCT
                SFC.ID SF_NATIVE_CONTACT_ID,
                SFA.ID SF_NATIVE_ASSET_ID,
                SFS.SF_SITE_ID SF_SITE_ID
            FROM
                SF_CONTACT_TEMP SFC,
                SF_SITE SFS,
                MSS_DISBAM_SVU_STAGE DISBAM,
                SF_ASSET_TEMP SFA
            WHERE
                SFC.IS_SERVICE_PROVIDER = 'Y'
                AND SFC.IS_ACTIVE = 'Y'
                AND SFC.LAST_NAME = DISBAM.NAME_FMT
                AND SFC.ADDRESS = DISBAM.ABSERVICEPROVID
                AND SFC.PHONE = NVL(CASE WHEN INSTR(DISBAM.PRIMARYPHONE,',')>0 AND LENGTH(DISBAM.PRIMARYPHONE)>20 THEN SUBSTR(DISBAM.PRIMARYPHONE,0,(INSTR(DISBAM.PRIMARYPHONE,',')-1)) ELSE TRIM(DISBAM.PRIMARYPHONE) END, '000-000-0000')
                AND SFA.EQUIPMENT = 'SVU ' || SUBSTR(DISBAM.FAILURE_CODE, 5 ,8)
                AND SFA.SF_SITE_ID = SFS.SF_SITE_ID
                AND TRIM(DISBAM.BANNER || ' ' || DISBAM.LOCATION) = TRIM(SFS.SF_SITE_NAME)
                AND SFS.SF_CUST_ID = i_cust_id

            UNION

            SELECT DISTINCT
                SFC.ID SF_NATIVE_CONTACT_ID,
                SFA.ID SF_NATIVE_ASSET_ID,
                SFS.SF_SITE_ID SF_SITE_ID
            FROM
                SF_CONTACT_TEMP SFC,
                SF_SITE SFS,
                MSS_DISBAM_SVU_EMR_PROV_STAGE DISBAM,
                SF_ASSET_TEMP SFA
            WHERE
                SFC.IS_SERVICE_PROVIDER = 'Y'
                AND SFC.IS_ACTIVE = 'Y'
                AND SFC.LAST_NAME = DISBAM.EMER_CONTACT_1_FMT
                AND SFC.ADDRESS = DISBAM.EMER_CONTACT_ID
                AND SFC.PHONE = NVL(CASE WHEN INSTR(DISBAM.EMER_PHONE_1,',')>0 AND LENGTH(DISBAM.EMER_PHONE_1)>20 THEN SUBSTR(DISBAM.EMER_PHONE_1,0,(INSTR(DISBAM.EMER_PHONE_1,',')-1)) ELSE TRIM(DISBAM.EMER_PHONE_1) END, '000-000-0000')
                AND SFA.EQUIPMENT IS NOT NULL
                AND SFA.SF_SITE_ID = SFS.SF_SITE_ID
                AND TRIM(DISBAM.BANNER || ' ' || DISBAM.LOCATION) = TRIM(SFS.SF_SITE_NAME)
                AND SFS.SF_CUST_ID = i_cust_id
        )
        WHERE SF_NATIVE_CONTACT_ID = SFCA.SF_NATIVE_CONTACT_ID
        AND SF_NATIVE_ASSET_ID = SFCA.SF_NATIVE_ASSET_ID
        AND SF_SITE_ID = SFCA.SF_SITE_ID
    );

--Merge
MERGE INTO SF_CONTACT_ASSIGNEE_TEMP SFCA
    USING (
        SELECT DISTINCT
            SFC.ID SF_NATIVE_CONTACT_ID,
            SFA.ID SF_NATIVE_ASSET_ID,
            'Y' AS IS_SERVICE_PROVIDER,
            DISBAM.PRIORITY AS PRIORITY,
            'Y' AS IS_ACTIVE,
            SFS.SF_SITE_ID SF_SITE_ID,
            'YQ' AS SEND_FLAG,
            G_CREATED_ON CREATED_ON,
            G_CREATED_BY CREATED_BY,
            G_MODIFIED_ON MODIFIED_ON,
            G_MODIFIED_BY MODIFIED_BY
        FROM
            SF_CONTACT_TEMP SFC,
            SF_SITE SFS,
            MSS_DISBAM_SVU_STAGE DISBAM,
            SF_ASSET_TEMP SFA
        WHERE
            SFC.IS_SERVICE_PROVIDER = 'Y'
            AND SFC.IS_ACTIVE = 'Y'
            AND SFC.LAST_NAME = DISBAM.NAME_FMT
            AND SFC.ADDRESS = DISBAM.ABSERVICEPROVID
            AND SFC.PHONE = NVL(CASE WHEN INSTR(DISBAM.PRIMARYPHONE,',')>0 AND LENGTH(DISBAM.PRIMARYPHONE)>20 THEN SUBSTR(DISBAM.PRIMARYPHONE,0,(INSTR(DISBAM.PRIMARYPHONE,',')-1)) ELSE TRIM(DISBAM.PRIMARYPHONE) END, '000-000-0000')
            AND SFA.EQUIPMENT = 'SVU ' || SUBSTR(DISBAM.FAILURE_CODE, 5 ,8)
            AND SFA.SF_SITE_ID = SFS.SF_SITE_ID
            AND TRIM(DISBAM.BANNER || ' ' || DISBAM.LOCATION) = TRIM(SFS.SF_SITE_NAME)
            AND SFS.SF_CUST_ID = i_cust_id

        UNION

        SELECT DISTINCT
            SFC.ID SF_NATIVE_CONTACT_ID,
            SFA.ID SF_NATIVE_ASSET_ID,
            'Y' AS IS_SERVICE_PROVIDER,
            DECODE(DISBAM.EMER_TYPE, 'EMR1', '200', 'EMR2', '300', 'DM', '400') PRIORITY,
            'Y' AS IS_ACTIVE,
            SFS.SF_SITE_ID SF_SITE_ID,
            'YQ' AS SEND_FLAG,
            G_CREATED_ON CREATED_ON,
            G_CREATED_BY CREATED_BY,
            G_MODIFIED_ON MODIFIED_ON,
            G_MODIFIED_BY MODIFIED_BY
        FROM
            SF_CONTACT_TEMP SFC,
            SF_SITE SFS,
            MSS_DISBAM_SVU_EMR_PROV_STAGE DISBAM,
            SF_ASSET_TEMP SFA
        WHERE
            SFC.IS_SERVICE_PROVIDER = 'Y'
            AND SFC.IS_ACTIVE = 'Y'
            AND SFC.LAST_NAME = DISBAM.EMER_CONTACT_1_FMT
            AND SFC.ADDRESS = DISBAM.EMER_CONTACT_ID
            AND SFC.PHONE = NVL(CASE WHEN INSTR(DISBAM.EMER_PHONE_1,',')>0 AND LENGTH(DISBAM.EMER_PHONE_1)>20 THEN SUBSTR(DISBAM.EMER_PHONE_1,0,(INSTR(DISBAM.EMER_PHONE_1,',')-1)) ELSE TRIM(DISBAM.EMER_PHONE_1) END, '000-000-0000')
            AND SFA.EQUIPMENT IS NOT NULL 
            AND SFA.SF_SITE_ID = SFS.SF_SITE_ID
            AND TRIM(DISBAM.BANNER || ' ' || DISBAM.LOCATION) = TRIM(SFS.SF_SITE_NAME)
            AND SFS.SF_CUST_ID = i_cust_id
    ) MDSS
    ON (
        SFCA.SF_NATIVE_CONTACT_ID = MDSS.SF_NATIVE_CONTACT_ID 
        AND SFCA.SF_NATIVE_ASSET_ID = MDSS.SF_NATIVE_ASSET_ID 
        AND SFCA.SF_SITE_ID = MDSS.SF_SITE_ID 
        AND SFCA.IS_ACTIVE = 'Y'
        AND SFCA.IS_SERVICE_PROVIDER = 'Y'
    )
    WHEN MATCHED THEN
        UPDATE SET
            SFCA.PRIORITY = MDSS.PRIORITY,
            SFCA.SEND_FLAG = 'YQ',
            SFCA.MODIFIED_ON = G_MODIFIED_ON,
            SFCA.MODIFIED_BY = G_MODIFIED_BY
        WHERE
            SFCA.PRIORITY <> MDSS.PRIORITY
    WHEN NOT MATCHED THEN
        INSERT(
            SF_NATIVE_CONTACT_ID,
            SF_NATIVE_ASSET_ID,
            IS_SERVICE_PROVIDER,
            PRIORITY,
            IS_ACTIVE,
            SF_SITE_ID,
            SEND_FLAG,
            CREATED_ON,
            CREATED_BY,
            MODIFIED_ON,
            MODIFIED_BY
        )
        VALUES(
            MDSS.SF_NATIVE_CONTACT_ID,
            MDSS.SF_NATIVE_ASSET_ID,
            MDSS.IS_SERVICE_PROVIDER,
            MDSS.PRIORITY,
            MDSS.IS_ACTIVE,
            MDSS.SF_SITE_ID,
            MDSS.SEND_FLAG,
            MDSS.CREATED_ON,
            MDSS.CREATED_BY,
            MDSS.MODIFIED_ON,
            MDSS.MODIFIED_BY
        );

           g_info_msz :='   ASSIGN_PROVIDERS Assign DIVMAINT providers to Activity  :' || sql%ROWCOUNT;
           Write_Log_File(File_Name => V_File_Name ,Info     => G_Info_Msz,O_Return_Status  => V_Return_Status);
           Debug_Mesg( I_File_Type => V_Log_Type,  I_Mesg  => G_Info_Msz );

            o_return_status := 'S';

EXCEPTION WHEN OTHERS THEN
   o_return_status := 'E';
     --FND_FILE.PUT_LINE(FND_FILE.LOG, ' Unhandled exception in ASSIGN_PROVIDERS proc ' || SQLERRM );
     DBMS_OUTPUT.PUT_LINE( ' Unhandled exception in ASSIGN_PROVIDERS proc ' || SQLERRM );
      g_info_msz :=' Unhandled exception in ASSIGN_PROVIDERS proc ' || SQLERRM ;

            WRITE_LOG_FILE(file_name => v_file_name
                  ,info     => g_info_msz
                  ,o_return_status  => v_return_status);
END ASSIGN_PROVIDERS;

PROCEDURE FORMAT_DATA( o_return_status OUT VARCHAR2 ) IS
BEGIN

    Debug_Mesg(v_log_type, '=========================================================================');
    Debug_Mesg(v_log_type, 'Formatting The Data in Interface Tables in MSS_SUPERVAL_DISBAM_STG, MSS_DISBAM_SVU_EMR_PROV_STAGE ');
    UPDATE
     MSS_DISBAM_SVU_STAGE
    SET
       BANNER                 = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from BANNER)))))
       ,ABSERVICEPROVID        = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from ABSERVICEPROVID)))))
       ,LOCATION               = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from LOCATION)))))
       ,FAILURE_CODE           = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from FAILURE_CODE)))))
       ,Company                = Trim(Both '"'  From Trim(Both Chr(13) From Trim(Both Chr(32) From Trim(Both Chr(9) From Trim(Both '"' From Company)))))
       ,NAME_FMT               = DECODE(FAILURE_CODE,NULL,NULL,DECODE(NAME,NULL,NULL,DECODE(trade,'DIVMAINT', 'FM ' || trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from NAME))))),
                                Trim(Both '"'  From Trim(Both Chr(13) From Trim(Both Chr(32) From Trim(Both Chr(9) From Trim(Both '"' From Name))))))||' '||
                                Trim(Both '"'  From Trim(Both Chr(13) From Trim(Both Chr(32) From Trim(Both Chr(9) From Trim(Both '"' From Abserviceprovid)))))))
       ,Priority               = Decode(Trade,'DIVMAINT',500,Trim(Both '"'  From Trim(Both Chr(13) From Trim(Both Chr(32) From Trim(Both Chr(9) From Trim(Both '"' From Priority))))))
       ,TRADE                  = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from TRADE)))))
       ,PRIMARYPHONE           = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from PRIMARYPHONE)))))
       ,PHONE02                = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from PHONE02)))))
       ,PAGER                  = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from PAGER)))))
       ,CELL                   = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from CELL)))))
       ,FAX                    = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from FAX)))))
       ,EMAIL                  = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from EMAIL)))))
       ,FEM_NAME               = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from FEM_NAME)))))
       ,FEM_PHONE              = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from FEM_PHONE)))))
       --,XXCS_DISBAM_SVU_STAGE_ID    = NVL(xxcs_disbam_svu_stage_id, mss_disbam_svu_stage_seq.NEXTVAL)
       ;

    COMMIT;

    UPDATE
        mss_disbam_svu_emr_prov_stage
    SET
      BANNER                           = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from BANNER)))))
      ,LOCATION                        = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from LOCATION)))))
      ,EMER_TYPE                       = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from EMER_TYPE)))))
      ,EMER_CONTACT_CODE               = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from EMER_CONTACT_CODE)))))
      ,EMER_CONTACT_ID                 = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from EMER_CONTACT_ID)))))
      ,Emer_Contact_1_Fmt              = Decode(Emer_Contact_1,Null,Null,Decode(Emer_Type,'EMR1','EMR 1 ','EMR2','EMR 2 ','DM','DM ') || Trim(Both '"'  From Trim(Both Chr(13) From Trim(Both Chr(32) From Trim(Both Chr(9) From Trim(Both '"' From Emer_Contact_1)))))
                                        ||' '||Trim(Both '"'  From Trim(Both Chr(13) From Trim(Both Chr(32) From Trim(Both Chr(9) From Trim(Both '"' From Emer_Contact_Id))))))
      ,EMER_PHONE_1                    = trim(BOTH '"'  from trim(BOTH chr(13) from trim(BOTH chr(32) from trim(BOTH chr(9) from trim(BOTH '"' from EMER_PHONE_1)))))
      --,DISBAM_SVU_EMR_PROV_ID = NVL(disbam_svu_emr_prov_id, mss_disbam_svu_emr_prov_seq.NEXTVAL)
      ;
    COMMIT;

    Debug_Mesg(v_log_type, 'Formatted the data successfully' );
    Debug_Mesg(v_log_type, '=========================================================================');
    o_return_status := 'S';

EXCEPTION WHEN OTHERS THEN
     --FND_FILE.PUT_LINE(FND_FILE.LOG, ' Unhandled exception in format_data ' || SQLERRM );
     DBMS_OUTPUT.PUT_LINE( ' Unhandled exception in format_data ' || SQLERRM );
      g_info_msz :=' Unhandled exception in format_data ' || SQLERRM ;

            WRITE_LOG_FILE(file_name => v_file_name
                  ,info     => g_info_msz
                  ,o_return_status  => v_return_status);
END format_data;


PROCEDURE VALIDATE_PROC(
    errbuf OUT VARCHAR2,
    retcode OUT VARCHAR2,
    i_customer             IN VARCHAR2,
    i_format_data          IN VARCHAR2,
    i_disbam_upt           IN VARCHAR2,
    i_provider_upt         IN VARCHAR2,
    i_assign_providers     IN VARCHAR2,
    i_emr_prov_upt         IN VARCHAR2,
    i_assign_emr_providers IN VARCHAR2,
    i_load_alarm_contacts  IN VARCHAR2 )
IS
  v_validate_excep  EXCEPTION;
  v_execution_excep EXCEPTION;
  v_return_status   VARCHAR2(1);
  v_cust_id         VARCHAR2(100) := NULL;
BEGIN
    LOG_FILE (directory_name => g_directory ,log_filename=>i_customer, file_name => v_file_name ,o_return_status => v_return_status);
    G_Info_Msz := '---------------------------------'|| Chr(13) || Chr(10)||'VMS Package Execution Started @ '||To_Char(Sysdate,'MM/DD/YYYY HH24:MI:SS')|| Chr(13) || Chr(10)||'----------------------------------------------------------------------------------';
    Write_Log_File(File_Name => V_File_Name ,Info => G_Info_Msz ,O_Return_Status => V_Return_Status) ;
    Debug_Mesg( I_File_Type => V_Log_Type,  I_Mesg  => G_Info_Msz );

    IF i_customer IS NULL THEN
      Debug_Mesg( i_file_type => v_log_type, i_mesg => ' Customer name is required ' );
      RAISE v_validate_excep;
    End If;

    T1 := DBMS_UTILITY.get_time;
    --Cache_Values (O_Return_Status => V_Return_Status);
    T2 := Dbms_Utility.Get_Time;

    g_info_msz := '===Cache_Values Execution time:'||TO_CHAR((T2       -T1)/100,'999.999')||' v_return_status:'||v_return_status;
    Write_Log_File(File_Name => V_File_Name ,Info => G_Info_Msz,O_Return_Status => V_Return_Status);
    Debug_Mesg( I_File_Type => V_Log_Type,  I_Mesg  => G_Info_Msz );

    IF v_return_status <> 'S' THEN
      Debug (i_mesg => 'Error Caching variables ');
    ELSE
      Debug (i_mesg => 'Not getting any Error on Caching variables ');
    END IF;

    T1 := Dbms_Utility.Get_Time;
    VALIDATE_PARAM( i_customer => i_customer,o_cust_id => v_cust_id,o_return_status => v_return_status);
    T2 := Dbms_Utility.Get_Time;

    g_info_msz := '===VALIDATE_PARAM Execution time:'||TO_CHAR((T2-T1)/100,'999.999')||' v_return_status:'||v_return_status;
    Write_Log_File(File_Name => V_File_Name ,Info => G_Info_Msz,O_Return_Status => V_Return_Status);
    Debug_Mesg( I_File_Type => V_Log_Type,  I_Mesg  => G_Info_Msz );

    IF v_return_status <> 'S' THEN
      RAISE v_validate_excep;
    End If;

    IF NVL(i_format_data,'N') = 'Y' THEN
      T1 := Dbms_Utility.Get_Time;
      Format_Data( O_Return_Status => V_Return_Status );
      T2 := Dbms_Utility.Get_Time;

      g_info_msz := '===Format_Data Execution time:'||TO_CHAR((T2       -T3)/100,'999.999')||' v_return_status:'||v_return_status;
      Write_Log_File(File_Name => V_File_Name ,Info => G_Info_Msz,O_Return_Status => V_Return_Status);
      Debug_Mesg( I_File_Type => V_Log_Type,  I_Mesg  => G_Info_Msz );

    End If;

    IF v_return_status <> 'S' THEN
      RAISE v_validate_excep;
    End If;

    IF NVL(I_Disbam_Upt,'N') = 'Y' THEN
      T3   := DBMS_UTILITY.get_time;
      Load_Dispatch_Data( I_Cust_Id => V_Cust_Id, O_Return_Status => V_Return_Status );
      T2 := Dbms_Utility.Get_Time;

      g_info_msz := '===Load_Dispatch_Data Execution time:'||TO_CHAR((T2       -T3)/100,'999.999')||' v_return_status:'||v_return_status;
      Write_Log_File(File_Name => V_File_Name ,Info => G_Info_Msz,O_Return_Status => V_Return_Status);
      Debug_Mesg( I_File_Type => V_Log_Type,  I_Mesg  => G_Info_Msz );

    End If;

    IF v_return_status <> 'S' THEN
      RAISE v_execution_excep;
    END IF;

    IF NVL(I_Provider_Upt,'N') = 'Y' THEN
      T3 := DBMS_UTILITY.get_time;
      LOAD_PROV_DATA( I_Cust_Id => V_Cust_Id, o_return_status => v_return_status );
      T2 := Dbms_Utility.Get_Time;
      g_info_msz := '===LOAD_PROV_DATA Execution time:'||TO_CHAR((T2-T3)/100,'999.999')||' v_return_status:'||v_return_status;
      WRITE_LOG_FILE(file_name => v_file_name ,info => g_info_msz,o_return_status => v_return_status);
      Debug_Mesg( I_File_Type => V_Log_Type,  I_Mesg  => G_Info_Msz );
    END IF;

--    IF NVL(I_Provider_Upt,'N') = 'Y' THEN
--      T3 := DBMS_UTILITY.get_time;
--      LOAD_PROV_CONTACT_DATA_INSERT(o_return_status => v_return_status );
--      T2 := Dbms_Utility.Get_Time;
--
--      g_info_msz := '===LOAD_PROV_CONTACT_DATA_INSERT Execution time:'||TO_CHAR((T2-T3)/100,'999.999')||' v_return_status:'||v_return_status;
--      Write_Log_File(File_Name => V_File_Name ,Info => G_Info_Msz,O_Return_Status => V_Return_Status);
--      Debug_Mesg( I_File_Type => V_Log_Type,  I_Mesg  => G_Info_Msz );
--    End If;

    IF v_return_status <> 'S' THEN
      RAISE v_execution_excep;
    END IF;

--    IF NVL(I_Emr_Prov_Upt,'N') = 'Y' THEN
--      T3 := DBMS_UTILITY.get_time;
--      LOAD_EMERGENCY_PROV( I_Cust_Id => V_Cust_Id, o_return_status => v_return_status );
--      T2 := Dbms_Utility.Get_Time;
--      g_info_msz := '===LOAD_EMERGENCY_PROV Execution time:'||TO_CHAR((T2-T3)/100,'999.999')||' v_return_status:'||v_return_status;
--      Write_Log_File(File_Name => V_File_Name ,Info => G_Info_Msz,O_Return_Status => V_Return_Status);
--      Debug_Mesg( I_File_Type => V_Log_Type,  I_Mesg  => G_Info_Msz );
--    End If;

--    IF v_return_status <> 'S' THEN
--      RAISE v_execution_excep;
--    End If;

--    IF NVL(I_Emr_Prov_Upt,'N') = 'Y' THEN
--      T3  := DBMS_UTILITY.get_time;
--      LOAD_EMER_CONTACT_DATA_INSERT(o_return_status => v_return_status );
--      T2 := Dbms_Utility.Get_Time;
--      g_info_msz := '===LOAD_EMER_CONTACT_DATA_INSERT Execution time:'||TO_CHAR((T2-T3)/100,'999.999')||' v_return_status:'||v_return_status;
--      Write_Log_File(File_Name => V_File_Name ,Info => G_Info_Msz,O_Return_Status => V_Return_Status);
--      Debug_Mesg( I_File_Type => V_Log_Type,  I_Mesg  => G_Info_Msz );
--    End If;
--
--    IF v_return_status <> 'S' THEN
--      RAISE v_execution_excep;
--    END IF;

--    T3 := DBMS_UTILITY.get_time;
--    POPULATE_SERVICE_GROUP (o_return_status => v_return_status );
--    T2         := Dbms_Utility.Get_Time;
--    g_info_msz := '===POPULATE_SERVICE_GROUP Execution time:'||TO_CHAR((T2-T3)/100,'999.999')||' v_return_status:'||v_return_status;
--    Write_Log_File(File_Name => V_File_Name ,Info => G_Info_Msz,O_Return_Status => V_Return_Status);
--    Debug_Mesg( I_File_Type => V_Log_Type,  I_Mesg  => G_Info_Msz );
--
--    IF v_return_status <> 'S' THEN
--      RAISE v_execution_excep;
--    End If;

--    IF NVL(i_assign_providers,'N') = 'Y' AND NVL(i_assign_emr_providers,'N') = 'Y' THEN
--      T3 := DBMS_UTILITY.get_time;
--      Update_Providers( I_Cust_Id => V_Cust_Id, O_Return_Status => V_Return_Status );
--      T2 := Dbms_Utility.Get_Time;
--      g_info_msz := '===Update_Providers Execution time:'||TO_CHAR((T2-T3)/100,'999.999')||' v_return_status:'||v_return_status;
--      Write_Log_File(File_Name => V_File_Name ,Info => G_Info_Msz,O_Return_Status => V_Return_Status);
--       Debug_Mesg( I_File_Type => V_Log_Type,  I_Mesg  => G_Info_Msz );
--    End If;

    IF NVL(I_Assign_Providers,'N') = 'Y' THEN
      T3 := DBMS_UTILITY.get_time;
      ASSIGN_PROVIDERS( i_cust_id => v_cust_id, o_return_status => v_return_status );
      T2 := Dbms_Utility.Get_Time;
      g_info_msz := '===ASSIGN_PROVIDERS Execution time:'||TO_CHAR((T2-T3)/100,'999.999')||' v_return_status:'||v_return_status;
      WRITE_LOG_FILE(file_name => v_file_name ,info => g_info_msz,o_return_status => v_return_status);
    End If;

--    IF v_return_status <> 'S' THEN
--      RAISE v_execution_excep;
--    End If;

--    IF NVL(I_Assign_Emr_Providers,'N') = 'Y' THEN
--      T3 := DBMS_UTILITY.get_time;
--      ASSIGN_EMERGENCY_PROVIDERS( i_cust_id => v_cust_id, o_return_status => v_return_status );
--      T2  := Dbms_Utility.Get_Time;
--      g_info_msz := '===ASSIGN_EMERGENCY_PROVIDERS Execution time:'||TO_CHAR((T2-T3)/100,'999.999')||' v_return_status:'||v_return_status;
--      Write_Log_File(File_Name => V_File_Name ,Info => G_Info_Msz,O_Return_Status => V_Return_Status);
--      Debug_Mesg( I_File_Type => V_Log_Type,  I_Mesg  => G_Info_Msz );
--    End If;
--
--    IF v_return_status <> 'S' THEN
--      RAISE v_execution_excep;
--    End If;

    IF NVL(I_Load_Alarm_Contacts,'N') = 'Y' THEN
      T3  := DBMS_UTILITY.get_time;
      LOAD_ALARM_EMER_CONTACTS( i_cust_id => v_cust_id, o_return_status => v_return_status );
      T2 := Dbms_Utility.Get_Time;
      g_info_msz := '===LOAD_ALARM_EMER_CONTACTS Execution time:'||TO_CHAR((T2       -T3)/100,'999.999')||' v_return_status:'||v_return_status;
      Write_Log_File(File_Name => V_File_Name ,Info => G_Info_Msz,O_Return_Status => V_Return_Status);
      Debug_Mesg( I_File_Type => V_Log_Type,  I_Mesg  => G_Info_Msz );
    End If;

    IF v_return_status <> 'S' THEN
      RAISE v_execution_excep;
    End If;

    Commit;
    G_Info_Msz := '----------------------------------------'|| Chr(13) || Chr(10)||'VMS Execution Completed @  '||To_Char(Sysdate,'MM/DD/YYYY HH24:MI:SS')|| Chr(13) || Chr(10)||'----------------------------------------------------------------------------------';
    Debug_Mesg( I_File_Type => V_Log_Type,  I_Mesg  => G_Info_Msz );
    Write_Log_File(File_Name => V_File_Name ,Info => G_Info_Msz ,O_Return_Status => V_Return_Status);

    CLOSE_LOG_FILE(v_file_name);
EXCEPTION
    WHEN v_validate_excep THEN
      --FND_FILE.PUT_LINE(FND_FILE.LOG, ' Validation failed in mss_disbam_sv_pkg.validate_proc ' );
      dbms_output.put_line(' Validation failed in mss_disbam_sv_pkg.validate_proc ' );
      g_info_msz :='  Validation failed in mss_disbam_sv_pkg.validate_proc ';
      WRITE_LOG_FILE(file_name => v_file_name ,info => g_info_msz ,o_return_status => v_return_status);
      Close_Log_File(V_File_Name);

    WHEN v_execution_excep THEN
      --FND_FILE.PUT_LINE(FND_FILE.LOG, ' Execution error in mss_disbam_sv_pkg.validate_proc ' );
      dbms_output.put_line('Execution error in mss_disbam_sv_pkg.validate_proc ' );
      g_info_msz :='  Execution error in mss_disbam_sv_pkg.validate_proc ';
      WRITE_LOG_FILE(file_name => v_file_name ,info => g_info_msz ,o_return_status => v_return_status);
      Close_Log_File(V_File_Name);

End Validate_Proc;
END Mss_Disbam_Sv_Pkg;