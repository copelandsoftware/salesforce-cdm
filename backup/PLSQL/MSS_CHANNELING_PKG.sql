create or replace PACKAGE BODY     MSS_CHANNELING_PKG AS
  /*=========================================================================================
  ||  PROJECT NAME          : Monitoring Service Systems Replacement(MSSR)
  ||  APPLICATION NAME      : Oracle Based Enrichment and Routing
  ||  SCRIPT NAME           : mss_channeling_pkg
  ||  CREATION INFORMATION
  ||       07/22/11         : Neelam Maheshwari
  ||
  ||  SCRIPT DESCRIPTION / USAGE
  ||     This Package is used for enrichment of pushing alarms from MSS_RAW_ALARM into MSS_NORM_ALARM
  ||  MODIFICATION HISTORY
  ||      Ver   DATE      Author             Description
  ||     ----   --------  -------------      -----------------------------------------
  ||   1.0    07/22/11    Neelam Maheshwari  Created
  ||   2.0    04/02/14    Gary Sun           Refactoring
  ||
  =========================================================================================== */
   g_default_sm_cust_id  NUMBER          := NULL;
   g_default_sm_site_id  NUMBER          := NULL;
   g_default_role_id     NUMBER          := NULL;

   --chandra, sep,2018 static priority number
   g_static_priority_no NUMBER := 500;
   g_sf_default_group VARCHAR2(50) := 'Supervisor';
   g_sf_default_cust_id VARCHAR2(18);
   g_sf_default_site_id VARCHAR2(18);

   g_limit_count        NUMBER          := 10000;

   g_created_by                 VARCHAR2(30) := 'MSSR';
   g_modified_by                VARCHAR2(30) := 'MSSR';

   g_directory                  VARCHAR2(130) := 'MSSCHANNELINGPROC';
   g_info_msz                   VARCHAR2 (2000);
   g_file_name                  UTL_FILE.FILE_TYPE;

   --
   --Added the Procedure P_Log_File to create a Dynamic file where we can write the log of the Channeling Package
   --
 PROCEDURE P_Log_File (directory_name IN VARCHAR2,file_name OUT UTL_FILE.FILE_TYPE, o_return_status OUT VARCHAR2) AS

  P_Log_File UTL_FILE.FILE_TYPE;

  BEGIN
    P_Log_File := UTL_FILE.FOPEN(directory_name,'MSSR_CHANNELINGPROC'||'.log','a');
    file_name := P_Log_File;

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
    UTL_FILE.FCLOSE(P_Log_File);
    o_return_status := 'E';
  END P_Log_File;
   --
   --Added the Procedure P_WriteLog_File to write the log of the Channeling Package
   --
 PROCEDURE P_WriteLog_File (file_name IN UTL_FILE.FILE_TYPE, info IN VARCHAR2,o_return_status OUT VARCHAR2 ) AS
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
  END P_WriteLog_File;
   --
   --Added the Procedure P_CloseLog_File to write the log of the Channeling Package
   --
 PROCEDURE P_CloseLog_File (file_name IN UTL_FILE.FILE_TYPE) AS
  BEGIN
   IF utl_file.is_open(file_name) THEN
      utl_file.fclose_all;
   END IF;
  END P_CloseLog_File;
   --
   --Write Log
   --
 PROCEDURE P_LogError_Table( i_alm_id IN NUMBER
                            ,i_error_name IN VARCHAR2
                            ,i_error    IN VARCHAR2
                           ) IS
     v_error_id NUMBER;
     v_return_status              VARCHAR2 (1);
     begin
         g_info_msz :='INFO:- description is :- '||'Error Nmae :-'||i_error_name||'   '|| SUBSTR ('Alm Id = ' || i_alm_id || '  ' || i_error,1,250);
         dbms_output.put_line(g_info_msz);
         P_WriteLog_File(file_name => g_file_name
                         ,info     => g_info_msz
                         ,o_return_status  => v_return_status
                       ) ;
  END P_LogError_Table;
   --
   --Get Cache Default Value
   --
 PROCEDURE P_Cache_Default_Sitemapping( x_return_status OUT VARCHAR2 ) IS

   CURSOR c_sitemapping IS
   SELECT msm.site_id
         ,c.cust_id
   FROM   mss_site_mapping msm
         ,cmn_cust c
         ,cmn_site s
   WHERE  msm.site_id = s.site_id
   AND    s.cust_id   = c.cust_id
   AND    msm.site_mapping_value = '*';

  CURSOR C_DEFAULT_ROLE IS
   select cr.rol_id
     from cmn_rol_type_ref crtr
         ,cmn_rol cr
         ,mss_routing_group_map mrgm
   where crtr.ROL_TYPE_ID = cr.ROL_TYPE_ID
     and crtr.rol_type_descr = 'QUEUE'
     and cr.rol_id = mrgm.rol_id
     and mrgm.cust_id =(select c.cust_id from cmn_cust c where c.cust_name='ERSMON')
     and mrgm.site_id = (select   min(s.site_id)
                          from
                            cmn_site s,
                            cmn_cust c,
                            --chandra jan, 23, 2012 table is changed in cdm
                     cmn_reference r
                          where
                     s.cust_id=c.cust_id
                          --fixed on sep,02,2011,
                    and r.ref_id = s.site_id
                          and r.ref_name like 'ERS%'
                          and c.cust_name='ERSMON');

   --chandra sep,2018 salesforce detault cust id, site id ersmon
   CURSOR c_sf_default_cust IS
   SELECT
     c.sf_cust_id
     ,s.sf_site_id
   from
     sf_customer c
     ,sf_site s
   where
    c.sf_cust_id = s.sf_cust_id
    and c.sf_cust_name = 'ERSMON'
    and s.sf_site_name = 'ERSMON 00100';

   v_no_df_sm EXCEPTION;

 BEGIN

   FOR sm_rec IN C_Sitemapping LOOP

     g_default_sm_site_id  := sm_rec.site_id;
     g_default_sm_cust_id  := sm_rec.cust_id ;

   END LOOP;

   FOR role_rec IN C_DEFAULT_ROLE LOOP
     g_default_role_id := role_rec.rol_id;
   END LOOP;

   IF g_default_role_id IS NULL THEN
       select rol_id into g_default_role_id from cmn_rol where rol_name='Supervisor';
   END IF;
     dbms_output.put_line('default site id is '||g_default_sm_site_id);
     dbms_output.put_line('default cust id is'||g_default_sm_cust_id);
     dbms_output.put_line('default role iD is '||g_default_role_id);
   IF  ( g_default_sm_site_id IS NULL ) OR ( g_default_sm_cust_id IS NULL )
       OR ( g_default_role_id IS NULL )THEN
     x_return_status := 'E';
     P_LogError_Table( i_alm_id => null
                     ,i_error_name =>substr(' Either one of Default Site ID or Cust ID or Role ID is not defined',1,60)
                     ,i_error  => ' ERSMON Dummy Site Not Found'
                     );
   ELSE
       x_return_status := 'S';
   END IF;

   --chandra sep,2018 salesforce default customer
   for sf_def_cust_rec in c_sf_default_cust loop
      g_sf_default_cust_id :=  sf_def_cust_rec.sf_cust_id;
      g_sf_default_site_id := sf_def_cust_rec.sf_site_id;
   end loop;



 EXCEPTION
   WHEN v_no_df_sm THEN
     x_return_status := 'E';
   WHEN OTHERS THEN
     x_return_status := 'E';
 END P_Cache_Default_Sitemapping;
   --
   --Get Routing Group
   --
 PROCEDURE P_Get_RoutingGroup(   p_cust_id           IN  NUMBER
                               ,p_site_id           IN  NUMBER
                               ,x_routing_role_id   OUT NUMBER
                               ,x_return_status     OUT VARCHAR2
                             ) IS
   CURSOR c_grp IS
   select case
                 when p_site_id is null
                 then (
       select mrgm1.rol_id
       from   mss_routing_group_map mrgm1,cmn_service cs
       where  mrgm1.service_id=cs.service_id(+)
       and    mrgm1.cust_id = p_cust_id
       and    cs.service_name='Mobile'
       and    mrgm1.site_id    is null)
       when (select 1
       from cmn_rol_type_ref crtr
           ,cmn_rol cr
           ,mss_routing_group_map mrgm
           ,cmn_service cs
      where mrgm.service_id=cs.service_id(+)
       and crtr.ROL_TYPE_ID = cr.ROL_TYPE_ID
       and crtr.rol_type_descr = 'QUEUE'
       and cs.service_name='Mobile'
       and cr.rol_id           = mrgm.rol_id
       and   mrgm.cust_id      =p_cust_id
       AND mrgm.site_id =p_site_id) is null
           then (select cr.rol_id
       from cmn_rol_type_ref crtr
           ,cmn_rol cr
           ,mss_routing_group_map mrgm
           ,cmn_service cs
      where mrgm.service_id=cs.service_id(+)
       and crtr.ROL_TYPE_ID = cr.ROL_TYPE_ID
       and crtr.rol_type_descr = 'QUEUE'
       and    cs.service_name='Mobile'
       and cr.rol_id           = mrgm.rol_id
       and   mrgm.cust_id      = p_cust_id
       and mrgm.site_id is null)
       else (select cr.rol_id
       from cmn_rol_type_ref crtr
           ,cmn_rol cr
           ,mss_routing_group_map mrgm
           ,cmn_service cs
      where mrgm.service_id=cs.service_id(+)
       and crtr.ROL_TYPE_ID = cr.ROL_TYPE_ID
       and crtr.rol_type_descr = 'QUEUE'
       and cs.service_name='Mobile'
       and cr.rol_id           = mrgm.rol_id
       and   mrgm.cust_id      =p_cust_id
       AND mrgm.site_id =p_site_id)
       end rol_id
       from dual;

   CURSOR c_grp1 IS
   select case
                 when p_site_id is null
                 then (
       select mrgm1.rol_id
       from   mss_routing_group_map mrgm1,cmn_service cs
       where  mrgm1.service_id=cs.service_id(+)
       and    mrgm1.cust_id = p_cust_id
       and    cs.service_name='Monitoring'
       and    mrgm1.site_id    is null)
       when (select 1
           from mss_routing_group_map
           where cust_id        = p_cust_id
           and site_id = p_site_id) is null
           then (select cr.rol_id
       from cmn_rol_type_ref crtr
           ,cmn_rol cr
           ,mss_routing_group_map mrgm
           ,cmn_service cs
      where mrgm.service_id=cs.service_id(+)
       and crtr.ROL_TYPE_ID = cr.ROL_TYPE_ID
       and crtr.rol_type_descr = 'QUEUE'
       and    cs.service_name='Monitoring'
       and cr.rol_id           = mrgm.rol_id
       and   mrgm.cust_id      = p_cust_id
       and mrgm.site_id is null)
       else (select cr.rol_id
       from cmn_rol_type_ref crtr
           ,cmn_rol cr
           ,mss_routing_group_map mrgm
           ,cmn_service cs
      where mrgm.service_id=cs.service_id(+)
       and crtr.ROL_TYPE_ID = cr.ROL_TYPE_ID
       and crtr.rol_type_descr = 'QUEUE'
       and cs.service_name='Monitoring'
       and cr.rol_id           = mrgm.rol_id
       and   mrgm.cust_id      =p_cust_id
       AND mrgm.site_id =p_site_id)
       end rol_id
       from dual;

   v_role_id    NUMBER       := NULL;
   v_found      VARCHAR2(1);
   v_mobile_service VARCHAR2(1);

  --Mobile service first
  BEGIN
  FOR service in (
       select cs.service_id
       from mss_monitor_service_site mmss,cmn_service cs
       where mmss.service_id=cs.service_id and site_id=p_site_id and cs.service_name='Mobile'
     )loop
         v_mobile_service :='Y';
     end loop;

     v_found := 'N';
     IF v_mobile_service IS NOT NULL THEN
       FOR grp_rec in  c_grp LOOP
         v_role_id   := grp_rec.rol_id ;
         IF v_role_id IS NOT NULL THEN
            v_found := 'Y';
         END IF;
       END LOOP;

       IF v_found = 'N' THEN
           select rol_id into v_role_id from cmn_rol where rol_name='Mobile';
		   v_found :='Y';
       END IF;
     ELSE
       FOR grp_rec1 in  c_grp1 LOOP
         v_role_id   := grp_rec1.rol_id ;
         IF v_role_id IS NOT NULL THEN
            v_found := 'Y';
         END IF;
       END LOOP;
     END IF;

  IF  v_found = 'Y' THEN
     x_routing_role_id := v_role_id;
     x_return_status    := 'S';
  ELSE
     --If a customer doesn't configure routing group, it will use the default routing group "Supervisor"
     select rol_id into v_role_id from cmn_rol where rol_name='Supervisor';
     x_routing_role_id := v_role_id;
     x_return_status := 'E';
  END IF;
 EXCEPTION
   WHEN OTHERS THEN
     x_return_status := 'E';
     MSS_SEND_MAIL_PKG.send_error_to_mail
     (
          p_application_name => 'MSSR Channeling PL/SQL',
          p_error_message => ' Unhandled Exception in P_Get_RoutingGroup proc p_cust_id =>' || p_cust_id || '. Error is ' || SQLERRM,
		  p_error_code=>'10003'
    );
     dbms_output.put_line(' Unhandled Exception in P_Get_RoutingGroup proc p_cust_id =>' || p_cust_id || '. Error is ' || SQLERRM   );
 END P_Get_RoutingGroup;
   --
   --Get Site Mapping
   --
 PROCEDURE P_Get_Sitemapping(    p_site_name    IN VARCHAR2
                               ,x_cust_id          OUT NUMBER
                               ,x_channel_site_id  OUT NUMBER
                               ,x_return_status    OUT VARCHAR2
                           ) IS
                           
   CURSOR c_site_map IS
   SELECT s.mss_site_id
         ,c.mss_cust_id
   FROM   sf_dispatch_mapping sdm
         ,sf_customer c
         ,sf_site s
   WHERE  sdm.sf_cust_id          = s.sf_site_id
   AND    s.sf_cust_id            = c.sf_cust_id
   AND    sdm.external_value = p_site_name;
   v_cust_id NUMBER;
   v_site_id NUMBER;
   v_found   VARCHAR2(1);
  BEGIN
   FOR site_map_rec IN c_site_map LOOP
     v_cust_id := site_map_rec.mss_cust_id;
     v_site_id := site_map_rec.mss_site_id;
     v_found := 'Y';
   END LOOP;


   IF v_found = 'Y' THEN
     x_cust_id         := v_cust_id;
     x_channel_site_id := v_site_id;
     x_return_status   := 'S';
    /*--------------------------------------------------------------
      dbms_output.put_line('x_cust_id1'||v_cust_id);
      dbms_output.put_line('x_channel_site_id1'||v_site_id);
      dbms_output.put_line(x_return_status);
     --------------------------------------------------------------*/
   ELSE
     x_return_status := 'E';
   END IF;
   EXCEPTION
   WHEN OTHERS THEN
     x_return_status := 'E';
 END P_Get_Sitemapping;
   --
   --Get Site Mapping for given caller_id
   --
 PROCEDURE P_Get_Commmapping(  p_uard_area_code        IN  VARCHAR2
                              ,p_uard_phone            IN  VARCHAR2
                              ,x_cust_id               OUT NUMBER
                              ,x_site_id               OUT NUMBER
                              ,x_return_status         OUT VARCHAR2
                            ) IS
   CURSOR    c_comm_map IS
   SELECT    mci.cust_id
            ,mci.site_id
   FROM      mss_site_mapping msm
            ,mss_caller_id mci
   WHERE     msm.site_id                 = mci.site_id
   AND       nvl(substr(mci.caller_id_value ,5,LENGTH(mci.caller_id_value )),-9823)= nvl(p_uard_phone, -9823)
   AND       substr(mci.caller_id_value,1,3)       = p_uard_area_code;

   v_cust_id NUMBER;
   v_site_id NUMBER;
   v_count   NUMBER := 0;
   BEGIN

   v_cust_id := NULL;
   v_site_id := NULL;
  FOR comm_rec IN c_comm_map
  LOOP
  v_cust_id := comm_rec.cust_id;
  v_site_id := comm_rec.site_id;
  v_count   := v_count + 1;
  END LOOP;
   /*
    *  Check for Single / Multiple Site Mappings for the given caller id
    *  if only single site mapping found then we are good
    *  otherwise, we have an issue, raise and exception
    */

   IF v_count > 1 THEN
     -- Since we have multiple sites for the given caller id, raise and error
     P_LogError_Table( i_alm_id => null
                     ,i_error_name => ' Multiple Channel Sites found for area code => ' || p_uard_area_code
                    ,i_error  => ' Multiple Channel Sites found'
                    );
   END IF;

   IF v_count > 0 THEN
     x_cust_id         := v_cust_id;
     x_site_id         := v_site_id;
     x_return_status   := 'S';
   ELSE
     x_cust_id := v_cust_id;
     x_site_id := v_site_id;
     x_return_status := 'E';
   END IF;

  EXCEPTION
   WHEN OTHERS THEN
     x_return_status := 'E';
 END P_Get_Commmapping;
   --
   --Get Timezone
   --
 FUNCTION F_Get_Timezone(
            I_TIMEVALUE IN DATE)
          RETURN DATE
        IS
          V_UPDATED_DATE DATE;
        BEGIN
          SELECT
            I_TIMEVALUE                             - (SUBSTR (TZ_OFFSET(SESSIONTIMEZONE), 1, INSTR (TZ_OFFSET(
            SESSIONTIMEZONE), ':')                  - 1) / 24 + SUBSTR (TZ_OFFSET(SESSIONTIMEZONE),
            INSTR (TZ_OFFSET(SESSIONTIMEZONE), ':') + 1, 2) / 1440)
          INTO
            V_UPDATED_DATE
          FROM
            DUAL;
          RETURN V_UPDATED_DATE;
          --
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
          --
          dbms_output.put_line ( 'Execution Error in getting the timezone details' );
        WHEN OTHERS THEN
          --
          dbms_output.put_line ('Unhandled exception in F_Get_Timezone');
          dbms_output.put_line (SQLERRM);
          --
    END F_Get_Timezone;
   --

   --chandra, sep,2018 identify the routing qroup from cust, site
   PROCEDURE p_get_routing_group_SF( p_cust_id IN NUMBER
   				      ,p_site_id IN NUMBER
   				      ,o_routing_group OUT VARCHAR2
   				   ) IS

	CURSOR C_Routing_grp IS
	SELECt
	  sc.sf_cust_id
	  ,sc.sf_routing_group
	  ,ss.sf_site_id
	  ,ss.sf_routing_group
	from
	JAM.SF_CUSTOMER sc
	,jam.sf_site ss
	where
	 sc.sf_cust_id = ss.sf_cust_id;


   BEGIN

     null;

   END;

   --chandra, sep,2018 get alarm static priority for SF
   PROCEDURE p_get_alarm_static_priority_SF( psf_cust_id IN VARCHAR2
   					,psf_site_id IN VARCHAR2
   					,p_alarm_desc IN VARCHAR2
   					,p_alarm_source IN VARCHAR2
   					,o_priority_no OUT NUMBER
   					,o_routing_group OUT VARCHAR2
   					) IS

      CURSOR c_static IS
 select
   alarm_priority
   ,routing_group
 from
 (SELECT
        sf_cust_id
        ,sf_site_id
        ,alarm_desc
        ,alarm_source
      	,alarm_priority
      	,routing_group
   	,case 	when (sf_cust_id IS NOT NULL) AND (sf_site_id is not null) and (alarm_desc is not null) and (alarm_source is not null) THEN
   	  	 1
        	when (sf_cust_id IS NOT NULL) AND (sf_site_id is not null) and (alarm_desc is not null) and (alarm_source is null) THEN
   	  	 2
   		when (sf_cust_id IS NOT NULL) AND (sf_site_id is not null) and (alarm_desc is null) and (alarm_source is NOT null) THEN
   	  	 3
		when (sf_cust_id IS NOT NULL) AND (sf_site_id is null) and (alarm_desc is NOT null) and (alarm_source is NOT null)  THEN
   	  	 4
		when (sf_cust_id IS NOT NULL) AND (sf_site_id is null) and (alarm_desc is NOT null) and (alarm_source is null) THEN
   	  	 5
   		when (sf_cust_id IS NOT NULL) AND (sf_site_id is null) and (alarm_desc is null) and (alarm_source is NOT null) THEN
   	  	 6
		when (sf_cust_id IS NULL) AND (sf_site_id is null) and (alarm_desc is NOT null) and (alarm_source is NOT null) THEN
   	  	 7
		when (sf_cust_id IS NULL) AND (sf_site_id is null) and (alarm_desc is NOT null) and (alarm_source is null) THEN
   	  	 8
		when (sf_cust_id IS NULL) AND (sf_site_id is null) and (alarm_desc is null) and (alarm_source is NOT null) THEN
   	  	 9
		when (sf_cust_id IS NOT NULL) AND (sf_site_id is NOT null) and (alarm_desc is null) and (alarm_source is null) THEN
          	 10
		when (sf_cust_id IS NOT NULL) AND (sf_site_id is null) and (alarm_desc is null) and (alarm_source is null) THEN
   	  	 11
    	end priority_order
      from
       SF_ALARM_STATIC_PRIORITY_CONF
      where
        nvl(sf_cust_id,psf_cust_id) = psf_cust_id
        and nvl(sf_site_id,psf_site_id) = psf_site_id
        and nvl(alarm_desc,p_alarm_desc) = p_alarm_desc
        and nvl(alarm_source, p_alarm_source) = p_alarm_source
        and status = 'Active'
       order by priority_order asc, created_on desc
    )
    where
     rownum < 2;

      v_ret_status VARCHAR2(1);

   BEGIN

	FOR static_pr_rec in c_static LOOP
   	     o_priority_no := static_pr_rec.alarm_priority;
   	     o_routing_group := static_pr_rec.routing_group;
	END LOOP;

   EXCEPTION WHEN OTHERS THEN

         P_WriteLog_File(file_name => g_file_name
                         ,info     => ' Exception in Alarm Static Priority ' || sqlerrm
                         ,o_return_status  => v_ret_status
                       ) ;

   END p_get_alarm_static_priority_SF;


   --Chandra, sep,2018 identifiy the alarm flow either MSSR or SF
   PROCEDURE p_get_alarm_flow_MSS_SF( p_cust_id IN NUMBER
   				      ,p_site_id IN NUMBER
                ,in_currentGMTTime IN DATE
   				      ,o_sf_cust_id OUT VARCHAR2
   				      ,o_sf_site_id OUT VARCHAR2
   				      ,o_sf_routing_group OUT VARCHAR2
   				      ,o_flow_flag OUT VARCHAR2
                ,o_sf_asset_id OUT VARCHAR2
                ,o_sf_site_status OUT VARCHAR2
   				     ) is
   	CURSOR c_sf_config IS
	select
	  Alarm_flow_flag
	from
 	 SF_CUST_ALARM_FLOW_CONFIG
	where
  	  mss_cust_id = p_cust_id
  	  and nvl(mss_site_id,p_site_id) = p_site_id
  	order by created_on asc;

  	CURSOR c_sf_cust_site IS
  	SELECT
  	  sc.sf_cust_id
  	  ,sc.sf_routing_group cust_routing_group
  	  ,ss.sf_site_id
  	  ,ss.sf_routing_group site_routing_group
  	from
  	 jam.sf_customer sc,
  	 jam.sf_site ss
	where
	  sc.mss_cust_id = p_cust_id
	  and ss.mss_site_id = p_site_id
	  and sc.sf_cust_id = ss.sf_cust_id;

    CURSOR c_sf_control_sys(v_sf_site_id VARCHAR2) IS
    SELECT
    SF_CONTROL_SYS_ID
    from
    SF_CONTROL_SYS
    where sf_site_id =v_sf_site_id;

    CURSOR c_sf_service_contract(v_sf_site_id VARCHAR2) IS
    SELECT 
    START_DATE,
    END_DATE
    from
    SF_SERVICE_CONTRACT
    where 
    SF_ACCOUNT_ID =v_sf_site_id;


	v_ret_status VARCHAR2(1);

   BEGIN

     o_sf_cust_id :=  NULL;
     o_sf_site_id :=  NULL;
     o_flow_flag :=  'SF';
     o_sf_routing_group := NULL;
     o_sf_asset_id := NULL;
     o_sf_site_status :='1';

     for config_rec in  c_sf_config loop

       o_flow_flag :=  config_rec.alarm_flow_flag;

     end loop;

     if o_flow_flag in ('Both','SF') then

     	for sf_cust_site_rec in c_sf_cust_site loop
          o_sf_cust_id :=  sf_cust_site_rec.sf_cust_id;
     	  o_sf_site_id :=  sf_cust_site_rec.sf_site_id;
     	  o_sf_routing_group := nvl(sf_cust_site_rec.site_routing_group,sf_cust_site_rec.cust_routing_group);
     	end loop;

      if o_sf_site_id is not null then
        for sf_control_sys_rec in c_sf_control_sys(o_sf_site_id) loop
           o_sf_asset_id:=sf_control_sys_rec.SF_CONTROL_SYS_ID;
        end loop;

        --site status
        for sf_service_contract_rec in c_sf_service_contract(o_sf_site_id) loop
          if in_currentGMTTime not between nvl(sf_service_contract_rec.start_date,sysdate-2) and nvl(sf_service_contract_rec.end_date,sysdate+2) then
            o_sf_site_status :='0';
          end if;
        end loop;

      end if;

        if (o_sf_cust_id is null) or  (o_sf_site_id is null) then
          o_sf_cust_id :=  g_sf_default_cust_id;
     	  o_sf_site_id :=  g_sf_default_site_id;
     	  o_sf_routing_group := g_sf_default_group;
     	end if;

     end if;

   EXCEPTION WHEN OTHERS THEN

         P_WriteLog_File(file_name => g_file_name
                         ,info     => ' Exception in get alarm flow mss_SF ' || sqlerrm
                         ,o_return_status  => v_ret_status
                       ) ;

   END p_get_alarm_flow_MSS_SF;

   --Main procedure
   --
 PROCEDURE MSS_CHANNELING_PROC( errbuf      OUT   VARCHAR2,
                                 retcode     OUT   VARCHAR2
                               ) IS
   CURSOR c_raw_urd_data IS
   SELECT
          alarm_id                      ,
          site_name                     ,
          receiver                      ,
          descr                         ,
          source                        ,
          time_received                 ,
          time_occurred                 ,
          time_dialout                  ,
          controller                    ,
          sub_controller                ,
          alm_type                      ,
          alm_state                     ,
          alm_priority                  ,
          caller_id                     ,
          receiver_id                   ,
          src_probable                  ,
          desc_probable                 ,
          receiver_host                 ,
          file_id                       ,
          field1                        ,
          field2                        ,
          field3                        ,
          field4                        ,
          field5                        ,
          field6                        ,
          field7                        ,
          field8                        ,
          field9                        ,
          field10                       ,
          field11                       ,
          field12                       ,
          field13                       ,
          field14                       ,
          field15                       ,
          uard_site_name                ,
          uard_area_code                ,
          uard_phone                    ,
          comments                      ,
          alarm_count_24_hrs            ,
          alarm_count_7_days            ,
          create_date                   ,
          controller_instance           ,
          request_id                    ,
          processed_flag                ,
          attribute1                    ,
          attribute2                    ,
          attribute3                    ,
          attribute4                    ,
          attribute5                    ,
          attribute6                    ,
          attribute7                    ,
          attribute8                    ,
          attribute9                    ,
          attribute10                   ,
          id1                           ,
          id2                           ,
          id3                           ,
          id4                           ,
          id5                           ,
          program_app_id                ,
          version_number                ,
          norm_desc_id                  ,
          norm_source_id                ,
          rtn_date
   FROM
     MSS_raw_alarm
   WHERE
     processed_flag = 'N'
   ORDER BY
     ALARM_ID ASC;


     v_min_flag               CHAR(1) := 'Y';
     v_min_alarm              NUMBER  := 0;
     v_max_alarm              NUMBER  := 0;
     v_processed_alarms       NUMBER := 0;
     v_max_alm_id          NUMBER := NULL;
     v_return_status       VARCHAR2(1);
     currentGMTTime        DATE;

    TYPE sm_cust_id_tab IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
    TYPE sm_site_id_tab IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
    TYPE cm_cust_id_tab IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
    TYPE cm_site_id_tab IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
    TYPE role_id_tab IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
    TYPE site_status_tab IS TABLE OF CMN_SITE.STATUS_CD%TYPE INDEX BY BINARY_INTEGER;

    TYPE alarm_id_tab IS TABLE OF mss_raw_alarm.alarm_id%TYPE INDEX BY BINARY_INTEGER;
    TYPE site_name_tab IS TABLE OF mss_raw_alarm.site_name%TYPE INDEX BY BINARY_INTEGER;
    TYPE receiver_tab IS TABLE OF mss_raw_alarm.receiver%TYPE INDEX BY BINARY_INTEGER;
    TYPE descr_tab IS TABLE OF mss_raw_alarm.descr%TYPE INDEX BY BINARY_INTEGER;
    TYPE source_tab IS TABLE OF mss_raw_alarm.source%TYPE INDEX BY BINARY_INTEGER;
    TYPE time_received_tab IS TABLE OF mss_raw_alarm.time_received%TYPE INDEX BY BINARY_INTEGER;
    TYPE time_occurred_tab IS TABLE OF mss_raw_alarm.time_occurred%TYPE INDEX BY BINARY_INTEGER;
    TYPE time_dialout_tab IS TABLE OF mss_raw_alarm.time_dialout%TYPE INDEX BY BINARY_INTEGER;
    TYPE controller_tab IS TABLE OF mss_raw_alarm.controller%TYPE INDEX BY BINARY_INTEGER;
    TYPE sub_controller_tab IS TABLE OF mss_raw_alarm.sub_controller%TYPE INDEX BY BINARY_INTEGER;
    TYPE alm_type_tab IS TABLE OF mss_raw_alarm.alm_type%TYPE INDEX BY BINARY_INTEGER;
    TYPE alm_state_tab IS TABLE OF mss_raw_alarm.alm_state%TYPE INDEX BY BINARY_INTEGER;
    TYPE alm_priority_tab IS TABLE OF mss_raw_alarm.alm_priority%TYPE INDEX BY BINARY_INTEGER;
    TYPE caller_id_tab IS TABLE OF mss_raw_alarm.caller_id%TYPE INDEX BY BINARY_INTEGER;
    TYPE receiver_id_tab IS TABLE OF mss_raw_alarm.receiver_id%TYPE INDEX BY BINARY_INTEGER;
    TYPE src_probable_tab IS TABLE OF mss_raw_alarm.src_probable%TYPE INDEX BY BINARY_INTEGER;
    TYPE desc_probable_tab IS TABLE OF mss_raw_alarm.desc_probable%TYPE INDEX BY BINARY_INTEGER;
    TYPE receiver_host_tab IS TABLE OF mss_raw_alarm.receiver_host%TYPE INDEX BY BINARY_INTEGER;
    TYPE file_id_tab IS TABLE OF mss_raw_alarm.file_id%TYPE INDEX BY BINARY_INTEGER;
    TYPE field1_tab IS TABLE OF mss_raw_alarm.field1%TYPE INDEX BY BINARY_INTEGER;
    TYPE field2_tab IS TABLE OF mss_raw_alarm.field2%TYPE INDEX BY BINARY_INTEGER;
    TYPE field3_tab IS TABLE OF mss_raw_alarm.field3%TYPE INDEX BY BINARY_INTEGER;
    TYPE field4_tab IS TABLE OF mss_raw_alarm.field4%TYPE INDEX BY BINARY_INTEGER;
    TYPE field5_tab IS TABLE OF mss_raw_alarm.field5%TYPE INDEX BY BINARY_INTEGER;
    TYPE field6_tab IS TABLE OF mss_raw_alarm.field6%TYPE INDEX BY BINARY_INTEGER;
    TYPE field7_tab IS TABLE OF mss_raw_alarm.field7%TYPE INDEX BY BINARY_INTEGER;
    TYPE field8_tab IS TABLE OF mss_raw_alarm.field8%TYPE INDEX BY BINARY_INTEGER;
    TYPE field9_tab IS TABLE OF mss_raw_alarm.field9%TYPE INDEX BY BINARY_INTEGER;
    TYPE field10_tab IS TABLE OF mss_raw_alarm.field10%TYPE INDEX BY BINARY_INTEGER;
    TYPE field11_tab IS TABLE OF mss_raw_alarm.field11%TYPE INDEX BY BINARY_INTEGER;
    TYPE field12_tab IS TABLE OF mss_raw_alarm.field12%TYPE INDEX BY BINARY_INTEGER;
    TYPE field13_tab IS TABLE OF mss_raw_alarm.field13%TYPE INDEX BY BINARY_INTEGER;
    TYPE field14_tab IS TABLE OF mss_raw_alarm.field14%TYPE INDEX BY BINARY_INTEGER;
    TYPE field15_tab IS TABLE OF mss_raw_alarm.field15%TYPE INDEX BY BINARY_INTEGER;
    TYPE uard_site_name_tab IS TABLE OF mss_raw_alarm.uard_site_name%TYPE INDEX BY BINARY_INTEGER;
    TYPE uard_area_code_tab IS TABLE OF mss_raw_alarm.uard_area_code%TYPE INDEX BY BINARY_INTEGER;
    TYPE uard_phone_tab IS TABLE OF mss_raw_alarm.uard_phone%TYPE INDEX BY BINARY_INTEGER;
    TYPE comments_tab IS TABLE OF mss_raw_alarm.comments%TYPE INDEX BY BINARY_INTEGER;
    TYPE alarm_count_24_hrs_tab IS TABLE OF mss_raw_alarm.alarm_count_24_hrs%TYPE INDEX BY BINARY_INTEGER;
    TYPE alarm_count_7_days_tab IS TABLE OF mss_raw_alarm.alarm_count_7_days%TYPE INDEX BY BINARY_INTEGER;
    TYPE create_date_tab IS TABLE OF mss_raw_alarm.create_date%TYPE INDEX BY BINARY_INTEGER;
    TYPE controller_instance_tab IS TABLE OF mss_raw_alarm.controller_instance%TYPE INDEX BY BINARY_INTEGER;
    TYPE request_id_tab IS TABLE OF mss_raw_alarm.request_id%TYPE INDEX BY BINARY_INTEGER;
    TYPE processed_flag_tab IS TABLE OF mss_raw_alarm.processed_flag%TYPE INDEX BY BINARY_INTEGER;
    TYPE attribute1_tab IS TABLE OF mss_raw_alarm.attribute1%TYPE INDEX BY BINARY_INTEGER;
    TYPE attribute2_tab IS TABLE OF mss_raw_alarm.attribute2%TYPE INDEX BY BINARY_INTEGER;
    TYPE attribute3_tab IS TABLE OF mss_raw_alarm.attribute3%TYPE INDEX BY BINARY_INTEGER;
    TYPE attribute4_tab IS TABLE OF mss_raw_alarm.attribute4%TYPE INDEX BY BINARY_INTEGER;
    TYPE attribute5_tab IS TABLE OF mss_raw_alarm.attribute5%TYPE INDEX BY BINARY_INTEGER;
    TYPE attribute6_tab IS TABLE OF mss_raw_alarm.attribute6%TYPE INDEX BY BINARY_INTEGER;
    TYPE attribute7_tab IS TABLE OF mss_raw_alarm.attribute7%TYPE INDEX BY BINARY_INTEGER;
    TYPE attribute8_tab IS TABLE OF mss_raw_alarm.attribute8%TYPE INDEX BY BINARY_INTEGER;
    TYPE attribute9_tab IS TABLE OF mss_raw_alarm.attribute9%TYPE INDEX BY BINARY_INTEGER;
    TYPE attribute10_tab IS TABLE OF mss_raw_alarm.attribute10%TYPE INDEX BY BINARY_INTEGER;
    TYPE id1_tab IS TABLE OF mss_raw_alarm.id1%TYPE INDEX BY BINARY_INTEGER;
    TYPE id2_tab IS TABLE OF mss_raw_alarm.id2%TYPE INDEX BY BINARY_INTEGER;
    TYPE id3_tab IS TABLE OF mss_raw_alarm.id3%TYPE INDEX BY BINARY_INTEGER;
    TYPE id4_tab IS TABLE OF mss_raw_alarm.id4%TYPE INDEX BY BINARY_INTEGER;
    TYPE id5_tab IS TABLE OF mss_raw_alarm.id5%TYPE INDEX BY BINARY_INTEGER;
    TYPE program_app_id_tab IS TABLE OF mss_raw_alarm.program_app_id%TYPE INDEX BY BINARY_INTEGER;
    TYPE version_number_tab IS TABLE OF mss_raw_alarm.version_number%TYPE INDEX BY BINARY_INTEGER;
    TYPE norm_desc_id_tab IS TABLE OF mss_raw_alarm.norm_desc_id%TYPE INDEX BY BINARY_INTEGER;
    TYPE norm_source_id_tab IS TABLE OF mss_raw_alarm.norm_source_id%TYPE INDEX BY BINARY_INTEGER;
    TYPE rtn_date_tab IS TABLE OF mss_raw_alarm.rtn_date%TYPE INDEX BY BINARY_INTEGER;

    --chandra, Sep,2018 changes to support Salesforce alarm flow
    TYPE sf_cust_id_tab IS TABLE OF SF_NORM_ALARM.SF_CUST_ID%TYPE INDEX BY BINARY_INTEGER;
    TYPE sf_site_id_tab IS TABLE OF SF_NORM_ALARM.SF_SITE_ID%TYPE INDEX BY BINARY_INTEGER;
    TYPE sf_asset_id_tab IS TABLE OF SF_CONTROL_SYS.SF_CONTROL_SYS_ID%TYPE INDEX BY BINARY_INTEGER;
    TYPE sf_routing_group_tab IS TABLE OF SF_NORM_ALARM.SF_ROUTING_GROUP%TYPE INDEX BY BINARY_INTEGER;
    TYPE sf_static_priority_tab IS TABLE OF SF_NORM_ALARM.ALARM_PRIORITY_VALUE%TYPE INDEX BY BINARY_INTEGER;
    TYPE sf_alarm_flow_tab is TABLE OF SF_CUST_ALARM_FLOW_CONFIG.Alarm_flow_flag%TYPE INDEX BY BINARY_INTEGER;


    lt_sm_cust_id sm_cust_id_tab;
    lt_sm_site_id sm_site_id_tab;
    lt_cm_cust_id sm_cust_id_tab;
    lt_cm_site_id sm_site_id_tab;
    lt_role_id role_id_tab;
    lt_site_status site_status_tab;
    lt_sf_site_status site_status_tab;

    lt_alarm_id     alarm_id_tab;
    lt_site_name     site_name_tab;
    lt_receiver     receiver_tab;
    lt_descr     descr_tab;
    lt_source     source_tab;
    lt_time_received     time_received_tab;
    lt_time_occurred     time_occurred_tab;
    lt_time_dialout     time_dialout_tab;
    lt_controller     controller_tab;
    lt_sub_controller     sub_controller_tab;
    lt_alm_type     alm_type_tab;
    lt_alm_state     alm_state_tab;
    lt_alm_priority     alm_priority_tab;
    lt_caller_id     caller_id_tab;
    lt_receiver_id     receiver_id_tab;
    lt_src_probable     src_probable_tab;
    lt_desc_probable     desc_probable_tab;
    lt_receiver_host     receiver_host_tab;
    lt_file_id     file_id_tab;
    lt_field1     field1_tab;
    lt_field2     field2_tab;
    lt_field3     field3_tab;
    lt_field4     field4_tab;
    lt_field5     field5_tab;
    lt_field6     field6_tab;
    lt_field7     field7_tab;
    lt_field8     field8_tab;
    lt_field9     field9_tab;
    lt_field10     field10_tab;
    lt_field11     field11_tab;
    lt_field12     field12_tab;
    lt_field13     field13_tab;
    lt_field14     field14_tab;
    lt_field15     field15_tab;
    lt_uard_site_name     uard_site_name_tab;
    lt_uard_area_code     uard_area_code_tab;
    lt_uard_phone     uard_phone_tab;
    lt_comments     comments_tab;
    lt_alarm_count_24_hrs     alarm_count_24_hrs_tab;
    lt_alarm_count_7_days     alarm_count_7_days_tab;
    lt_create_date     create_date_tab;
    lt_controller_instance     controller_instance_tab;
    lt_request_id     request_id_tab;
    lt_processed_flag     processed_flag_tab;
    lt_attribute1     attribute1_tab;
    lt_attribute2     attribute2_tab;
    lt_attribute3     attribute3_tab;
    lt_attribute4     attribute4_tab;
    lt_attribute5     attribute5_tab;
    lt_attribute6     attribute6_tab;
    lt_attribute7     attribute7_tab;
    lt_attribute8     attribute8_tab;
    lt_attribute9     attribute9_tab;
    lt_attribute10     attribute10_tab;
    lt_id1     id1_tab;
    lt_id2     id2_tab;
    lt_id3     id3_tab;
    lt_id4     id4_tab;
    lt_id5     id5_tab;
    lt_program_app_id     program_app_id_tab;
    lt_version_number     version_number_tab;
    lt_norm_desc_id     norm_desc_id_tab;
    lt_norm_source_id     norm_source_id_tab;
    lt_rtn_date     rtn_date_tab;

    --chandra, Sep,2018 changes to support Salesforce alarm flow
    lt_sf_cust_id sf_cust_id_tab;
    lt_sf_site_id sf_site_id_tab;
    lt_sf_asset_id sf_asset_id_tab;
    lt_sf_routing_group sf_routing_group_tab;
    lt_sf_static_priority sf_static_priority_tab;
    lt_sf_alarm_flow sf_alarm_flow_tab;
    v_static_rg VARCHAR2(50);
    v_static_priority NUMBER(5);

    t1 INTEGER;
    t2 INTEGER;

 BEGIN
        t1 := DBMS_UTILITY.get_time;

        P_Log_File (directory_name   => g_directory
             ,file_name        => g_file_name
             ,o_return_status  => v_return_status);

        g_info_msz := '----------------------------------------------------------------------------------'||
                      chr(13) || chr(10)||'Channeling Package Execution Started @ '||to_char(sysdate,'MM/DD/YYYY HH24:MI:SS')||
                      chr(13) || chr(10)||'----------------------------------------------------------------------------------';
         dbms_output.put_line(g_info_msz);
         P_WriteLog_File(file_name => g_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;

   ----------------------------Business Logic Begin--------------------------------------------

   --Get defualt site mapping into cache variables
   P_Cache_Default_Sitemapping( v_return_status );

   --get current time with GMT timezone
   currentGMTTime := F_Get_Timezone(sysdate);

   --Get maximum alarm id from MSS-R database
   /**
   FOR malarm_rec IN c_max_alm LOOP
     v_max_alm_id := malarm_rec.alm_id;
   END LOOP;
   IF v_max_alm_id IS NULL THEN
     P_LogError_Table( i_alm_id => null
                      ,i_error_name => ' Max Alarm id not found in MSS-R Database.'
                      ,i_error  => ' Max Alarm id not found in MSS-R Database.'
                     );
   ELSE
     g_info_msz :=LPAD(' Max Alm Id from MSS-R Database =>  ',40) || v_max_alm_id;
     dbms_output.put_line(g_info_msz);
               P_WriteLog_File(file_name => g_file_name
                  ,info     => g_info_msz
                  ,o_return_status  => v_return_status) ;
   END IF;
   **/

  OPEN c_raw_urd_data;

  LOOP
  FETCH c_raw_urd_data BULK COLLECT INTO
        lt_alarm_id,
        lt_site_name,
        lt_receiver,
        lt_descr,
        lt_source,
        lt_time_received,
        lt_time_occurred,
        lt_time_dialout,
        lt_controller,
        lt_sub_controller,
        lt_alm_type,
        lt_alm_state,
        lt_alm_priority,
        lt_caller_id,
        lt_receiver_id,
        lt_src_probable,
        lt_desc_probable,
        lt_receiver_host,
        lt_file_id,
        lt_field1,
        lt_field2,
        lt_field3,
        lt_field4,
        lt_field5,
        lt_field6,
        lt_field7,
        lt_field8,
        lt_field9,
        lt_field10,
        lt_field11,
        lt_field12,
        lt_field13,
        lt_field14,
        lt_field15,
        lt_uard_site_name,
        lt_uard_area_code,
        lt_uard_phone,
        lt_comments,
        lt_alarm_count_24_hrs,
        lt_alarm_count_7_days,
        lt_create_date,
        lt_controller_instance,
        lt_request_id,
        lt_processed_flag,
        lt_attribute1,
        lt_attribute2,
        lt_attribute3,
        lt_attribute4,
        lt_attribute5,
        lt_attribute6,
        lt_attribute7,
        lt_attribute8,
        lt_attribute9,
        lt_attribute10,
        lt_id1,
        lt_id2,
        lt_id3,
        lt_id4,
        lt_id5,
        lt_program_app_id,
        lt_version_number,
        lt_norm_desc_id,
        lt_norm_source_id,
        lt_rtn_date
  LIMIT g_limit_count;

  EXIT WHEN lt_alarm_id.COUNT = 0;

  FOR i IN lt_alarm_id.FIRST..lt_alarm_id.LAST LOOP
    IF v_min_flag = 'Y' THEN
         v_min_alarm:= lt_alarm_id(i);
    END IF;
    --get site mapping from site mapping table
    IF  ( lt_site_name(i) IS NULL ) OR
           ( LENGTH(TRIM(lt_site_name(i))) = 0 ) OR
           ( UPPER(lt_site_name(i)) = 'UNKNOWN' )
    THEN
    lt_sm_cust_id(i):=g_default_sm_cust_id;
    lt_sm_site_id(i):=g_default_sm_site_id;
    ELSE
     lt_sm_cust_id(i):= NULL;
     lt_sm_site_id(i):= NULL;
     P_Get_Sitemapping(   p_site_name        => TRIM(lt_site_name(i))
                            ,x_cust_id          => lt_sm_cust_id(i)
                            ,x_channel_site_id  => lt_sm_site_id(i)
                            ,x_return_status    => v_return_status
                       );
       IF v_return_status <> 'S' THEN
       lt_sm_cust_id(i):=g_default_sm_cust_id;
       lt_sm_site_id(i):=g_default_sm_site_id;
       lt_role_id(i):=g_default_role_id;
       END IF;
     END IF;
   --get site mapping for given caller_id
       lt_cm_cust_id(i)     := NULL;
       lt_cm_site_id(i)     := NULL;

       --lt_caller_id(i)      := NULL;
       lt_caller_id(i)      := replace(lt_caller_id(i),'-',null);
       P_Get_Commmapping(   p_uard_area_code    => lt_uard_area_code(i)
                           ,p_uard_phone        => lt_uard_phone(i)
                           ,x_cust_id           => lt_cm_cust_id(i)
                           ,x_site_id           => lt_cm_site_id(i)
                           ,x_return_status     => v_return_status
                        );
        IF v_return_status <> 'S' THEN

         g_info_msz :=' Site mapping not found from comm mapping caller id => ' || lt_caller_id(i) ;
         P_WriteLog_File(file_name => g_file_name
                  ,info     => g_info_msz
                  ,o_return_status  => v_return_status) ;
        END IF;

        /*
        *  if Channel Site not found for site mapping , then use caller id channel site
        *  but if channel site exists for site mapping then use it. ignore the caller id channel site
        *
        */
       IF ( lt_sm_cust_id(i) = g_default_sm_cust_id AND lt_sm_site_id(i) = g_default_sm_site_id ) AND
          ( lt_cm_cust_id(i) IS NOT NULL AND lt_cm_site_id(i) IS NOT NULL ) THEN
           lt_sm_cust_id(i) :=  lt_cm_cust_id(i);
           lt_sm_site_id(i) :=  lt_cm_site_id(i);
       END IF;
       --dbms_output.put_line('v_sm_cust_id is '||lt_sm_cust_id(i));
       --dbms_output.put_line('v_sm_Site_id is '||lt_sm_site_id(i));
       g_info_msz :='v_sm_cust_id is '||lt_sm_cust_id(i) ||'and v_sm_Site_id is'||lt_sm_site_id(i);
               P_WriteLog_File(file_name => g_file_name
                  ,info     => g_info_msz
                  ,o_return_status  => v_return_status);

   --get routing group
       lt_role_id(i)      := NULL;
       P_Get_RoutingGroup(   p_cust_id           => lt_sm_cust_id(i)
                           ,p_site_id           => lt_sm_site_id(i)
                           ,x_routing_role_id   => lt_role_id(i)
                           ,x_return_status     => v_return_status
                           );
      IF v_return_status <> 'S' THEN
           P_LogError_Table( i_alm_id => lt_alarm_id(i)
                           ,i_error_name => 'Routing Role not found, use default routing group Supervisor'
                           ,i_error      => 'Routing Role not found, use default routing group Supervisor'
                          );
      END IF;

    --update mss_site_last_communicate table info for failed communicates report(edwr)
    lt_site_status(i):='1';
    for FC_site in (select status_cd from cmn_site WHERE SITE_ID=lt_sm_site_id(i))
     loop
       lt_site_status(i):=FC_site.STATUS_CD;
     end loop;
     

    --statistical information--------------------------
         v_processed_alarms := v_processed_alarms + 1;
         v_max_alarm := lt_alarm_id(i);
         v_min_flag := 'N';


      --chandra Sep,2018 Salesforce config look to route alarms to SF
      lt_sf_cust_id(i) :=  NULL;
      lt_sf_site_id(i) :=  NULL;
      lt_sf_asset_id(i):= NULL;
      lt_sf_routing_group(i) :=  NULL;
      lt_sf_static_priority(i) := g_static_priority_no;
      lt_sf_alarm_flow(i) := 'SF';

      p_get_alarm_flow_MSS_SF( p_cust_id => lt_sm_cust_id(i)
   				,p_site_id => lt_sm_site_id(i)
          ,in_currentGMTTime => currentGMTTime
   				,o_sf_cust_id => lt_sf_cust_id(i)
   				,o_sf_site_id => lt_sf_site_id(i)
   				,o_sf_routing_group => lt_sf_routing_group(i)
   				,o_flow_flag => lt_sf_alarm_flow(i)
          ,o_sf_asset_id => lt_sf_asset_id(i)
          ,o_sf_site_status => lt_sf_site_status(i)
   			     );

       dbms_output.put_line( ' o_sf_cust_id ' || lt_sf_cust_id(i) );
       dbms_output.put_line( ' o_sf_site_id ' || lt_sf_site_id(i) );
       dbms_output.put_line( ' lt_sf_asset_id(i) ' || lt_sf_asset_id(i) );
       dbms_output.put_line( ' lt_sf_site_status(i) ' || lt_sf_site_status(i) );
       --
       dbms_output.put_line( ' Alarm id ' || lt_alarm_id(i) );
       dbms_output.put_line( ' Cust id ' || lt_sm_cust_id(i) );
       dbms_output.put_line( ' Site id ' || lt_sm_site_id(i) );
       dbms_output.put_line( ' Direction ' || lt_sf_alarm_flow(i) );

       dbms_output.put_line( ' Role id ' || lt_role_id(i) );
       dbms_output.put_line( ' Group flow ' || lt_sf_routing_group(i) );

       IF lt_sf_alarm_flow(i) IN ('Both','SF') THEN

         v_static_rg := NULL;
	 v_static_priority := NULL;

         p_get_alarm_static_priority_SF( psf_cust_id => lt_sf_cust_id(i)
          				,psf_site_id => lt_sf_site_id(i)
          				,p_alarm_desc => lt_descr(i)
          				,p_alarm_source => lt_source(i)
          				,o_priority_no => v_static_priority
          				,o_routing_group => v_static_rg
   					);

         dbms_output.put_line( ' static priority ' || v_static_priority );

	 IF (v_static_rg IS NOT NULL) AND (v_static_priority IS NOT NULL) THEN
	 	lt_sf_static_priority(i) := v_static_priority;
	 	lt_sf_routing_group(i) := v_static_rg;
	 END IF;

         dbms_output.put_line( ' Group static ' || lt_sf_routing_group(i) );
       END IF;

   END LOOP;



    --insert alarm into mss_norm_alarm table with bulk--------------------------
    FORALL i IN lt_alarm_id.FIRST..lt_alarm_id.LAST
    INSERT ALL
    when lt_sf_alarm_flow(i) in ('MSSR','Both') then
    INTO MSS_NORM_ALARM(
                                   alarm_id             ,
                                   site_name            ,
                                   receiver             ,
                                   descr                ,
                                   source               ,
                                   time_received        ,
                                   time_occurred        ,
                                   time_dialout         ,
                                   controller           ,
                                   sub_controller       ,
                                   alm_type             ,
                                   alm_state            ,
                                   alm_priority         ,
                                   caller_id            ,
                                   receiver_id          ,
                                   src_probable         ,
                                   desc_probable        ,
                                   receiver_host        ,
                                   file_id              ,
                                   field1               ,
                                   field2               ,
                                   field3               ,
                                   field4               ,
                                   field5               ,
                                   field6               ,
                                   field7               ,
                                   field8               ,
                                   field9               ,
                                   field10              ,
                                   field11              ,
                                   field12              ,
                                   field13              ,
                                   field14              ,
                                   field15              ,
                                   uard_site_name       ,
                                   uard_area_code       ,
                                   uard_phone           ,
                                   comments             ,
                                   alarm_count_24_hrs   ,
                                   alarm_count_7_days   ,
                                   create_date          ,
                                   controller_instance  ,
                                   request_id           ,
                                   processed_flag       ,
                                   attribute1           ,
                                   attribute2           ,
                                   attribute3           ,
                                   attribute4           ,
                                   attribute5           ,
                                   attribute6           ,
                                   attribute7           ,
                                   attribute8           ,
                                   attribute9           ,
                                   attribute10          ,
                                   id1                  ,
                                   id2                  ,
                                   id3                  ,
                                   id4                  ,
                                   id5                  ,
                                   last_updated_login   ,
                                   created_by           ,
                                   created_on           ,
                                   modified_by          ,
                                   modified_on          ,
                                   time_received_ch     ,
                                   time_occurred_ch     ,
                                   time_dialout_ch      ,
                                   program_app_id       ,
                                   version_number       ,
                                   email_address        ,
                                   email_processed      ,
                                   email_alert_id       ,
                                   rptalm_setup_id      ,
                                   norm_desc_id         ,
                                   norm_source_id       ,
                                   site_id              ,
                                   cust_id              ,
                                   sr_reference         ,
                                   routing_group_id     ,
                                   rtn_date             ,
                                   event_type           ,
                   		   time_available_process                 --Added By Mritunjay Sinha on 13 June2012 for CR# 21142
                                  )
                           VALUES
                                  (
                                    lt_alarm_id(i)             ,
                                    lt_site_name(i)            ,
                                    lt_receiver(i)             ,
                                    lt_descr(i)                ,
                                    lt_source(i)               ,
                                    lt_time_received(i)        ,
                                    lt_time_occurred(i)        ,
                                    lt_time_dialout(i)         ,
                                    lt_controller(i)           ,
                                    lt_sub_controller(i)       ,
                                    lt_alm_type(i)             ,
                                    lt_alm_state(i)            ,
                                    lt_alm_priority(i)         ,
                                    lt_caller_id(i)            ,
                                    lt_receiver_id(i)          ,
                                    lt_src_probable(i)         ,
                                    lt_desc_probable(i)        ,
                                    lt_receiver_host(i)        ,
                                    lt_file_id(i)              ,
                                    lt_field1(i)               ,
                                    lt_field2(i)               ,
                                    lt_field3(i)               ,
                                    lt_field4(i)               ,
                                    lt_field5(i)               ,
                                    lt_field6(i)               ,
                                    lt_field7(i)               ,
                                    lt_field8(i)               ,
                                    lt_field9(i)               ,
                                    lt_field10(i)              ,
                                    lt_field11(i)              ,
                                    lt_field12(i)              ,
                                    lt_field13(i)              ,
                                    lt_field14(i)              ,
                                    lt_field15(i)              ,
                                    lt_uard_site_name(i)       ,
                                    lt_uard_area_code(i)       ,
                                    lt_uard_phone(i)           ,
                                    lt_comments(i)             ,
                                    lt_alarm_count_24_hrs(i)   ,
                                    lt_alarm_count_7_days(i)   ,
                                    lt_create_date(i)          ,
                                    lt_controller_instance(i)  ,
                                    lt_request_id(i)           ,
                                    'N'                        ,
                                    lt_attribute1(i)           ,
                                    lt_attribute2(i)           ,
                                    lt_attribute3(i)           ,
                                    lt_attribute4(i)           ,
                                    lt_attribute5(i)           ,
                                    lt_attribute6(i)           ,
                                    lt_attribute7(i)           ,
                                    lt_attribute8(i)           ,
                                    lt_attribute9(i)           ,
                                    lt_attribute10(i)          ,
                                    lt_id1(i)                  ,
                                    lt_id2(i)                  ,
                                    lt_id3(i)                  ,
                                    lt_id4(i)                  ,
                                    lt_id5(i)                  ,
                                    '-1'                           ,
                                    g_created_by                   ,
                                    F_Get_Timezone(sysdate)         ,
                                    g_modified_by                  ,
                                    F_Get_Timezone(sysdate)         ,
                                    to_char(lt_time_received(i),'DD-Mon-YYYY HH24:MI:SS'),
                                    to_char(lt_time_occurred(i),'DD-Mon-YYYY HH24:MI:SS'),
                                    to_char(lt_time_dialout(i),'DD-Mon-YYYY HH24:MI:SS'),
                                    lt_program_app_id(i)       ,
                                    lt_version_number(i)       ,
                                    NULL                           ,
                                    NULL                           ,
                                    NULL                           ,
                                    NULL                           ,
                                    lt_norm_desc_id(i)         ,
                                    lt_norm_source_id(i)       ,
                                    lt_sm_site_id(i)                   ,
                                    lt_sm_cust_id(i)                   ,
                                    NULL                           ,
                                    lt_role_id(i)                      ,
                                    lt_rtn_date(i)             ,
                                    'Alarm'                        ,
                                    lt_time_received(i) )
             when lt_sf_alarm_flow(i) in ('SF','Both') then
		     INTO SF_NORM_ALARM(
		                                    alarm_id             ,
		                                    site_name            ,
		                                    receiver             ,
		                                    descr                ,
		                                    source               ,
		                                    time_received        ,
		                                    time_occurred        ,
		                                    time_dialout         ,
		                                    controller           ,
		                                    sub_controller       ,
		                                    alm_type             ,
		                                    alm_state            ,
		                                    alm_priority         ,
		                                    caller_id            ,
		                                    receiver_id          ,
		                                    src_probable         ,
		                                    desc_probable        ,
		                                    receiver_host        ,
		                                    file_id              ,
		                                    field1               ,
		                                    field2               ,
		                                    field3               ,
		                                    field4               ,
		                                    field5               ,
		                                    field6               ,
		                                    field7               ,
		                                    field8               ,
		                                    field9               ,
		                                    field10              ,
		                                    field11              ,
		                                    field12              ,
		                                    field13              ,
		                                    field14              ,
		                                    field15              ,
		                                    uard_site_name       ,
		                                    uard_area_code       ,
		                                    uard_phone           ,
		                                    comments             ,
		                                    alarm_count_24_hrs   ,
		                                    alarm_count_7_days   ,
		                                    create_date          ,
		                                    controller_instance  ,
		                                    request_id           ,
		                                    processed_flag       ,
		                                    attribute1           ,
		                                    attribute2           ,
		                                    attribute3           ,
		                                    attribute4           ,
		                                    attribute5           ,
		                                    attribute6           ,
		                                    attribute7           ,
		                                    attribute8           ,
		                                    attribute9           ,
		                                    attribute10          ,
		                                    id1                  ,
		                                    id2                  ,
		                                    id3                  ,
		                                    id4                  ,
		                                    id5                  ,
		                                    last_updated_login   ,
		                                    created_by           ,
		                                    created_on           ,
		                                    modified_by          ,
		                                    modified_on          ,
		                                    time_received_ch     ,
		                                    time_occurred_ch     ,
		                                    time_dialout_ch      ,
		                                    program_app_id       ,
		                                    version_number       ,
		                                    email_processed      ,
		                                    email_alert_id       ,
		                                    rptalm_setup_id      ,
		                                    norm_desc_id         ,
		                                    norm_source_id       ,
		                                    sf_site_id              ,
		                                    sf_cust_id              ,
		                                    sr_reference         ,
		                                    SF_ROUTING_GROUP     ,
		                                    rtn_date             ,
		                                    event_type           ,
		                    time_available_process                 --Added By Mritunjay Sinha on 13 June2012 for CR# 21142
		                    		   -- chandra sep,2018 alarm flow SF
		                    		   ,ALARM_PRIORITY_VALUE
		                                   )
		                            VALUES
		                                   (
		                                     lt_alarm_id(i)             ,
		                                     lt_site_name(i)            ,
		                                     lt_receiver(i)             ,
		                                     lt_descr(i)                ,
		                                     lt_source(i)               ,
		                                     lt_time_received(i)        ,
		                                     lt_time_occurred(i)        ,
		                                     lt_time_dialout(i)         ,
		                                     lt_controller(i)           ,
		                                     lt_sub_controller(i)       ,
		                                     lt_alm_type(i)             ,
		                                     lt_alm_state(i)            ,
		                                     lt_alm_priority(i)         ,
		                                     lt_caller_id(i)            ,
		                                     lt_receiver_id(i)          ,
		                                     lt_src_probable(i)         ,
		                                     lt_desc_probable(i)        ,
		                                     lt_receiver_host(i)        ,
		                                     lt_file_id(i)              ,
		                                     lt_field1(i)               ,
		                                     lt_field2(i)               ,
		                                     lt_field3(i)               ,
		                                     lt_field4(i)               ,
		                                     lt_field5(i)               ,
		                                     lt_field6(i)               ,
		                                     lt_field7(i)               ,
		                                     lt_field8(i)               ,
		                                     lt_field9(i)               ,
		                                     lt_field10(i)              ,
		                                     lt_field11(i)              ,
		                                     lt_field12(i)              ,
		                                     lt_field13(i)              ,
		                                     lt_field14(i)              ,
		                                     lt_field15(i)              ,
		                                     lt_uard_site_name(i)       ,
		                                     lt_uard_area_code(i)       ,
		                                     lt_uard_phone(i)           ,
		                                     lt_comments(i)             ,
		                                     lt_alarm_count_24_hrs(i)   ,
		                                     lt_alarm_count_7_days(i)   ,
		                                     lt_create_date(i)          ,
		                                     lt_controller_instance(i)  ,
		                                     lt_request_id(i)           ,
		                                     'N'                        ,
		                                     lt_attribute1(i)           ,
		                                     lt_attribute2(i)           ,
		                                     lt_attribute3(i)           ,
		                                     lt_attribute4(i)           ,
		                                     lt_attribute5(i)           ,
		                                     lt_attribute6(i)           ,
		                                     lt_attribute7(i)           ,
		                                     lt_attribute8(i)           ,
		                                     lt_attribute9(i)           ,
		                                     lt_attribute10(i)          ,
		                                     lt_id1(i)                  ,
		                                     lt_id2(i)                  ,
		                                     lt_id3(i)                  ,
		                                     lt_id4(i)                  ,
		                                     lt_id5(i)                  ,
		                                     '-1'                           ,
		                                     g_created_by                   ,
		                                     F_Get_Timezone(sysdate)         ,
		                                     g_modified_by                  ,
		                                     F_Get_Timezone(sysdate)         ,
		                                     to_char(lt_time_received(i),'DD-Mon-YYYY HH24:MI:SS'),
		                                     to_char(lt_time_occurred(i),'DD-Mon-YYYY HH24:MI:SS'),
		                                     to_char(lt_time_dialout(i),'DD-Mon-YYYY HH24:MI:SS'),
		                                     lt_program_app_id(i)       ,
		                                     lt_version_number(i)       ,
		                                     NULL                           ,
		                                     NULL                           ,
		                                     NULL                           ,
		                                     lt_norm_desc_id(i)         ,
		                                     lt_norm_source_id(i)       ,
		                                     lt_sf_site_id(i)                   ,
		                                     lt_sf_cust_id(i)                   ,
		                                     NULL                           ,
		                                     lt_sf_routing_group(i)                      ,
		                                     lt_rtn_date(i)             ,
		                                     'Alarm'                        ,
		                                     lt_time_received(i)
		                                     ,lt_sf_static_priority(i)
		                                     )
		                  select * from dual;


   FORALL j IN lt_alarm_id.FIRST..lt_alarm_id.LAST
   UPDATE mss_raw_alarm SET processed_flag = 'P' WHERE alarm_id=lt_alarm_id(j);

   --For MSSR reporting project
   /**
        FORALL k IN lt_alarm_id.FIRST..lt_alarm_id.LAST 
        MERGE
        INTO MSS_SITE_LAST_COMMUNICATION MSLC USING
                    (SELECT lt_sm_cust_id(k) AS CUST_ID,lt_sm_site_id(k) AS SITE_ID,
                    TO_DATE(To_Char(Sysdate,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') as TIME_RECEIVED,
                    lt_alarm_id(k) as ALARM_ID, lt_site_status(k) AS SITE_STATUS, lt_descr(k) as alarm_desc
                    FROM DUAL
                    where
                      lt_descr(k) not in ('Lost connectivity')
                    ) ALARM ON ( MSLC.SITE_ID=ALARM.SITE_ID )
                    WHEN MATCHED THEN
                    UPDATE
                    set MSLC.LAST_ALARM_ID   = ALARM.ALARM_ID,
                    MSLC.LAST_RECEIVED_DATE=ALARM.TIME_RECEIVED,
                    MSLC.SITE_STATUS=ALARM.SITE_STATUS
                    WHEN NOT MATCHED THEN
                    INSERT(CUST_ID,SITE_ID,LAST_ALARM_ID,LAST_RECEIVED_DATE,SITE_STATUS)
                    VALUES(ALARM.CUST_ID,ALARM.SITE_ID,ALARM.ALARM_ID,ALARM.TIME_RECEIVED,ALARM.SITE_STATUS);
                    **/

        currentGMTTime := F_Get_Timezone(sysdate);

        FOR kk IN lt_alarm_id.FIRST..lt_alarm_id.LAST LOOP
         IF lt_sf_alarm_flow(kk) in ('SF','Both') then
           MERGE INTO SF_SITE_LAST_COMMUNICATION MSLC USING
                    (SELECT lt_sf_site_id(kk) AS SF_SITE_ID,lt_sf_asset_id(kk) AS SF_ASSET_ID,
                    TO_DATE(To_Char(Sysdate,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') as TIME_RECEIVED,
                    currentGMTTime as TIME_RECEIVED_UTC,
                    lt_alarm_id(kk) as ALARM_ID, lt_sf_site_status(kk) AS SITE_STATUS, lt_descr(kk) as alarm_desc
                    FROM DUAL
                    where
                      lt_descr(kk) not in ('Lost connectivity')
                    ) ALARM ON ( MSLC.SF_SITE_ID=ALARM.SF_SITE_ID)
                    WHEN MATCHED THEN
                    UPDATE
                    set MSLC.LAST_MSS_ALARM_ID   = ALARM.ALARM_ID,
                    MSLC.LAST_RECEIVED_DATE=ALARM.TIME_RECEIVED,
                    MSLC.LAST_RECEIVED_DATE_UTC=ALARM.TIME_RECEIVED_UTC,
                    MSLC.SITE_STATUS=ALARM.SITE_STATUS,
                    MSLC.SF_ASSET_ID = SF_ASSET_ID,
                    MSLC.modified_on=currentGMTTime
                    WHEN NOT MATCHED THEN
                    INSERT(SF_SITE_ID,SF_ASSET_ID,LAST_MSS_ALARM_ID,LAST_RECEIVED_DATE,LAST_RECEIVED_DATE_UTC,SITE_STATUS,created_on,created_by,modified_on,modified_by)
                    VALUES(ALARM.SF_SITE_ID,ALARM.SF_ASSET_ID,ALARM.ALARM_ID,ALARM.TIME_RECEIVED,ALARM.TIME_RECEIVED_UTC,ALARM.SITE_STATUS,currentGMTTime,'admin',currentGMTTime,'admin');
         END IF;
        END LOOP;
    

   COMMIT;
  END LOOP;
     ----------------------------Business Logic End--------------------------------------------

     t2 := DBMS_UTILITY.get_time;
     g_info_msz := '----------------------------------------------------------------------------------'||
                         chr(13) || chr(10)||'Minimun ALARM ID Processed  '||v_min_alarm||
                         chr(13) || chr(10)||'Maximum ALARM ID Processed  '||v_max_alarm||
                         chr(13) || chr(10)||'Total Alarm processed  '||v_processed_alarms||
                         chr(13) || chr(10)||'Execution time  '||TO_CHAR((t2-t1)/100,'999.999')||
                         chr(13) || chr(10)||'Channeling Package Execution Completed @  '||to_char(sysdate,'MM/DD/YYYY HH24:MI:SS')||
                         chr(13) || chr(10)||'----------------------------------------------------------------------------------';
        dbms_output.put_line(g_info_msz);
        P_WriteLog_File(file_name => g_file_name
                       ,info     => g_info_msz
                       ,o_return_status  => v_return_status) ;
        P_CloseLog_File(g_file_name);
        retcode := 0;
 EXCEPTION
   WHEN OTHERS THEN
     ROLLBACK;
     dbms_output.put_line(SQLERRM);
         g_info_msz :=' Unhandled exception in XXCS_CHANNELING_PROC '||SUBSTR(SQLERRM,1,1600);
         dbms_output.put_line(g_info_msz);
         P_WriteLog_File(file_name => g_file_name
                  ,info     => g_info_msz
                  ,o_return_status  => v_return_status) ;
     retcode := 1;
     P_CloseLog_File(g_file_name);

       MSS_SEND_MAIL_PKG.send_error_to_mail
     (
          p_application_name => 'MSSR Channeling PL/SQL',
          p_error_message => ' Unhandled exception in XXCS_CHANNELING_PROC '||SUBSTR(SQLERRM,1,1600),
		  p_error_code=>'10002'
    );
   END MSS_CHANNELING_PROC;

END MSS_CHANNELING_PKG;