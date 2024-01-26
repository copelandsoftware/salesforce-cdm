DECLARE
    V_SQL VARCHAR2(100);
BEGIN
    FOR CUR_TYPE IN (SELECT TYPE_NAME FROM ALL_TYPES WHERE TYPE_NAME IN ('V_SF_BARCODE_WO_ARRAY')) LOOP
        V_SQL := 'DROP TYPE ' || CUR_TYPE.TYPE_NAME;
        EXECUTE IMMEDIATE V_SQL;
    END LOOP;
END;

/

CREATE OR REPLACE TYPE strsplit_type IS TABLE OF VARCHAR2 (4000);

/

create or replace
TYPE G_SF_BARCODE_WO_TYPE
AS
  OBJECT
  (
    ALARMID       VARCHAR2(18),
    SITEID        VARCHAR2(18),
	SITENAME      VARCHAR2(100),
	DESCRIPTION   VARCHAR2(255),
    SOURCE        VARCHAR2(255),
    FIELD5        VARCHAR2(525),
    FIELD9        VARCHAR2(200),
	FIELD11       VARCHAR2(255),
    SUBCAT        VARCHAR2(200),
    LOCATION      VARCHAR2(200),
	WOID          VARCHAR2(18)
  );
 
/

create or replace
TYPE V_SF_BARCODE_WO_ARRAY AS VARRAY(2000) OF G_SF_BARCODE_WO_TYPE;

/

CREATE OR REPLACE PACKAGE   SF_BARCODE_PKG   IS
/*
 For WorkOrderIntegration project call
*/
FUNCTION  get_barcode(p_barcode_wo_ls IN V_SF_BARCODE_WO_ARRAY)  return varchar2;
/*
 Format barcode CSV file and caculate barcode mapping table
*/
PROCEDURE  preproccess_barcode( errbuf OUT VARCHAR2,
                                  retcode OUT VARCHAR2
                                 );
FUNCTION  splitstr(p_string IN VARCHAR2, p_delimiter IN VARCHAR2) return strsplit_type PIPELINED;
PROCEDURE format_barcode_csv;
PROCEDURE caculate_barcode_mapping;
PROCEDURE TEST_FORMAT_VALUE(VAL IN VARCHAR2,TYPEOF IN VARCHAR2);
END SF_BARCODE_PKG;

/

create or replace PACKAGE BODY SF_BARCODE_PKG
  /*=========================================================================================
  ||  PROJECT NAME          : Barcode(MSSR)
  ||  APPLICATION NAME      : Barcode
  ||  SCRIPT NAME           : mss_barcode_pkg
  ||  CREATION INFORMATION
  ||       02/05/16         : Gary Sun
  ||
  ||  SCRIPT DESCRIPTION / USAGE
  ||     This Package is used for processing barcode
  ||  MODIFICATION HISTORY
  ||      Ver   DATE      Author             Description
  ||     ----   --------  -------------      -----------------------------------------
  ||   1.0    02/05/16    Gary Sun           Created
  ||   1.1    02/17/16    Randy Zhang        Caculate mapping table
  ||
  =========================================================================================== */
IS
  TYPE regularExp_tab IS TABLE OF mss_sys_config.sys_config_value%type INDEX BY BINARY_INTEGER;
  TYPE replaceStr_tab IS TABLE OF mss_sys_config.sys_config_value%type INDEX BY BINARY_INTEGER;
  TYPE rule_tab IS TABLE OF mss_sys_config.sys_config_value%type INDEX BY BINARY_INTEGER;
  
  TYPE stat_tab IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
  
  TYPE g_barcode_tab IS TABLE OF MSS_BARCODE_MAPPING.BARCODE%type INDEX BY BINARY_INTEGER;
  TYPE g_barcode_site_id_tab IS TABLE OF MSS_BARCODE_MAPPING.SITE_ID%type INDEX BY BINARY_INTEGER;
  TYPE g_barcode_store_no_tab IS TABLE OF MSS_BARCODE_MAPPING.STORE_NO%type INDEX BY BINARY_INTEGER;
  TYPE g_barcode_department_tab IS TABLE OF MSS_BARCODE_MAPPING.DEPARTMENT%type INDEX BY BINARY_INTEGER;
  TYPE g_barcode_asset_category_tab IS TABLE OF MSS_BARCODE_MAPPING.ASSET_CATEGORY%type INDEX BY BINARY_INTEGER;
  TYPE g_barcode_asset_subcat_tab IS TABLE OF MSS_BARCODE_MAPPING.ASSET_SUBCATEGORY%type INDEX BY BINARY_INTEGER;
  TYPE g_barcode_attribute1_tab IS TABLE OF MSS_BARCODE_MAPPING.ATTRIBUTE1%type INDEX BY BINARY_INTEGER;
  TYPE g_barcode_converted_value1_tab IS TABLE OF MSS_BARCODE_MAPPING.CONVERTED_VALUE1%type INDEX BY BINARY_INTEGER;
  TYPE g_barcode_attribute2_tab IS TABLE OF MSS_BARCODE_MAPPING.ATTRIBUTE2%type INDEX BY BINARY_INTEGER;
  TYPE g_barcode_convert_value2_tab IS TABLE OF MSS_BARCODE_MAPPING.CONVERTED_VALUE2%type INDEX BY BINARY_INTEGER;
  TYPE g_regexp_replace_tab IS TABLE OF MSS_BARCODE_REPLACE_REF.REPLACE_OLD%type INDEX BY BINARY_INTEGER;
  TYPE g_regexp_replace_tab_tab IS TABLE OF g_regexp_replace_tab INDEX BY BINARY_INTEGER;
  
  g_barcode_ls g_barcode_tab;
  g_barcode_site_id_ls g_barcode_site_id_tab;
  g_barcode_store_no_ls g_barcode_store_no_tab;
  g_barcode_department_ls g_barcode_department_tab;
  g_barcode_asset_category_ls g_barcode_asset_category_tab;
  g_barcode_asset_subcategory_ls g_barcode_asset_subcat_tab;
  g_barcode_attribute1_ls g_barcode_attribute1_tab;
  g_barcode_convert_value1_ls g_barcode_converted_value1_tab;
  g_barcode_attribute2_ls g_barcode_attribute2_tab;
  g_barcode_convert_value2_ls g_barcode_convert_value2_tab;
  
  g_barcode_source_ls g_barcode_converted_value1_tab;
  g_barcode_case_ls g_barcode_converted_value1_tab;
  
  g_regexp_value1_ls regularExp_tab;
  g_replace_old_value1_ls g_regexp_replace_tab_tab;
  g_replace_new_value1_ls g_regexp_replace_tab_tab;
  
  g_regexp_value2_ls regularExp_tab;
  g_replace_old_value2_ls g_regexp_replace_tab_tab;
  g_replace_new_value2_ls g_regexp_replace_tab_tab;
  
  g_regexp_source_ls regularExp_tab;
  g_replace_old_source_ls g_regexp_replace_tab_tab;
  g_replace_new_source_ls g_regexp_replace_tab_tab;
  
  g_regexp_field5_ls regularExp_tab;
  g_replace_old_field5_ls g_regexp_replace_tab_tab;
  g_replace_new_field5_ls g_regexp_replace_tab_tab;
  
  g_regexp_field9_ls regularExp_tab;
  g_replace_old_field9_ls g_regexp_replace_tab_tab;
  g_replace_new_field9_ls g_regexp_replace_tab_tab;
  
  g_regexp_rack_ls regularExp_tab;
  g_replace_old_rack_ls g_regexp_replace_tab_tab;
  g_replace_new_rack_ls g_regexp_replace_tab_tab;
  
  g_common_replace_old_ls g_regexp_replace_tab;
  g_common_replace_new_ls g_regexp_replace_tab;
  
  g_limit_count        NUMBER          := 10000;
  
  g_barcode_match_regexp_stat stat_tab;
  g_source_match_regexp_stat stat_tab;
  g_field5_match_regexp_stat stat_tab;
  
  g_barcode_other_stat NUMBER(10);
  
  g_directory                  VARCHAR2(130) := 'SFBARCODEPROC';
  g_info_msz                   VARCHAR2 (30000); 
  g_file_name                  UTL_FILE.FILE_TYPE;
  
  g_replace_spliter VARCHAR(2) :='|';
  g_replace_regexp_idf_s VARCHAR(10) :='@RegExp/';
  g_replace_regexp_idf_e VARCHAR(10) :='/RegExp@';

  -----------
  --Write log
  -----------
  PROCEDURE P_Log_File (directory_name IN VARCHAR2,file_name OUT UTL_FILE.FILE_TYPE, o_return_status OUT VARCHAR2) AS
  P_Log_File UTL_FILE.FILE_TYPE;
  BEGIN 
    P_Log_File := UTL_FILE.FOPEN(directory_name,'SF_BARCODE'||'.log','a');
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
  -----------
  --Write log
  -----------
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
  -----------
  --Write log
  -----------
  PROCEDURE P_CloseLog_File (file_name IN UTL_FILE.FILE_TYPE) AS
  BEGIN 
   IF utl_file.is_open(file_name) THEN
      utl_file.fclose_all;
   END IF;
  END P_CloseLog_File;
  
  ------------------------------
  --Cache all regular expression
  ------------------------------
  PROCEDURE Cache_RegularExp
  IS
  v_value1_cnt NUMBER(5) :=0;
  v_value2_cnt NUMBER(5) :=0;
  v_source_cnt NUMBER(5) :=0;
  v_field5_cnt NUMBER(5) :=0;
  v_field9_cnt NUMBER(5) :=0;
  v_rack_cnt NUMBER(5) :=0;
  
  v_common_cnt NUMBER(5):=0;
 
  v_regexp_rep_old_ls   g_regexp_replace_tab;
  v_regexp_rep_new_ls   g_regexp_replace_tab;
  
  PROCEDURE P_GET_REGEXP_REPLACE(i_regexp_id IN NUMBER,
                                 o_regexp_rep_old_ls OUT g_regexp_replace_tab,
                                 o_regexp_rep_new_ls OUT g_regexp_replace_tab
            )
  IS
  v_cnt NUMBER;
  BEGIN
  v_cnt:=0;
  o_regexp_rep_old_ls.delete;
  o_regexp_rep_new_ls.delete;
  FOR REPLACE_TAB IN(
    SELECT REPLACE_OLD,REPLACE_NEW
    FROM MSS_BARCODE_REPLACE_REF
    WHERE REGEXP_ID=i_regexp_id
    ORDER BY ORDER_NO ASC
  )LOOP
  v_cnt:=v_cnt+1;
  o_regexp_rep_old_ls(v_cnt):=REPLACE_TAB.REPLACE_OLD;
  o_regexp_rep_new_ls(v_cnt):=REPLACE_TAB.REPLACE_NEW;
  END LOOP;
  END P_GET_REGEXP_REPLACE;

  BEGIN
  FOR REGEXP_TAB IN(
   SELECT TYPE,REGEXP,RULE,ID
       FROM MSS_BARCODE_REGEXP_REF ORDER BY ORDER_NO ASC
  )LOOP
  IF REGEXP_TAB.TYPE = 'common' THEN
  v_common_cnt:=v_common_cnt+1;  
  P_GET_REGEXP_REPLACE(
      i_regexp_id=>REGEXP_TAB.ID,
      o_regexp_rep_old_ls=>g_common_replace_old_ls,
      o_regexp_rep_new_ls=>g_common_replace_new_ls
      );
  END IF;
  IF REGEXP_TAB.TYPE = 'value1' THEN
  v_value1_cnt:=v_value1_cnt+1;
  g_regexp_value1_ls(v_value1_cnt):=REGEXP_TAB.REGEXP;
  P_GET_REGEXP_REPLACE(
      i_regexp_id=>REGEXP_TAB.ID,
      o_regexp_rep_old_ls=>v_regexp_rep_old_ls,
      o_regexp_rep_new_ls=>v_regexp_rep_new_ls
      );
  g_replace_old_value1_ls(v_value1_cnt):=v_regexp_rep_old_ls;
  g_replace_new_value1_ls(v_value1_cnt):=v_regexp_rep_new_ls;
  END IF;
  IF REGEXP_TAB.TYPE = 'value2' THEN 
  v_value2_cnt:=v_value2_cnt+1;
  g_regexp_value2_ls(v_value2_cnt):=REGEXP_TAB.REGEXP;
  P_GET_REGEXP_REPLACE(
      i_regexp_id=>REGEXP_TAB.ID,
      o_regexp_rep_old_ls=>v_regexp_rep_old_ls,
      o_regexp_rep_new_ls=>v_regexp_rep_new_ls
      );
  g_replace_old_value2_ls(v_value2_cnt):=v_regexp_rep_old_ls;
  g_replace_new_value2_ls(v_value2_cnt):=v_regexp_rep_new_ls;
  END IF;
  IF REGEXP_TAB.TYPE = 'source' THEN 
  v_source_cnt:=v_source_cnt+1;
  g_regexp_source_ls(v_source_cnt):=REGEXP_TAB.REGEXP;
  P_GET_REGEXP_REPLACE(
      i_regexp_id=>REGEXP_TAB.ID,
      o_regexp_rep_old_ls=>v_regexp_rep_old_ls,
      o_regexp_rep_new_ls=>v_regexp_rep_new_ls
      );
  g_replace_old_source_ls(v_source_cnt):=v_regexp_rep_old_ls;
  g_replace_new_source_ls(v_source_cnt):=v_regexp_rep_new_ls;
  END IF;
  IF REGEXP_TAB.TYPE = 'field5' THEN 
  v_field5_cnt:=v_field5_cnt+1;
  g_regexp_field5_ls(v_field5_cnt):=REGEXP_TAB.REGEXP;
  P_GET_REGEXP_REPLACE(
      i_regexp_id=>REGEXP_TAB.ID,
      o_regexp_rep_old_ls=>v_regexp_rep_old_ls,
      o_regexp_rep_new_ls=>v_regexp_rep_new_ls
      );
  g_replace_old_field5_ls(v_field5_cnt):=v_regexp_rep_old_ls;
  g_replace_new_field5_ls(v_field5_cnt):=v_regexp_rep_new_ls;
  END IF;
  IF REGEXP_TAB.TYPE = 'field9' THEN 
  v_field9_cnt:=v_field9_cnt+1;
  g_regexp_field9_ls(v_field9_cnt):=REGEXP_TAB.REGEXP;
  P_GET_REGEXP_REPLACE(
      i_regexp_id=>REGEXP_TAB.ID,
      o_regexp_rep_old_ls=>v_regexp_rep_old_ls,
      o_regexp_rep_new_ls=>v_regexp_rep_new_ls
      );
  g_replace_old_field9_ls(v_field9_cnt):=v_regexp_rep_old_ls;
  g_replace_new_field9_ls(v_field9_cnt):=v_regexp_rep_new_ls;
  END IF;
  IF REGEXP_TAB.TYPE = 'source-rack' THEN 
  v_rack_cnt:=v_rack_cnt+1;
  g_regexp_rack_ls(v_rack_cnt):=REGEXP_TAB.REGEXP;
  P_GET_REGEXP_REPLACE(
      i_regexp_id=>REGEXP_TAB.ID,
      o_regexp_rep_old_ls=>v_regexp_rep_old_ls,
      o_regexp_rep_new_ls=>v_regexp_rep_new_ls
      );
  g_replace_old_rack_ls(v_rack_cnt):=v_regexp_rep_old_ls;
  g_replace_new_rack_ls(v_rack_cnt):=v_regexp_rep_new_ls;
  END IF;
  END LOOP; 
  
  --test
  /*
  for m in g_regexp_value1_ls.first .. g_regexp_value1_ls.last loop
  dbms_output.put_line('regexp:'||g_regexp_value1_ls(m));
  for n in g_replace_old_value1_ls(m).first .. g_replace_old_value1_ls(m).last loop
  dbms_output.put_line('rep_old:'||g_replace_old_value1_ls(m)(n));
  dbms_output.put_line('rep_new:'||g_replace_new_value1_ls(m)(n));
  end loop;
    dbms_output.put_line('------------------------------------------');
  end loop;
  */
  
  IF g_regexp_value1_ls.COUNT>0 THEN
  FOR i in g_regexp_value1_ls.FIRST .. g_regexp_value1_ls.LAST LOOP 
   g_barcode_match_regexp_stat(i):=0;
  END LOOP; 
  END IF;
  
  IF g_regexp_source_ls.COUNT>0 THEN
  FOR j in g_regexp_source_ls.FIRST .. g_regexp_source_ls.LAST LOOP 
   g_source_match_regexp_stat(j):=0;
  END LOOP;
  END IF;
  
  IF g_regexp_field5_ls.COUNT>0 THEN
  FOR k in g_regexp_field5_ls.FIRST .. g_regexp_field5_ls.LAST LOOP 
   g_field5_match_regexp_stat(k):=0;
  END LOOP;
  END IF;
  
  END Cache_RegularExp;
  
  --------------------------------------------------------------------------------
  --The common procedure to format value1, value2, source, field5, field9 and so on
  --------------------------------------------------------------------------------

  PROCEDURE P_Format_value(i_value IN VARCHAR2,
                           i_regularExp_ls IN regularExp_tab,
                           i_replace_old_ls IN g_regexp_replace_tab_tab,
                           i_replace_new_ls IN g_regexp_replace_tab_tab,
                           i_check_regexp IN VARCHAR2,
                           o_match_flag OUT VARCHAR2,
                           o_match_number OUT NUMBER,
                           o_convert_value OUT VARCHAR2
                           )
  IS
   v_return_status VARCHAR2(1);
   v_regexp_rep_old_ls g_regexp_replace_tab;
   v_regexp_rep_new_ls g_regexp_replace_tab;
   v_rep_old VARCHAR2(240);
   v_rep_new VARCHAR2(240);
   
   
   PROCEDURE P_HANDLE_SPECIAL_VAL(
                        i_rep_value IN VARCHAR2,
                        o_rep_value OUT VARCHAR2
   )
   IS
   v_rep_start_pos NUMBER(3);
   v_rep_end_pos NUMBER(3);
   v_rep_special_regexp VARCHAR2(100);
   v_rep_special_str VARCHAR2(100);
   BEGIN
      v_rep_start_pos:=INSTR(i_rep_value,g_replace_regexp_idf_s)+LENGTH(g_replace_regexp_idf_s);
      v_rep_end_pos:=INSTR(i_rep_value,g_replace_regexp_idf_e);
      v_rep_special_regexp:=SUBSTR(i_rep_value,v_rep_start_pos,(v_rep_end_pos-v_rep_start_pos));
      v_rep_special_str:=regexp_substr(i_value,v_rep_special_regexp);
      o_rep_value:=SUBSTR(i_rep_value,1,v_rep_start_pos-1-length(g_replace_regexp_idf_s))
                 ||v_rep_special_str
                 ||SUBSTR(i_rep_value,v_rep_end_pos+LENGTH(g_replace_regexp_idf_e));
   END P_HANDLE_SPECIAL_VAL;
  BEGIN
  o_convert_value:=trim(i_value);
  IF i_check_regexp = 'Y' THEN 
  FOR j in 1 .. i_regularExp_ls.COUNT LOOP

  --dbms_output.put_line(lt_value1(i)||'  '||g_regexp_value1_ls(j));
  o_match_flag:=NULL;
  IF regexp_like(o_convert_value,i_regularExp_ls(j),'im') THEN
   --dbms_output.put_line('Match Regular expression : '||i_regularExp_ls(j));
   o_match_number:=j;
   o_match_flag:='Y';
   IF i_replace_old_ls(j).COUNT>0 THEN
   v_regexp_rep_old_ls:=i_replace_old_ls(j);
   v_regexp_rep_new_ls:=i_replace_new_ls(j);
   FOR i IN v_regexp_rep_old_ls.FIRST .. v_regexp_rep_old_ls.LAST
   LOOP
   v_rep_old:=v_regexp_rep_old_ls(i);
   v_rep_new:=v_regexp_rep_new_ls(i);
   IF regexp_like(v_rep_new,g_replace_regexp_idf_s,'im') THEN
      P_HANDLE_SPECIAL_VAL(
                      i_rep_value=>v_rep_new,
                      o_rep_value=>v_rep_new
      );
   END IF;
   --dbms_output.put_line('Replace From '||v_rep_old);
   --dbms_output.put_line('Replace To '||v_rep_new);
   o_convert_value:=REGEXP_REPLACE(o_convert_value,v_rep_old,v_rep_new);
   --dbms_output.put_line('Converted To '||o_convert_value);
   --dbms_output.put_line('**********');
   --dbms_output.put_line('RULE='||i_regexp_no||' rawvalue1='||i_value1||' v_replace_from='||v_replace_from||'  v_replace_to='||v_replace_to||'  v_format_value1='||v_format_value1);
   END LOOP;
   END IF;
   EXIT;
   END IF;
   END LOOP;
   END IF;
   
   --common replace
   IF g_common_replace_old_ls.COUNT >0 THEN
   FOR i IN g_common_replace_old_ls.FIRST .. g_common_replace_old_ls.LAST
   LOOP
      v_rep_old:=g_common_replace_old_ls(i);
      v_rep_new:=g_common_replace_new_ls(i);
      IF regexp_like(v_rep_new,g_replace_regexp_idf_s,'im') THEN
      P_HANDLE_SPECIAL_VAL(
                      i_rep_value=>v_rep_new,
                      o_rep_value=>v_rep_new
      );
      END IF;
      o_convert_value:=REGEXP_REPLACE(o_convert_value
                                      ,v_rep_old
                                      ,v_rep_new);
   END LOOP;
   END IF;
   
  END P_Format_value;
  
  
  ---------------------------------------
  --For WorkOrderIntegration project call
  ---------------------------------------
  FUNCTION get_barcode(
      p_barcode_wo_ls IN V_SF_BARCODE_WO_ARRAY
      )
    RETURN VARCHAR2
  IS

    TYPE convert_value2_tab IS TABLE OF MSS_BARCODE_MAPPING.CONVERTED_VALUE2%type INDEX BY BINARY_INTEGER;
    TYPE barcode_tab IS TABLE OF MSS_BARCODE_MAPPING.BARCODE%type INDEX BY BINARY_INTEGER;
	TYPE id_tab IS TABLE OF MSS_BARCODE_MAPPING.ID%type INDEX BY BINARY_INTEGER;
    TYPE cat_tab IS TABLE OF MSS_BARCODE_MAPPING.ASSET_CATEGORY%type INDEX BY BINARY_INTEGER;
    TYPE subcat_tab IS TABLE OF MSS_BARCODE_MAPPING.ASSET_SUBCATEGORY%type INDEX BY BINARY_INTEGER;
    TYPE department_tab IS TABLE OF MSS_BARCODE_MAPPING.DEPARTMENT%type INDEX BY BINARY_INTEGER;

    lt_value2 convert_value2_tab;
    lt_barcode barcode_tab;
	lt_id id_tab;
    lt_cat cat_tab;
    lt_subcat subcat_tab;
    lt_department department_tab;
    
    V_BARCODE VARCHAR2(100);
	V_ID NUMBER(30);
    V_MATCH_NUMBER NUMBER(2);
    V_NORM_SOURCE VARCHAR2(255);
    V_NORM_FIELD5 VARCHAR2(525);
    V_NORM_FIELD9 VARCHAR2(100);
  
    v_casulate_flag VARCHAR2(2);
    v_found VARCHAR2(2);
 
    v_source_match_flag VARCHAR2(2);
    v_field5_match_flag VARCHAR2(2);
    v_field9_match_flag VARCHAR2(2);
    
    v_source_match_number NUMBER(5);
    v_field5_match_number NUMBER(5);
    v_field9_match_number NUMBER(5);
    
    v_return_status VARCHAR2(1);
    v_rtn_barcodes VARCHAR2(30000);
    
    v_match2_number NUMBER(5);
    v_rack_cnt NUMBER(5);
	v_alarm_exist_cnt NUMBER(2);
    
    
    t1 INTEGER;
    t2 INTEGER;
       
  BEGIN
    t1 := DBMS_UTILITY.get_time;
    P_Log_File (directory_name   => g_directory
             ,file_name        => g_file_name   
             ,o_return_status  => v_return_status);
    g_info_msz := '----------------------------------------------------------------------------------'||
                      chr(13) || chr(10)||'Get Barcode Execution Started @ '||to_char(sysdate,'MM/DD/YYYY HH24:MI:SS')||
                      chr(13) || chr(10)||'----------------------------------------------------------------------------------';
    P_WriteLog_File(file_name => g_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
    
    v_rtn_barcodes:='';
    FOR i IN p_barcode_wo_ls.FIRST .. p_barcode_wo_ls.LAST
    LOOP
     v_found:='N';
     V_MATCH_NUMBER:=0;
     v_casulate_flag:='N';
     V_BARCODE:='N';
	g_info_msz:=chr(13) || chr(10)||'******************************************************'||chr(13) || chr(10);
	g_info_msz:=g_info_msz||'Parameters From WorkOrderIntegration project:'||chr(13) || chr(10);
    g_info_msz:=g_info_msz||chr(13) || chr(10)||' Alarm id='||p_barcode_wo_ls(i).ALARMID||chr(13) || chr(10)
	                                            ||' Site Id=' ||p_barcode_wo_ls(i).SITEID||chr(13) || chr(10)
												||' Site Name='||p_barcode_wo_ls(i).SITENAME||chr(13) || chr(10)
												||' DESCRIPTION='||p_barcode_wo_ls(i).DESCRIPTION||chr(13) || chr(10)
												||' SOURCE='||p_barcode_wo_ls(i).SOURCE||chr(13) || chr(10)
												||' Field5='||p_barcode_wo_ls(i).FIELD5||chr(13) || chr(10)
												||' Field9='||p_barcode_wo_ls(i).FIELD9||chr(13) || chr(10)
												||' Field11='||p_barcode_wo_ls(i).FIELD11||chr(13) || chr(10)
												||' Sub category='||p_barcode_wo_ls(i).SUBCAT||chr(13) || chr(10)
												||' Location='||p_barcode_wo_ls(i).LOCATION||chr(13) || chr(10)
												||' Work Order Id='||p_barcode_wo_ls(i).WOID||chr(13) || chr(10);
	g_info_msz:=g_info_msz||chr(13) || chr(10)||'Execute algorithm:'||chr(13) || chr(10);		
    P_WriteLog_File(file_name => g_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
    --get barcode from barcode mapping table
    FOR BARCODE_MAPPING_TABLE IN (
      SELECT ID,BARCODE,CONVERTED_VALUE2,ASSET_CATEGORY,ASSET_SUBCATEGORY,DEPARTMENT
      FROM MSS_BARCODE_MAPPING
      WHERE SITE_ID=p_barcode_wo_ls(i).SITEID
      AND NVL(MSS_SOURCE,'ZZZYYYXXX')=trim(NVL(p_barcode_wo_ls(i).SOURCE,'ZZZYYYXXX'))
      AND NVL(MSS_FIELD5,'XXXYYYZ')=trim(NVL(p_barcode_wo_ls(i).FIELD5,'XXXYYYZ'))
	  AND NVL(MSS_FIELD9,'XXXYYYZ')=trim(NVL(p_barcode_wo_ls(i).FIELD9,'XXXYYYZ'))
	  AND ASSET_SUBCATEGORY=trim(p_barcode_wo_ls(i).SUBCAT)
	  AND DEPARTMENT=trim(p_barcode_wo_ls(i).LOCATION)
    )LOOP
    IF BARCODE_MAPPING_TABLE.BARCODE IS NOT NULL THEN
      V_MATCH_NUMBER:=V_MATCH_NUMBER+1;
      lt_barcode(V_MATCH_NUMBER):=BARCODE_MAPPING_TABLE.BARCODE;
	  lt_id(V_MATCH_NUMBER):=BARCODE_MAPPING_TABLE.ID;
      lt_value2(V_MATCH_NUMBER):=BARCODE_MAPPING_TABLE.CONVERTED_VALUE2;
      lt_cat(V_MATCH_NUMBER):=BARCODE_MAPPING_TABLE.ASSET_CATEGORY;
      lt_subcat(V_MATCH_NUMBER):=BARCODE_MAPPING_TABLE.ASSET_SUBCATEGORY;
      lt_department(V_MATCH_NUMBER):=BARCODE_MAPPING_TABLE.DEPARTMENT;
    END IF;
    END LOOP;
    
    IF lt_barcode.COUNT=1 THEN
      v_found:='Y';
      g_info_msz:='Get Barcode from Mapping table.';
      P_WriteLog_File(file_name => g_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status);
      V_BARCODE:=lt_barcode(1);
	  V_ID:=lt_id(1);
    END IF;
    --If not find barcode from mapping table,then caculate from format csv table
    IF v_found = 'N' AND lt_barcode.COUNT = 0 THEN 
      v_casulate_flag:='Y';
	  --cache regular expression
      Cache_RegularExp;
      --nomilized source
      P_Format_value(
               i_value=>p_barcode_wo_ls(i).SOURCE,
               i_regularExp_ls=>g_regexp_source_ls,
               i_replace_old_ls=>g_replace_old_source_ls,
               i_replace_new_ls=>g_replace_new_source_ls,
               i_check_regexp=>'Y',
               o_match_flag=>v_source_match_flag,
               o_match_number=>v_source_match_number,
               o_convert_value=>V_NORM_SOURCE);
      g_info_msz:='converted source:'||V_NORM_SOURCE;
      P_WriteLog_File(file_name => g_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
      --nomilized field5
      P_Format_value(
               i_value=>p_barcode_wo_ls(i).FIELD5,
               i_regularExp_ls=>g_regexp_field5_ls,
               i_replace_old_ls=>g_replace_old_field5_ls,
               i_replace_new_ls=>g_replace_new_field5_ls,
               i_check_regexp=>'Y',
               o_match_flag=>v_field5_match_flag,
               o_match_number=>v_field5_match_number,
               o_convert_value=>V_NORM_FIELD5);
      g_info_msz:='converted field5:'||V_NORM_FIELD5;     
      P_WriteLog_File(file_name => g_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
      FOR BARCODE_MAPPING_TABLE IN(
        SELECT ID,BARCODE,CONVERTED_VALUE2,ASSET_CATEGORY,ASSET_SUBCATEGORY,DEPARTMENT
        FROM MSS_BARCODE_MAPPING
        WHERE CONVERTED_VALUE1=upper(V_NORM_SOURCE)||upper(V_NORM_FIELD5)
		      AND SITE_ID=p_barcode_wo_ls(i).SITEID
      )LOOP
        IF BARCODE_MAPPING_TABLE.BARCODE IS NOT NULL THEN
          V_MATCH_NUMBER:=V_MATCH_NUMBER+1;
          lt_barcode(V_MATCH_NUMBER):=BARCODE_MAPPING_TABLE.BARCODE;
		  lt_id(V_MATCH_NUMBER):=BARCODE_MAPPING_TABLE.ID;
          lt_value2(V_MATCH_NUMBER):=BARCODE_MAPPING_TABLE.CONVERTED_VALUE2;
          lt_cat(V_MATCH_NUMBER):=BARCODE_MAPPING_TABLE.ASSET_CATEGORY;
          lt_subcat(V_MATCH_NUMBER):=BARCODE_MAPPING_TABLE.ASSET_SUBCATEGORY;
          lt_department(V_MATCH_NUMBER):=BARCODE_MAPPING_TABLE.DEPARTMENT;
        END IF;
      END LOOP; 
    END IF;
    
    IF v_casulate_flag='Y' AND lt_barcode.COUNT=1 THEN
       v_found:='Y';
       g_info_msz:='Caculate Barcode with algorithm.';
       P_WriteLog_File(file_name => g_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
       V_BARCODE:=lt_barcode(1);
	   V_ID:=lt_id(1);
    END IF;
    
    --If match barcode more than 2, then       
    IF lt_barcode.COUNT>=2 THEN
	   g_info_msz:='Find more than 2 barcode';
     P_WriteLog_File(file_name => g_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
       v_match2_number:=0;
       V_MATCH_NUMBER:=0;
       v_rack_cnt:=0;
       --nomilized field9        
       P_Format_value(
       i_value=>p_barcode_wo_ls(i).FIELD9,
       i_regularExp_ls=>g_regexp_field9_ls,
       i_replace_old_ls=>g_replace_old_field9_ls,
       i_replace_new_ls=>g_replace_new_field9_ls,
       i_check_regexp=>'Y',
       o_match_flag=>v_field9_match_flag,
       o_match_number=>v_field9_match_number,
       o_convert_value=>V_NORM_FIELD9);
       g_info_msz:='converted field9:'||V_NORM_FIELD9;   
       P_WriteLog_File(file_name => g_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
       --compare v2 and f9
       FOR j IN lt_barcode.FIRST .. lt_barcode.LAST LOOP
          select count(*) into v_rack_cnt from (
                      select column_value as f9
                      from table(MSS_BARCODE_PKG.splitstr(V_NORM_FIELD9,g_replace_spliter))) f9_tab,
                      (select column_value as v2
                      from table(MSS_BARCODE_PKG.splitstr(lt_value2(j),g_replace_spliter))) v2_tab
                      where f9_tab.f9=v2_tab.v2;
          IF v_rack_cnt >0 THEN
             V_MATCH_NUMBER:=V_MATCH_NUMBER+1;
             v_match2_number:=j;
          END IF;     
       END LOOP;

       IF V_MATCH_NUMBER = 1 THEN
          v_found := 'Y';
          g_info_msz:='Compare field9 with value2 to find the barcode.';
          P_WriteLog_File(file_name => g_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
          V_BARCODE:=lt_barcode(v_match2_number);
		  V_ID:=lt_id(v_match2_number);
       END IF;
       
       --compare the first part of source with value2
       IF v_found = 'N' THEN 
          v_match2_number:=0;
          V_MATCH_NUMBER:=0;
              P_Format_value(
           i_value=>p_barcode_wo_ls(i).SOURCE,
           i_regularExp_ls=>g_regexp_rack_ls,
           i_replace_old_ls=>g_replace_old_rack_ls,
           i_replace_new_ls=>g_replace_new_rack_ls,
           i_check_regexp=>'Y',
           o_match_flag=>v_field9_match_flag,
           o_match_number=>v_field9_match_number,
           o_convert_value=>V_NORM_FIELD9);
		   g_info_msz:='converted source-rack:'||V_NORM_FIELD9;   
       P_WriteLog_File(file_name => g_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
           FOR j IN lt_barcode.FIRST .. lt_barcode.LAST LOOP
              IF  lt_value2(j) LIKE '%'||V_NORM_FIELD9||'%' THEN 
                 V_MATCH_NUMBER:=V_MATCH_NUMBER+1;
                 v_match2_number:=j;
              END IF;
           END LOOP;
       END IF;
       
       IF V_MATCH_NUMBER = 1 THEN
          v_found := 'Y';
          g_info_msz:='Compare field9 with pre-source to find the barcode.';
          P_WriteLog_File(file_name => g_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
          V_BARCODE:=lt_barcode(v_match2_number);
		  V_ID:=lt_id(v_match2_number);
       END IF;
       
       --compare sub category and department
       IF v_found ='N' THEN
          v_match2_number:=0;
          V_MATCH_NUMBER:=0;
          FOR j IN lt_barcode.FIRST .. lt_barcode.LAST LOOP
            IF p_barcode_wo_ls(i).SUBCAT = lt_subcat(j) AND p_barcode_wo_ls(i).LOCATION = lt_department(j) THEN
               V_MATCH_NUMBER:=V_MATCH_NUMBER+1;
               v_match2_number:=j;
            END IF;
          END LOOP;
       END IF;   
       
       IF V_MATCH_NUMBER = 1 THEN
          v_found := 'Y';
          g_info_msz:='Compare subcategory to find the barcode.';
          P_WriteLog_File(file_name => g_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
          V_BARCODE:=lt_barcode(v_match2_number);
		  V_ID:=lt_id(v_match2_number);
       END IF;
       
    END IF;
	
	
	     
    IF v_found = 'Y' THEN
      --UPDATE Mss_Ext_Work_Order_Intf SET BARCODE=V_BARCODE 
      --WHERE MSS_WORK_ORDER_ID=p_barcode_wo_ls(i).WOID;
      UPDATE sf_work_order set SERIAL_NUMBER=V_BARCODE
      WHERE SF_WORK_ORDER_ID=p_barcode_wo_ls(i).WOID;
    END IF;      
      --if it is a caculated barcode, store mapping and raw alarm table.
      IF v_casulate_flag='Y' AND v_found = 'Y' THEN
       --update mapping table
       UPDATE MSS_BARCODE_MAPPING 
             SET MSS_SOURCE=p_barcode_wo_ls(i).SOURCE, 
                 MSS_FIELD5=p_barcode_wo_ls(i).FIELD5,
                 MSS_FIELD9=p_barcode_wo_ls(i).FIELD9
       WHERE  ID = V_ID;
	   
	   
	   SELECT COUNT(*) into  v_alarm_exist_cnt 
	   FROM MSS_BARCODE_RAW_ALARM_TEMP 
	   WHERE SITE_NAME=p_barcode_wo_ls(i).SITENAME
	         AND NVL(SOURCE,'XXXDDDZZZ')=NVL(p_barcode_wo_ls(i).SOURCE,'XXXDDDZZZ')
			 AND NVL(FIELD5,'XXXDDDZZZ')=NVL(p_barcode_wo_ls(i).FIELD5,'XXXDDDZZZ');
       --insert barcode raw alarm temple table
	   IF v_alarm_exist_cnt = 0 THEN 
         INSERT INTO 
           MSS_BARCODE_RAW_ALARM_TEMP
            (
              SITE_NAME,
              DESCRIPTION,
              SOURCE,
              FIELD5,
              FIELD9,
              FIELD11,
              CREATED_ON,
              CREATED_BY,
              MODIFIED_ON,
              MODIFIED_BY
            )
          VALUES
            (
              p_barcode_wo_ls(i).SITENAME,
              p_barcode_wo_ls(i).DESCRIPTION,
              p_barcode_wo_ls(i).SOURCE,
              p_barcode_wo_ls(i).FIELD5,
              p_barcode_wo_ls(i).FIELD9,
              p_barcode_wo_ls(i).FIELD11,
              SYSDATE,
              'MSSR',
              SYSDATE,
              'MSSR'
            );
		END IF;
     END IF;
  
    COMMIT;
    IF v_found = 'N' THEN 
      g_info_msz:='Can not find barcode.';
      P_WriteLog_File(file_name => g_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
    END IF;
    
    lt_id.DELETE;
    lt_barcode.DELETE;
    lt_value2.DELETE;
    lt_cat.DELETE;
    lt_subcat.DELETE;
    lt_department.DELETE;
    
    v_rtn_barcodes:=v_rtn_barcodes||V_BARCODE||'@';
    END LOOP;
    t2 := DBMS_UTILITY.get_time;
    g_info_msz := '----------------------------------------------------------------------------------'||
					               chr(13) || chr(10)||'Return barcodes : '||v_rtn_barcodes||
                         chr(13) || chr(10)||'Execution time  '||TO_CHAR((t2-t1)/100,'999.999')||
                         chr(13) || chr(10)||'Get Barcode Execution Completed @  '||to_char(sysdate,'MM/DD/YYYY HH24:MI:SS')||'s'||
                         chr(13) || chr(10)||'----------------------------------------------------------------------------------';   
    P_WriteLog_File(file_name => g_file_name
                         ,info     => g_info_msz
                         ,o_return_status  => v_return_status) ;
    P_CloseLog_File(g_file_name);
    
    RETURN v_rtn_barcodes;
  END get_barcode;
  
  
  ---------------------------------------
  --Format the values of csv stage table
  ---------------------------------------
  PROCEDURE format_barcode_csv
  IS
  
  CURSOR c_csv_file IS
  SELECT 
        mbcs.ASSET_TAG_NO,
        mbcs.STORE_NO,
        mss_site.site_id,
        mbcs.DEPARTMENT,
        mbcs.ASSET_CATEGORY,
        mbcs.ASSET_SUBCATEGORY,
        mbcs.ATTRIBUTE1,
        mbcs.VALUE1,
        mbcs.ATTRIBUTE2,
        mbcs.VALUE2
  FROM 
      MSS_BARCODE_CSV_STAGE mbcs,
      (
         SELECT ref1.ref_id site_id,medm.ext_value store_no 
         FROM jam.mss_ext_dispatch_mapping medm,jam.cmn_reference ref1 
         WHERE medm.mss_value=trim(ref1.ref_name)
       ) mss_site
  WHERE mbcs.store_no=mss_site.store_no
        --and rownum<101
        --and REGEXP_COUNT(mbcs.VALUE1,'CASE')>0
        and process_flag='N'
        --and value1='DELI C-1 CASE 1-3'
  ORDER BY mbcs.VALUE1;
  
   TYPE barcode_tab IS TABLE OF MSS_BARCODE_CSV_STAGE.ASSET_TAG_NO%TYPE INDEX BY BINARY_INTEGER;
   TYPE store_no_tab IS TABLE OF MSS_BARCODE_CSV_STAGE.STORE_NO%TYPE INDEX BY BINARY_INTEGER;
   TYPE site_id_tab IS TABLE OF MSS_BARCODE_MAPPING.SITE_ID%TYPE INDEX BY BINARY_INTEGER;
   TYPE department_tab IS TABLE OF MSS_BARCODE_CSV_STAGE.DEPARTMENT%TYPE INDEX BY BINARY_INTEGER;
   TYPE asset_category_tab IS TABLE OF MSS_BARCODE_CSV_STAGE.ASSET_CATEGORY%TYPE INDEX BY BINARY_INTEGER;
   TYPE asset_subcategory_tab IS TABLE OF MSS_BARCODE_CSV_STAGE.ASSET_SUBCATEGORY%TYPE INDEX BY BINARY_INTEGER;
   TYPE attribute1_tab IS TABLE OF MSS_BARCODE_CSV_STAGE.ATTRIBUTE1%TYPE INDEX BY BINARY_INTEGER;
   TYPE value1_tab IS TABLE OF MSS_BARCODE_CSV_STAGE.VALUE1%TYPE INDEX BY BINARY_INTEGER;
   TYPE attribute2_tab IS TABLE OF MSS_BARCODE_CSV_STAGE.ATTRIBUTE2%TYPE INDEX BY BINARY_INTEGER;
   TYPE value2_tab IS TABLE OF MSS_BARCODE_CSV_STAGE.VALUE2%TYPE INDEX BY BINARY_INTEGER;
   
   lt_barcode barcode_tab;
   lt_store_no store_no_tab;
   lt_site_id site_id_tab;
   lt_department department_tab;
   lt_asset_category asset_category_tab;
   lt_asset_subcategory asset_subcategory_tab;
   lt_attribute1 attribute1_tab;
   lt_value1 value1_tab;
   lt_attribute2 attribute2_tab;
   lt_value2 value2_tab;
   
  
   t1 INTEGER;
   t2 INTEGER;
   
   v_convert_value1 VARCHAR2(100);
   v_convert_value2 VARCHAR2(100);
   
   v_mapping_tab_cnt NUMBER(5);
   
   v_match_flag VARCHAR2(2);
   v_match_number NUMBER(5);
   
   v_processed_number NUMBER :=0;
   v_match_regexp_number NUMBER :=0;
   v_stat_info VARCHAR2(30000);
   v_return_status       VARCHAR2(1);
   
   PROCEDURE P_SPLITE_VALUE1(i_convert_value1 IN VARCHAR2
   )
   IS
   v_source_cnt NUMBER(2);
   v_case_cnt NUMBER(2);
   v_put_flag VARCHAR2(2);
   BEGIN
   v_source_cnt:=0;
   v_case_cnt:=0;
  --dbms_output.put_line('v_source='||v_source||' v_case='||v_case);
   FOR REPLACE_CHAR IN (
   select column_value
   from table(splitstr(i_convert_value1,g_replace_spliter))
   )LOOP
   v_put_flag:='Y';
   --dbms_output.put_line('REPLACE_CHAR.column_value='||REPLACE_CHAR.column_value);
   IF REPLACE_CHAR.column_value like '%CASE%' THEN
     IF g_barcode_case_ls.COUNT > 0 THEN 
       FOR j IN g_barcode_case_ls.FIRST .. g_barcode_case_ls.LAST LOOP
          IF g_barcode_case_ls(j) = REPLACE_CHAR.column_value THEN
             v_put_flag:='N';
          END IF;
       END LOOP;
     END IF;
     
     IF v_put_flag = 'Y' THEN
     v_case_cnt:=v_case_cnt+1;
     g_barcode_case_ls(v_case_cnt):=REPLACE_CHAR.column_value;
     END IF;
     
   ELSE
     IF g_barcode_source_ls.COUNT > 0 THEN
        FOR j IN g_barcode_source_ls.FIRST .. g_barcode_source_ls.LAST LOOP
          IF g_barcode_source_ls(j) = REPLACE_CHAR.column_value THEN
             v_put_flag:='N';
          END IF;
       END LOOP;
     END IF;
     
     IF v_put_flag = 'Y' THEN
       v_source_cnt:=v_source_cnt+1;
       g_barcode_source_ls(v_source_cnt):=REPLACE_CHAR.column_value;
     END IF;
   END IF;
   END LOOP;
   IF g_barcode_case_ls.COUNT = 0 THEN
      g_barcode_case_ls(1):='';
   END IF;
   IF g_barcode_source_ls.COUNT = 0 THEN
      g_barcode_source_ls(1):='';
   END IF;
   --dbms_output.put_line('g_barcode_source_ls='||g_barcode_source_ls(1)||' g_barcode_case_ls='||g_barcode_case_ls(1));
   END P_SPLITE_VALUE1;
    
   PROCEDURE P_STORE_BARCODE_MAP_DATA(
      i_cnt IN NUMBER,
      i_barcode_attribute1 IN VARCHAR2,
      i_barcode_convert_value1 IN VARCHAR2,
      i_barcode IN VARCHAR2,
      i_barcode_site_id IN NUMBER,
      i_barcode_store_no IN VARCHAR2,
      i_barcode_department IN VARCHAR2,
      i_barcode_asset_category IN VARCHAR2,
      i_barcode_asset_subcat IN VARCHAR2,
      i_barcode_attribute2 IN VARCHAR2,
      i_barcode_convert_value2 IN VARCHAR2
   )
   IS
   BEGIN
      --dbms_output.put_line('P_STORE_BARCODE_MAP_DATA::'||i_cnt);
      g_barcode_attribute1_ls(i_cnt):=i_barcode_attribute1;
      g_barcode_convert_value1_ls(i_cnt):=trim(i_barcode_convert_value1);
      g_barcode_ls(i_cnt):=i_barcode;
      g_barcode_site_id_ls(i_cnt):=i_barcode_site_id;
      g_barcode_store_no_ls(i_cnt):=i_barcode_store_no;
      g_barcode_department_ls(i_cnt):=trim(i_barcode_department);
      g_barcode_asset_category_ls(i_cnt):=trim(i_barcode_asset_category);
      g_barcode_asset_subcategory_ls(i_cnt):=trim(i_barcode_asset_subcat);
      g_barcode_attribute2_ls(i_cnt):=i_barcode_attribute2;
      g_barcode_convert_value2_ls(i_cnt):=trim(i_barcode_convert_value2);
   END P_STORE_BARCODE_MAP_DATA;
   
   PROCEDURE P_INSERT_BARCODE_MAPPING
   IS
   BEGIN
   --dbms_output.put_line('INSERT MAPPING TABLE');
   FORALL i IN g_barcode_ls.FIRST .. g_barcode_ls.LAST

    INSERT INTO MSS_BARCODE_MAPPING(
       BARCODE,
       SITE_ID,
       STORE_NO,
       DEPARTMENT,
       ASSET_CATEGORY,
       ASSET_SUBCATEGORY,
       ATTRIBUTE1,
       CONVERTED_VALUE1,
       ATTRIBUTE2,
       CONVERTED_VALUE2,
       CREATED_ON,
       CREATED_BY,
       MODIFIED_ON,
       MODIFIED_BY
    )
    VALUES
    (
    g_barcode_ls(i),
    g_barcode_site_id_ls(i),
    g_barcode_store_no_ls(i),
    g_barcode_department_ls(i),
    g_barcode_asset_category_ls(i),
    g_barcode_asset_subcategory_ls(i),
    g_barcode_attribute1_ls(i),
    g_barcode_convert_value1_ls(i),
    g_barcode_attribute2_ls(i),
    g_barcode_convert_value2_ls(i),
    SYSDATE,
    'MSSR',
    SYSDATE,
    'MSSR'
    );
    
    g_barcode_ls.delete;
    g_barcode_site_id_ls.delete;
    g_barcode_store_no_ls.delete;
    g_barcode_department_ls.delete;
    g_barcode_asset_category_ls.delete;
    g_barcode_asset_subcategory_ls.delete;
    g_barcode_attribute1_ls.delete;
    g_barcode_convert_value1_ls.delete;
    g_barcode_attribute2_ls.delete;
    g_barcode_convert_value2_ls.delete;
     
   END P_INSERT_BARCODE_MAPPING;
   
   
  BEGIN
  P_Log_File (directory_name   => g_directory
             ,file_name        => g_file_name   
             ,o_return_status  => v_return_status);
  g_info_msz := '----------------------------------------------------------------------------------'||
                      chr(13) || chr(10)||'Barcode CSV Format Package Execution Started @ '||to_char(sysdate,'MM/DD/YYYY HH24:MI:SS')||
                      chr(13) || chr(10)||'----------------------------------------------------------------------------------';
  P_WriteLog_File(file_name => g_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
                   
 
  --delete mapping table and recaculate the mapping table
  DELETE FROM  MSS_BARCODE_MAPPING;
    
  g_barcode_other_stat:=0;

  t1 := DBMS_UTILITY.get_time;
  
  OPEN c_csv_file;
  LOOP
  v_mapping_tab_cnt:=0;
  FETCH c_csv_file BULK COLLECT INTO
        lt_barcode,
        lt_store_no,
        lt_site_id,
        lt_department,
        lt_asset_category,
        lt_asset_subcategory,
        lt_attribute1,
        lt_value1,
        lt_attribute2,
        lt_value2
  LIMIT g_limit_count; 
  EXIT WHEN lt_barcode.COUNT = 0;
  FOR i IN lt_barcode.FIRST..lt_barcode.LAST LOOP
  v_match_flag:=NULL;
  v_processed_number:=v_processed_number+1;
  
  --format value2
  P_Format_value(i_value=>lt_value2(i),
                           i_regularExp_ls=>g_regexp_value2_ls,
                           i_replace_old_ls=>g_replace_old_value2_ls,
                           i_replace_new_ls=>g_replace_new_value2_ls,
                           i_check_regexp=>'Y',
                           o_match_flag=>v_match_flag,
                           o_match_number=>v_match_number,
                           o_convert_value=>v_convert_value2
                           );
  
  --format value1
  P_Format_value(i_value=>lt_value1(i),
                           i_regularExp_ls=>g_regexp_value1_ls,
                           i_replace_old_ls=>g_replace_old_value1_ls,
                           i_replace_new_ls=>g_replace_new_value1_ls,
                           i_check_regexp=>'Y',
                           o_match_flag=>v_match_flag,
                           o_match_number=>v_match_number,
                           o_convert_value=>v_convert_value1
                           );
                           
  IF v_match_flag IS NOT NULL THEN
   
  v_match_regexp_number:=v_match_regexp_number+1;
  g_barcode_match_regexp_stat(v_match_number):=g_barcode_match_regexp_stat(v_match_number)+1;
  P_SPLITE_VALUE1(i_convert_value1=>v_convert_value1);

   FOR n IN 1 .. g_barcode_source_ls.COUNT LOOP
      FOR m IN 1 .. g_barcode_case_ls.COUNT LOOP
      v_mapping_tab_cnt:=v_mapping_tab_cnt+1;
      P_STORE_BARCODE_MAP_DATA(
      i_cnt=>v_mapping_tab_cnt,
      i_barcode_attribute1=>lt_attribute1(i),
      i_barcode_convert_value1=>g_barcode_source_ls(n)||g_barcode_case_ls(m),
      i_barcode=>lt_barcode(i),
      i_barcode_site_id=>lt_site_id(i),
      i_barcode_store_no=>lt_store_no(i),
      i_barcode_department=>lt_department(i),
      i_barcode_asset_category=>lt_asset_category(i),
      i_barcode_asset_subcat=>lt_asset_subcategory(i),
      i_barcode_attribute2=>lt_attribute2(i),
      i_barcode_convert_value2=>v_convert_value2
      );
      END LOOP;
   END LOOP;
   g_info_msz :='V1: '||lt_value1(i)||'@RegExp#'||v_match_number||'# '||'@CV: '||v_convert_value1;
   P_WriteLog_File(file_name => g_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
   --clear
   g_barcode_source_ls.delete;
   g_barcode_case_ls.delete;
  END IF;
  
  --not match
  IF v_match_flag IS NULL THEN
  
    g_barcode_other_stat:=g_barcode_other_stat+1;
    
    v_mapping_tab_cnt:=v_mapping_tab_cnt+1;
    
   --format value1
   P_Format_value(i_value=>lt_value1(i),
                           i_regularExp_ls=>g_regexp_value1_ls,
                           i_replace_old_ls=>g_replace_old_value1_ls,
                           i_replace_new_ls=>g_replace_new_value1_ls,
                           i_check_regexp=>'N',
                           o_match_flag=>v_match_flag,
                           o_match_number=>v_match_number,
                           o_convert_value=>v_convert_value1
                           );
    
    P_STORE_BARCODE_MAP_DATA(
      i_cnt=>v_mapping_tab_cnt,
      i_barcode_attribute1=>lt_attribute1(i),
      i_barcode_convert_value1=>upper(v_convert_value1),
      i_barcode=>lt_barcode(i),
      i_barcode_site_id=>lt_site_id(i),
      i_barcode_store_no=>lt_store_no(i),
      i_barcode_department=>lt_department(i),
      i_barcode_asset_category=>lt_asset_category(i),
      i_barcode_asset_subcat=>lt_asset_subcategory(i),
      i_barcode_attribute2=>lt_attribute2(i),
      i_barcode_convert_value2=>upper(v_convert_value2)
   );
   g_info_msz :='V1: '||lt_value1(i)||'@Reg:N';
   P_WriteLog_File(file_name => g_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status);
   END IF;
  END LOOP;
  
  --insert into mapping table
  P_INSERT_BARCODE_MAPPING;
  
  END LOOP;
  --update process flag
  UPDATE MSS_BARCODE_CSV_STAGE SET PROCESS_FLAG='Y';
  COMMIT;
  --output statistics info
  t2 := DBMS_UTILITY.get_time;
  
  IF g_barcode_match_regexp_stat.COUNT>0 THEN 
  FOR i IN g_barcode_match_regexp_stat.FIRST .. g_barcode_match_regexp_stat.LAST LOOP
   v_stat_info:=v_stat_info||chr(13) || chr(10)||'RegExp#'||i||'#=>'||g_regexp_value1_ls(i)||' : '||g_barcode_match_regexp_stat(i);
  END LOOP;
  END IF;
  
  v_stat_info:=v_stat_info||chr(13) || chr(10)||'NO Match Regexp : '||g_barcode_other_stat;
  v_stat_info:=v_stat_info||chr(13) || chr(10)||'Total Match : '||v_match_regexp_number;
  
  IF v_processed_number <> 0 THEN 
     v_stat_info:=v_stat_info||chr(13) || chr(10)||'Match percentage : '||TO_CHAR(v_match_regexp_number*100/v_processed_number,'99.99')||'%';
  END IF;

  
  g_info_msz := '----------------------------------------------------------------------------------'||
                         chr(13) || chr(10)||'Total Processed  '||v_processed_number
                         ||v_stat_info||
                         chr(13) || chr(10)||'Execution time  '||TO_CHAR((t2-t1)/100,'999.999')||
                         chr(13) || chr(10)||'Barcode CSV Format Package Execution Completed @  '||to_char(sysdate,'MM/DD/YYYY HH24:MI:SS')||'s'||
                         chr(13) || chr(10)||'----------------------------------------------------------------------------------';   
  P_WriteLog_File(file_name => g_file_name
                       ,info     => g_info_msz
                       ,o_return_status  => v_return_status) ;
  P_CloseLog_File(g_file_name);
  CLOSE c_csv_file;
  END format_barcode_csv;
  
  --------------------------------
  --caculate_barcode mapping table
  --------------------------------
  PROCEDURE caculate_barcode_mapping
  IS
    CURSOR raw_alarm_data IS
    SELECT
      mbat.ID,
      cr.site_ID,
      mbat.SITE_NAME,
      mbat.SOURCE,
      mbat.DESCRIPTION,
      mbat.FIELD5,
      mbat.FIELD9
    FROM
      MSS_BARCODE_RAW_ALARM_TEMP mbat,
      mss_site_mapping cr
    WHERE
      trim(mbat.SITE_NAME) = trim(cr.site_mapping_value);
   TYPE mtid_tab IS TABLE OF MSS_BARCODE_RAW_ALARM_TEMP.ID%TYPE INDEX BY BINARY_INTEGER;
   TYPE siteid_tab IS TABLE OF CMN_REFERENCE.REF_ID%TYPE INDEX BY BINARY_INTEGER;
   TYPE sitename_tab IS TABLE OF MSS_BARCODE_RAW_ALARM_TEMP.SITE_NAME%TYPE INDEX BY BINARY_INTEGER;
   TYPE source_tab IS TABLE OF MSS_BARCODE_RAW_ALARM_TEMP.SOURCE%TYPE INDEX BY BINARY_INTEGER;
   TYPE descr_tab IS TABLE OF MSS_BARCODE_RAW_ALARM_TEMP.DESCRIPTION%TYPE INDEX BY BINARY_INTEGER;
   TYPE field5_tab IS TABLE OF MSS_BARCODE_RAW_ALARM_TEMP.FIELD5%TYPE INDEX BY BINARY_INTEGER;
   TYPE field9_tab IS TABLE OF MSS_BARCODE_RAW_ALARM_TEMP.FIELD9%TYPE INDEX BY BINARY_INTEGER;
   TYPE id_tab IS TABLE OF MSS_BARCODE_MAPPING.ID%TYPE INDEX BY BINARY_INTEGER;
   
   raw_id mtid_tab;
   raw_site_id siteid_tab;
   raw_site_name sitename_tab;
   raw_source source_tab;
   raw_descr descr_tab;
   raw_field5 field5_tab;
   raw_field9 field9_tab;
  
   id_cache_lt id_tab;
   raw_source_cache_lt source_tab;
   raw_field5_cache_lt field5_tab;
   raw_field9_cache_lt field9_tab;
   
   converted_id_lt    mtid_tab;
   converted_source_lt source_tab;
   converted_field5_lt field5_tab;
   converted_field9_lt field9_tab;
   
   r_source      VARCHAR2(255);
   r_field5      VARCHAR2(525);
   r_field9      VARCHAR2(250);
   r_siteid      NUMBER(30);
   r_value       VARCHAR2(250);
   
   CT1 INTEGER;
   CT2 INTEGER;
  
   v_source_match_flag VARCHAR2(2);
   v_field5_match_flag VARCHAR2(2);
   v_source_match_number NUMBER(5);
   v_field5_match_number NUMBER(5);
   v_source_match_regexp_number NUMBER :=0;
   v_field5_match_regexp_number NUMBER :=0;
   v_Sfield5_match_regexp_number NUMBER :=0;
   v_source_match_percentage NUMBER := 0;
   v_field5_match_percentage NUMBER := 0;
   v_Sfield5_match_percentage NUMBER := 0;
   
   v_case_match_percentage NUMBER :=0;
   v_case_match_number NUMBER :=0;
   v_case_total_number NUMBER :=0;
   v_case_match_flag VARCHAR2(2);
   
   v_refg_matchall_percentage NUMBER :=0;
   v_refg_match_percentage NUMBER :=0;
   v_refg BOOLEAN := false;
   v_refg_match_number NUMBER :=0;
   v_refg_total_number NUMBER :=0;
   
   v_rawalarm_processed_number NUMBER :=0;
   v_source_info VARCHAR2(30000);
   v_field5_info VARCHAR2(30000);
   v_return_status       VARCHAR2(1);
   
   v_raw_match_cnt NUMBER(5); 
   
  BEGIN
  
	P_Log_File (directory_name   => g_directory
             ,file_name        => g_file_name   
             ,o_return_status  => v_return_status);
    g_info_msz := '----------------------------------------------------------------------------------'||
                      chr(13) || chr(10)||'Caculate_barcode_mapping Execution Started @ '||to_char(sysdate,'MM/DD/YYYY HH24:MI:SS')||
                      chr(13) || chr(10)||'----------------------------------------------------------------------------------';
    P_WriteLog_File(file_name => g_file_name
                   ,info     => g_info_msz
                   ,o_return_status  => v_return_status) ;
    
    --dbms_output.put_line('caculate_barcode_mapping');
    CT1 := DBMS_UTILITY.get_time;
------------------------------------------------------------------------------
  
  OPEN raw_alarm_data;
  LOOP
  FETCH raw_alarm_data BULK COLLECT INTO
        raw_id,
        raw_site_id,
        raw_site_name,
        raw_source,
        raw_descr,
        raw_field5,
        raw_field9
  LIMIT g_limit_count;
  EXIT WHEN raw_id.COUNT = 0;
      v_raw_match_cnt :=0;
      FOR i IN raw_id.FIRST..raw_id.LAST LOOP
      v_source_match_flag :=NULL;
      v_field5_match_flag :=NULL;
      v_rawalarm_processed_number:=v_rawalarm_processed_number+1;
      
         P_Format_value(
             i_value=>raw_source(i),
             i_regularExp_ls=>g_regexp_source_ls,
             i_replace_old_ls=>g_replace_old_source_ls,
             i_replace_new_ls=>g_replace_new_source_ls,
             i_check_regexp=>'Y',
             o_match_flag=>v_source_match_flag,
             o_match_number=>v_source_match_number,
             o_convert_value=>r_source);
         IF v_source_match_flag IS NOT NULL THEN
            v_source_match_regexp_number:=v_source_match_regexp_number+1;
            g_source_match_regexp_stat(v_source_match_number):=g_source_match_regexp_stat(v_source_match_number)+1;
            g_info_msz :='Source: '||raw_source(i)||'@RegExp#'||v_source_match_number||'#=>'||g_regexp_source_ls(v_source_match_number)||'@CV: '||r_source;
            P_WriteLog_File(file_name => g_file_name
            ,info     => g_info_msz
            ,o_return_status  => v_return_status);
         END IF;
          
         P_Format_value(
             i_value=>raw_field5(i),
             i_regularExp_ls=>g_regexp_field5_ls,
             i_replace_old_ls=>g_replace_old_field5_ls,
             i_replace_new_ls=>g_replace_new_field5_ls,
             i_check_regexp=>'Y',
             o_match_flag=>v_field5_match_flag,
             o_match_number=>v_field5_match_number,
             o_convert_value=>r_field5);
         IF v_field5_match_flag IS NOT NULL THEN
            v_field5_match_regexp_number:=v_field5_match_regexp_number+1;
            g_field5_match_regexp_stat(v_field5_match_number):=g_field5_match_regexp_stat(v_field5_match_number)+1;
            g_info_msz :='Field5: '||raw_field5(i)||'@RegExp#'||v_field5_match_number||'#=>'||g_regexp_field5_ls(v_field5_match_number)||'@CV: '||r_field5;
         
            P_WriteLog_File(file_name => g_file_name
            ,info     => g_info_msz
            ,o_return_status  => v_return_status);
         END IF;
         
          
        IF raw_descr(i) = 'Case Temp Hi Limit Exceeded' OR raw_descr(i) = 'Case Temp Low Limit Exceeded' THEN
            v_case_total_number := v_case_total_number+1;
        END IF; 
		
		v_refg := raw_descr(i) ='Product Temp Hi Limit Exceeded' OR raw_descr(i) ='High Discharge Limit Exceeded' 
          OR raw_descr(i) = 'Multi High Discharge in 24 HR' OR raw_descr(i) = 'Case Temp Low Limit Exceeded' 
          OR raw_descr(i) = 'Case Temp Hi Limit Exceeded' OR raw_descr(i) = 'High Suction Limit Exceeded' 
          OR raw_descr(i) = 'Multi High Suction in 24 HR' OR raw_descr(i) = 'Low Suction Limit Exceeded'
          OR raw_descr(i) = 'Multi Low Suction in 24 HR' OR raw_descr(i) = 'IRLDS: Voltage data error'
          OR raw_descr(i) = 'Rack Fail Discharge Trip' OR raw_descr(i) = 'Rack Fail Phase Fail'
          OR raw_descr(i) = 'Leak level exceeded' OR raw_descr(i) = 'Rack Fail Pump Down'
          OR raw_descr(i) = 'System in Pump Down' OR raw_descr(i) = 'Low Liquid Level'
          OR raw_descr(i) = 'REFR Phase Loss' OR raw_descr(i) = 'Discharge Trip';
        IF v_refg THEN
          v_refg_total_number := v_refg_total_number +1;
        END IF;
        
         r_siteid := raw_site_id(i);
         r_value  := r_source || r_field5;
         
         converted_id_lt(i) := raw_id(i);
         converted_source_lt(i) := r_source;
         converted_field5_lt(i) := r_field5;
         converted_field9_lt(i) := raw_field9(i);
         
         v_case_match_flag := 'N';
         --dbms_output.put_line('r_value is:' ||r_value);
        FOR BARCODE_FORMAT_TAB IN(
           SELECT ID,SITE_ID,CONVERTED_VALUE1,CONVERTED_VALUE2,MSS_SOURCE,MSS_FIELD5,MSS_FIELD9
           FROM MSS_BARCODE_MAPPING
           WHERE CONVERTED_VALUE1 = UPPER(r_value)
           AND SITE_ID =  r_siteid
           )LOOP
           IF BARCODE_FORMAT_TAB.CONVERTED_VALUE1 IS NOT NULL THEN
             v_case_match_flag := 'Y';
             v_raw_match_cnt := v_raw_match_cnt + 1;
             id_cache_lt(v_raw_match_cnt) := BARCODE_FORMAT_TAB.ID;
             raw_source_cache_lt(v_raw_match_cnt) := raw_source(i);
             raw_field5_cache_lt(v_raw_match_cnt) := raw_field5(i);
             raw_field9_cache_lt(v_raw_match_cnt) := raw_field9(i);
           END IF;
        END LOOP;

        IF v_case_match_flag = 'Y' THEN
          v_Sfield5_match_regexp_number := v_Sfield5_match_regexp_number+1;
          IF raw_descr(i) = 'Case Temp Hi Limit Exceeded' OR raw_descr(i) = 'Case Temp Low Limit Exceeded' 
          THEN
              v_case_match_number := v_case_match_number+1;
          END IF;

          IF v_refg THEN
            v_refg_match_number := v_refg_match_number+1;
          END IF;
        END IF;
        
      END LOOP;
      
      FORALL j IN raw_source_cache_lt.FIRST..raw_source_cache_lt.LAST
        UPDATE MSS_BARCODE_MAPPING  SET 
              CREATED_ON = SYSDATE,
              MODIFIED_ON = SYSDATE,
              MSS_SOURCE = trim(raw_source_cache_lt(j)),
              MSS_FIELD5 = trim(raw_field5_cache_lt(j)),
              MSS_FIELD9 = trim(raw_field9_cache_lt(j))
        WHERE 
             ID = id_cache_lt(j);
      
      FORALL j IN converted_id_lt.FIRST..converted_id_lt.LAST
        UPDATE MSS_BARCODE_RAW_ALARM_TEMP  SET 
              CREATED_ON = SYSDATE,
              MODIFIED_ON = SYSDATE,
              SOURCE_FORMAT = converted_source_lt(j),
              FIELD5_FORMAT = converted_field5_lt(j),
              FIELD9_FORMAT = converted_field9_lt(j)
        WHERE 
             ID = converted_id_lt(j);
      
             
  END LOOP;
  
  COMMIT;
  
  CT2 := DBMS_UTILITY.get_time;
  --dbms_output.put_line('Execution time is '||TO_CHAR((CT2-CT1)/100,'999.999'));
  
  IF g_source_match_regexp_stat.COUNT > 0 THEN
  FOR j IN g_source_match_regexp_stat.FIRST .. g_source_match_regexp_stat.LAST LOOP
   v_source_info:=v_source_info||chr(13) || chr(10)||'Source Regexp @'||j||'#=>'||g_regexp_source_ls(j)||': '||g_source_match_regexp_stat(j);
  END LOOP;
  END IF;
  
  IF g_field5_match_regexp_stat.COUNT > 0 THEN 
  FOR k IN g_field5_match_regexp_stat.FIRST .. g_field5_match_regexp_stat.LAST LOOP
   v_field5_info:=v_field5_info||chr(13) || chr(10)||'Field Regexp @'||k||'#=>'||g_regexp_field5_ls(k)||': '||g_field5_match_regexp_stat(k);
  END LOOP;
  END IF;
  
  
  IF v_rawalarm_processed_number <> 0 THEN
       v_source_match_percentage :=  TO_CHAR(v_source_match_regexp_number*100/v_rawalarm_processed_number,'99.99');    
  END IF;
  IF v_rawalarm_processed_number <> 0 THEN
       v_field5_match_percentage :=  TO_CHAR(v_field5_match_regexp_number*100/v_rawalarm_processed_number,'99.99');    
  END IF;
  IF v_rawalarm_processed_number <> 0 THEN
       v_Sfield5_match_percentage :=  TO_CHAR(v_Sfield5_match_regexp_number*100/v_rawalarm_processed_number,'99.99');    
  END IF;
  IF v_case_total_number <> 0 THEN
         v_case_match_percentage :=  TO_CHAR(v_case_match_number*100/v_case_total_number,'99.99');    
  END IF;
  
  IF v_refg_total_number <> 0 THEN
       v_refg_match_percentage :=  TO_CHAR(v_refg_match_number*100/v_refg_total_number,'99.99');    
  END IF;
  
  IF v_rawalarm_processed_number <> 0 THEN
       v_refg_matchall_percentage :=  TO_CHAR(v_refg_match_number*100/v_rawalarm_processed_number,'99.99');    
  END IF;

  g_info_msz := '----------------------------------------------------------------------------------'||
                         chr(13) || chr(10)||'Total Processed  '||v_rawalarm_processed_number||v_source_info||v_field5_info||
                         chr(13) || chr(10)||'Total Source Match '||v_source_match_regexp_number||
                         chr(13) || chr(10)||'Total Field5 Match '||v_field5_match_regexp_number||
                         chr(13) || chr(10)||'Source Match percentage '||v_source_match_percentage||'%'||
                         chr(13) || chr(10)||'Field5 Match percentage '||v_field5_match_percentage||'%'||
                         chr(13) || chr(10)||'SourceField5 Match percentage '||v_Sfield5_match_percentage||'%'||
                         chr(13) || chr(10)||'Only CaseTemp Type Match percentage '||v_case_match_percentage||'%'||
                         chr(13) || chr(10)||'Refrigeration Match percentage '||v_refg_match_percentage||'%'||
                         chr(13) || chr(10)||'Refrigeration Match all percentage '||v_refg_matchall_percentage||'%'||
                         chr(13) || chr(10)||'Execution time  '||TO_CHAR((CT2-CT1)/100,'999.999')||
                         chr(13) || chr(10)||'Caculate_barcode_mapping Completed @  '||to_char(sysdate,'MM/DD/YYYY HH24:MI:SS')||'s'||
                         chr(13) || chr(10)||'----------------------------------------------------------------------------------';
  P_WriteLog_File(file_name => g_file_name
                       ,info     => g_info_msz
                       ,o_return_status  => v_return_status) ;
  P_CloseLog_File(g_file_name);
  CLOSE raw_alarm_data;
  END caculate_barcode_mapping;
  
  
  FUNCTION splitstr(p_string IN VARCHAR2, p_delimiter IN VARCHAR2)
    RETURN strsplit_type 
    PIPELINED
    AS
    v_length   NUMBER := LENGTH(p_string);
    v_start    NUMBER := 1;
    v_index    NUMBER;
   BEGIN
    WHILE(v_start <= v_length)
    LOOP
        v_index := INSTR(p_string, p_delimiter, v_start);

        IF v_index = 0
        THEN
            PIPE ROW(SUBSTR(p_string, v_start));
            v_start := v_length + 1;
        ELSE
            PIPE ROW(SUBSTR(p_string, v_start, v_index - v_start));
            v_start := v_index + 1;
        END IF;
    END LOOP;

    RETURN;
  END splitstr;
  
  ------------------------------------------------------------
  --Format barcode CSV file and caculate barcode mapping table
  ------------------------------------------------------------
  PROCEDURE preproccess_barcode( errbuf OUT VARCHAR2,
                                  retcode OUT VARCHAR2
                                 )
  IS
  v_to_process_number NUMBER(10);
  BEGIN
  SELECT COUNT(*) INTO v_to_process_number 
  FROM MSS_BARCODE_CSV_STAGE
  WHERE PROCESS_FLAG='N';
  
  IF v_to_process_number>0 THEN
  --cache regular expression
  Cache_RegularExp;
  
  format_barcode_csv;
  caculate_barcode_mapping;
  END IF;
  END preproccess_barcode;
  
  --Only For testing P_FORMAT_VALUE procedure
  PROCEDURE TEST_FORMAT_VALUE(
            VAL IN VARCHAR2,
            TYPEOF IN VARCHAR2
  )
  IS
  v_to_process_number NUMBER(10);
  v_match_flag varchar2(10);
  v_match_number number(2);
  v_convert_value1 varchar2(100);
  v_return_status VARCHAR2(1);
  BEGIN
  P_Log_File (directory_name   => g_directory
             ,file_name        => g_file_name   
             ,o_return_status  => v_return_status);
  Cache_RegularExp;
    --format value1
    dbms_output.put_line('-----------------------------------------');
    dbms_output.put_line(TYPEOF||' : '||VAL);
    IF TYPEOF='value1' THEN
      P_Format_value(i_value=>VAL,
                             i_regularExp_ls=>g_regexp_value1_ls,
                             i_replace_old_ls=>g_replace_old_value1_ls,
                             i_replace_new_ls=>g_replace_new_value1_ls,
                             i_check_regexp=>'Y',
                             o_match_flag=>v_match_flag,
                             o_match_number=>v_match_number,
                             o_convert_value=>v_convert_value1
                             );
      
    END IF;
    IF TYPEOF='value2' THEN
       P_Format_value(i_value=>VAL,
                               i_regularExp_ls=>g_regexp_value2_ls,
                               i_replace_old_ls=>g_replace_old_value2_ls,
                               i_replace_new_ls=>g_replace_new_value2_ls,
                               i_check_regexp=>'Y',
                               o_match_flag=>v_match_flag,
                               o_match_number=>v_match_number,
                               o_convert_value=>v_convert_value1
                               );
    END IF;
    
    IF TYPEOF='source' THEN
      P_Format_value(i_value=>VAL,
                             i_regularExp_ls=>g_regexp_source_ls,
                             i_replace_old_ls=>g_replace_old_source_ls,
                             i_replace_new_ls=>g_replace_new_source_ls,
                             i_check_regexp=>'Y',
                             o_match_flag=>v_match_flag,
                             o_match_number=>v_match_number,
                             o_convert_value=>v_convert_value1
                             );  
    END IF;
    
    IF TYPEOF='field5' THEN
      P_Format_value(i_value=>VAL,
                             i_regularExp_ls=>g_regexp_field5_ls,
                             i_replace_old_ls=>g_replace_old_field5_ls,
                             i_replace_new_ls=>g_replace_new_field5_ls,
                             i_check_regexp=>'Y',
                             o_match_flag=>v_match_flag,
                             o_match_number=>v_match_number,
                             o_convert_value=>v_convert_value1
                             ); 
    END IF;
    
    IF TYPEOF='field9' THEN
      P_Format_value(i_value=>VAL,
                             i_regularExp_ls=>g_regexp_field9_ls,
                             i_replace_old_ls=>g_replace_old_field9_ls,
                             i_replace_new_ls=>g_replace_new_field9_ls,
                             i_check_regexp=>'Y',
                             o_match_flag=>v_match_flag,
                             o_match_number=>v_match_number,
                             o_convert_value=>v_convert_value1
                             ); 
    END IF;
    
    IF TYPEOF='rack' THEN
      P_Format_value(i_value=>VAL,
                             i_regularExp_ls=>g_regexp_rack_ls,
                             i_replace_old_ls=>g_replace_old_rack_ls,
                             i_replace_new_ls=>g_replace_new_rack_ls,
                             i_check_regexp=>'Y',
                             o_match_flag=>v_match_flag,
                             o_match_number=>v_match_number,
                             o_convert_value=>v_convert_value1
                             );
    END IF;
    
    dbms_output.put_line('Converted value:'||v_convert_value1);
	dbms_output.put_line('' ||v_match_number );
    dbms_output.put_line('-----------------------------------------');
    
  END TEST_FORMAT_VALUE;
   
END SF_BARCODE_PKG;

/