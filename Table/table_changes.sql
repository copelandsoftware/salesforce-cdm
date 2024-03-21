--ALL CDM table changes

--add place in pending recommendation return start time and end time
--status-Deployed to prod
alter table jam.SF_NORM_ALARM add ADM_RECOMM_PENDING_START_DATE DATE;
alter table jam.SF_NORM_ALARM add ADM_RECOMM_PENDING_END_DATE DATE;
alter table jam.SF_NORM_ALARM add ADM_POINT_ID NUMBER(18);

--add a new table to persist ADM request and response json
--2022-06-16
--Gary Sun
--status-Development
  CREATE SEQUENCE SF_ADM_REQ_LOG_SQ
  MINVALUE 1
  NOMAXVALUE
  START WITH 1
  INCREMENT BY 1
  NOCYCLE
  NOCACHE
;
/
  CREATE TABLE "JAM"."SF_ADM_REQ_LOG"
   (	"SF_ADM_REQ_LOG_ID" NUMBER(38,0),
	"REQUEST_NAME" VARCHAR2(255),
	"REQ_TIME" DATE,
	"REQ_MESSAGE" CLOB,
	"CREATED_ON" DATE,
	"CREATED_BY" VARCHAR2(50 BYTE),
	"MODIFIED_ON" DATE,
	"MODIFIED_BY" VARCHAR2(50 BYTE),
	 PRIMARY KEY ("SF_ADM_REQ_LOG_ID")
	 )
 PCTFREE 10
INITRANS 1
MAXTRANS 255
TABLESPACE SFRCD01
STORAGE(INITIAL 16M
        NEXT 8M
        MINEXTENTS 1
        MAXEXTENTS UNLIMITED
        PCTINCREASE 0
        )
/

  CREATE OR REPLACE EDITIONABLE TRIGGER "JAM"."SF_ADM_REQ_LOG_TG"
	  BEFORE INSERT ON SF_ADM_REQ_LOG
	  FOR EACH ROW
	  DECLARE
	    n_id NUMBER;
	    seq_name VARCHAR2(50);
	  BEGIN
	    seq_name := 'SF_ADM_REQ_LOG_SQ' || '.NEXTVAL';
	    EXECUTE IMMEDIATE 'SELECT ' || seq_name || ' FROM DUAL' INTO n_id;
	    :new.SF_ADM_REQ_LOG_ID := n_id;
	  END;

/
ALTER TRIGGER "JAM"."SF_ADM_REQ_LOG_TG" ENABLE;

/
alter table jam.SF_ADM_REQ_LOG add ADM_BATCH_ID NUMBER(38);
/
alter table sf_site modify sf_site_name varchar2(100);
/
----------above have already deployed
alter table sf_norm_alarm drop column SITE_CONDITION_FLAG;
drop table sf_site_condition_config;

--if need
alter table sf_customer drop column SITE_CONDITION_ENABLED;
alter table sf_customer drop column SC_AUTO_DISREGARD;

alter table sf_site drop column SITE_CONDITION_ENABLED;
alter table sf_site drop column SC_AUTO_DISREGARD;

---------------
alter table SF_CONTROL_SYS drop column SITE_MAPPING1;
alter table SF_CONTROL_SYS drop column SITE_MAPPING2;
alter table SF_CONTROL_SYS drop column SITE_MAPPING3;
alter table SF_CONTROL_SYS drop column SITE_MAPPING4;
alter table SF_CONTROL_SYS drop column SITE_MAPPING5;

alter table SF_ALARM_EMAIL_DELIVERY drop column IS_CLOSED_LOOP_ENABLED;

---filter work order alert
Insert into MSS_SYS_CONFIG (SYS_CONFIG_ID,SYS_CONFIG_TYPE_CD,SYS_CONFIG_CD,SERVICE_PROVIDER_ID,SYS_CONFIG_VALUE,STATUS_CD,CREATED_BY,CREATED_ON,MODIFIED_BY,MODIFIED_ON,REQUEST_ID,PROGRAM_APP_ID,VERSION_NUMBER,CUST_ID) values (111,'FilterOutExceptions','ErrorMessageDesc',null,'Servlet.service() for servlet [dispatcherServlet] in context with path [] threw exception;;Servlet.service() for servlet [dispatcherServlet] threw exception;;Exception Processing ErrorPage[errorCode=0, location=/error]','1','SEEDED',sysdate,'SEEDED',sysdate,null,null,null,null);

--Auto email
alter table jam.SF_NORM_ALARM add auto_email_flag varchar2(10);
alter table jam.SF_MESSAGE_CONFIG add AUTO_EMAIL_DESCRIPTION varchar2(4000);

--
alter table jam.SF_MESSAGE_CONFIG add AUTO_EMAIL_SOURCE varchar2(4000);
alter table jam.SF_MESSAGE_CONFIG add AUTO_EMAIL_CRITERIA varchar2(20);
/
--US4154,Nominate Alarm Rule
alter table sf_message_config modify APPROVED_IND varchar2(20);
alter table sf_alarm_email_delivery modify APPROVED_IND varchar2(20);
/
--ADM support controllers configuration
Insert into MSS_SYS_CONFIG (SYS_CONFIG_ID,SYS_CONFIG_TYPE_CD,SYS_CONFIG_CD,SERVICE_PROVIDER_ID,SYS_CONFIG_VALUE,STATUS_CD,CREATED_BY,CREATED_ON,MODIFIED_BY,MODIFIED_ON,REQUEST_ID,PROGRAM_APP_ID,VERSION_NUMBER,CUST_ID) values (111,'ADM','SUPPORT_CONTROLLER',null,'E2;E3;Site Supv','1','SEEDED',sysdate,'SEEDED',sysdate,null,null,null,null);

--KT auto email without Salesforce
alter table jam.SF_NORM_ALARM add auto_email_cdm_flag varchar2(10);

Insert into MSS_SYS_CONFIG (SYS_CONFIG_ID,SYS_CONFIG_TYPE_CD,SYS_CONFIG_CD,SERVICE_PROVIDER_ID,SYS_CONFIG_VALUE,STATUS_CD,CREATED_BY,CREATED_ON,MODIFIED_BY,MODIFIED_ON,REQUEST_ID,PROGRAM_APP_ID,VERSION_NUMBER,CUST_ID) values (111,'AutoEmailCDM','SUPPORT_CUSTOMER',null,'Kwik Trip, Inc','1','SEEDED',sysdate,'SEEDED',sysdate,null,null,null,null);
