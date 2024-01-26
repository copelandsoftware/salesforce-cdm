CREATE TABLE SF_DG_LIGHTING
(
  ID                                   NUMBER(19)
 ,STORE                                VARCHAR2(255)
 ,ALARM_MONITORING_DATE                DATE
 ,STORE_CHAIN                          VARCHAR2(100)
 ,LIGHTS_ON_TOO_LONG                   NUMBER
 ,EXCESSIVE_LIGHT_CYCLES               NUMBER
 ,LIGHTS_ON_TOO_LONG_APPS              VARCHAR2(4000)
 ,EXCESSIVE_LIGHT_CYCLES_APPS          VARCHAR2(4000)
 ,TOTAL_ALARMS                         NUMBER
 ,STATE                                VARCHAR2(60)
 ,created_on 		                   DATE
 ,created_by 		                   VARCHAR2(50)
 ,modified_on                          DATE
 ,modified_by                          VARCHAR2(50)
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

ALTER TABLE SF_DG_LIGHTING ADD (
  PRIMARY KEY
  (ID)
  USING INDEX
    TABLESPACE SFRCX01
    PCTFREE    10
    INITRANS   2
    MAXTRANS   255
    STORAGE    (
                INITIAL          16M
                NEXT             8M
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                PCTINCREASE      0
                BUFFER_POOL      DEFAULT
               )
  ENABLE VALIDATE)
  
/

CREATE SEQUENCE SF_DG_LIGHTING_SQ
  MINVALUE 1
  NOMAXVALUE
  START WITH 1
  INCREMENT BY 1
  NOCYCLE
  NOCACHE
;

/

CREATE OR REPLACE TRIGGER SF_DG_LIGHTING_TG
  BEFORE INSERT ON SF_DG_LIGHTING
  FOR EACH ROW
  DECLARE
    n_id NUMBER;
    seq_name VARCHAR2(50);
  BEGIN
    seq_name := 'SF_DG_LIGHTING_SQ' || '.NEXTVAL';
    EXECUTE IMMEDIATE 'SELECT ' || seq_name || ' FROM DUAL' INTO n_id;
    :new.ID := n_id;
  END;
/