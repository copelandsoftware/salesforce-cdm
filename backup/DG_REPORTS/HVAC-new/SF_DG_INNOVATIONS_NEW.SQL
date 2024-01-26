CREATE TABLE SF_DG_INNOVATIONS_NEW
(
  ID                                   NUMBER(19)
 ,STORE                                VARCHAR2(255)
 ,ALARM_MONITORING_DATE                DATE
 ,STORE_CHAIN                          VARCHAR2(255)
 ,DEG_55_EXCEEDED_1                    NUMBER
 ,DEG_80_EXCEEDED_1                    NUMBER
 ,DEG_85_EXCEEDED_1                    NUMBER
 ,UNIT_RUNNING_AND_NOT_COOLING_1       NUMBER
 ,SPACE_TEMP_NOT_ACHIEVED_1            NUMBER
 ,DEG_55_EXCEEDED_2                    VARCHAR2(4000)
 ,DEG_85_EXCEEDED_2                    VARCHAR2(4000)
 ,DEG_80_EXCEEDED_2                    VARCHAR2(4000)
 ,DEG_85_EXCEEDED_2                    VARCHAR2(4000)
 ,UNIT_RUNNING_AND_NOT_COOLING_2       VARCHAR2(4000)
 ,SPACE_TEMP_NOT_ACHIEVED_2            VARCHAR2(4000)
 ,TOTAL_ALARMS                         NUMBER
 ,STATE                                VARCHAR2(4000)
 ,NUMBER_OF_UNITS_DOWN                 VARCHAR2(4000)
 ,TOTAL_UNITS                          NUMBER(5)
 ,PROCESSED_FLAG                       VARCHAR2(10)
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

ALTER TABLE SF_DG_INNOVATIONS_NEW ADD (
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

CREATE SEQUENCE SF_DG_INNOVATIONS_NEW_SQ
  MINVALUE 1
  NOMAXVALUE
  START WITH 1
  INCREMENT BY 1
  NOCYCLE
  NOCACHE
;

/

CREATE OR REPLACE TRIGGER SF_DG_INNOVATIONS_NEW_TG
  BEFORE INSERT ON SF_DG_INNOVATIONS_NEW
  FOR EACH ROW
  DECLARE
    n_id NUMBER;
    seq_name VARCHAR2(50);
  BEGIN
    seq_name := 'SF_DG_INNOVATIONS_NEW_SQ' || '.NEXTVAL';
    EXECUTE IMMEDIATE 'SELECT ' || seq_name || ' FROM DUAL' INTO n_id;
    :new.ID := n_id;
  END;
/