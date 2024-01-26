CREATE TABLE SF_MEIJER_SCORECARD
(
  ID                                   NUMBER(19)
 ,DIVISION                             VARCHAR2(255)
 ,STORE                                VARCHAR2(255)
 ,MAIN_SYSTEM                          VARCHAR2(255)
 ,SOURCE                               VARCHAR2(255)
 ,DESCRIPTION                          VARCHAR2(255)
 ,ALARM_DATE                           DATE
 ,TOTAL_ALARMS                         NUMBER(10)
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

ALTER TABLE SF_MEIJER_SCORECARD ADD (
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

CREATE SEQUENCE SF_MEIJER_SCORECARD_SQ
  MINVALUE 1
  NOMAXVALUE
  START WITH 1
  INCREMENT BY 1
  NOCYCLE
  NOCACHE
;

/

CREATE OR REPLACE TRIGGER SF_MEIJER_SCORECARD_TG
  BEFORE INSERT ON SF_MEIJER_SCORECARD
  FOR EACH ROW
  DECLARE
    n_id NUMBER;
    seq_name VARCHAR2(50);
  BEGIN
    seq_name := 'SF_MEIJER_SCORECARD_SQ' || '.NEXTVAL';
    EXECUTE IMMEDIATE 'SELECT ' || seq_name || ' FROM DUAL' INTO n_id;
    :new.ID := n_id;
  END;
/