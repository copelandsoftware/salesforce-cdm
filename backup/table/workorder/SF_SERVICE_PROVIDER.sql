CREATE TABLE SF_SERVICE_PROVIDER
(
  SF_SERVICE_PROVIDER_ID   VARCHAR2(18)
 ,SF_ACCOUNT_ID        VARCHAR2(18)
 ,FIRST_NAME VARCHAR2(255)
 ,LAST_NAME      VARCHAR2(255)
 ,created_on 		       DATE
 ,created_by 		       VARCHAR2(50)
 ,modified_on              DATE
 ,modified_by              VARCHAR2(50)
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

ALTER TABLE SF_SERVICE_PROVIDER ADD (
  PRIMARY KEY
  (SF_SERVICE_PROVIDER_ID)
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
