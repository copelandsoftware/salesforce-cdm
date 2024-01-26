CREATE TABLE SF_CONTACT_TEMP
(
    ID                            NUMBER(19)
    ,SF_CONTACT_ID                VARCHAR2(18)
    ,FIRST_NAME                   VARCHAR2(255)
    ,LAST_NAME                    VARCHAR2(255)
    ,PHONE                        VARCHAR2(255)
    ,EMAIL                        VARCHAR2(255)
    ,IS_SERVICE_PROVIDER          VARCHAR2(2)
    ,ADDRESS                      VARCHAR2(255)
    ,METHOD_OF_DELIVERY           VARCHAR2(255)
    ,IS_ACTIVE                    VARCHAR2(2)
    ,SEND_FLAG                    VARCHAR2(10)
    ,CREATED_ON                   DATE
    ,CREATED_BY                   VARCHAR2(50)
    ,MODIFIED_ON                  DATE
    ,MODIFIED_BY                  VARCHAR2(50)
    ,SF_SITE_ID                   VARCHAR2(18)
    ,SF_CUST_ID                   VARCHAR2(18)
    ,SF_AVAILABILITY_ID           VARCHAR2(18)
    ,PRIORITY                     VARCHAR2(20)
    ,SYNCH_MSG                    VARCHAR2(3000)
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

ALTER TABLE SF_CONTACT_TEMP ADD (
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
  ENABLE VALIDATE);

/

ALTER TABLE SF_CONTACT_TEMP ADD CONSTRAINT REF_SF_CONTACT_TEMP_FK1
    FOREIGN KEY (SF_SITE_ID)
    REFERENCES SF_SITE(SF_SITE_ID);
/

CREATE SEQUENCE SF_CONTACT_TEMP_SQ
  MINVALUE 1
  NOMAXVALUE
  START WITH 1
  INCREMENT BY 1
  NOCYCLE
  NOCACHE
;

/

CREATE OR REPLACE TRIGGER SF_CONTACT_TEMP_TG
  BEFORE INSERT ON SF_CONTACT_TEMP
  FOR EACH ROW
  DECLARE
    n_id NUMBER;
    seq_name VARCHAR2(50);
  BEGIN
    seq_name := 'SF_CONTACT_TEMP_SQ' || '.NEXTVAL';
    EXECUTE IMMEDIATE 'SELECT ' || seq_name || ' FROM DUAL' INTO n_id;
    :new.ID := n_id;
  END;
/