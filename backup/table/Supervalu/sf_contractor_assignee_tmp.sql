CREATE TABLE sf_contact_assignee_temp
(
  ID                    NUMBER(19)
 ,sf_id                 VARCHAR2(18)
 ,sf_native_contact_id  NUMBER(19)
 ,sf_contact_id         VARCHAR2(18)
 ,sf_native_asset_id    NUMBER(19)
 ,sf_asset_id           VARCHAR2(18)
 ,IS_SERVICE_PROVIDER   VARCHAR2(2)
 ,priority              NUMBER(5)
 ,IS_ACTIVE             VARCHAR2(2)
 ,sf_site_id            VARCHAR2(18)
 ,SEND_FLAG             VARCHAR2(10)
 ,created_on 		    DATE
 ,created_by 		    VARCHAR2(50)
 ,modified_on           DATE
 ,modified_by           VARCHAR2(50)
 ,SYNCH_MSG             VARCHAR2(3000)
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

ALTER TABLE sf_contact_assignee_temp ADD (
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

ALTER TABLE sf_contact_assignee_temp ADD CONSTRAINT REF_sf_cont_assignee_temp_FK1
    FOREIGN KEY (SF_SITE_ID)
    REFERENCES SF_SITE(SF_SITE_ID)
/

CREATE SEQUENCE sf_contact_assignee_temp_SQ
  MINVALUE 1
  NOMAXVALUE
  START WITH 1
  INCREMENT BY 1
  NOCYCLE
  NOCACHE
;

/

CREATE OR REPLACE TRIGGER sf_contact_assignee_temp_TG
  BEFORE INSERT ON sf_contact_assignee_temp
  FOR EACH ROW
  DECLARE
    n_id NUMBER;
    seq_name VARCHAR2(50);
  BEGIN
    seq_name := 'sf_contact_assignee_temp_SQ' || '.NEXTVAL';
    EXECUTE IMMEDIATE 'SELECT ' || seq_name || ' FROM DUAL' INTO n_id;
    :new.ID := n_id;
  END;
/