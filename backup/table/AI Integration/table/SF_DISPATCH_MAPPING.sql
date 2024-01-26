alter table SF_DISPATCH_MAPPING drop constraint ref_sf_dis_map_fk1;

/

alter table jam.SF_DISPATCH_MAPPING add RECORD_TYPE_NAME VARCHAR2(50);