alter table SF_SITE add STATE VARCHAR2(50);

/

UPDATE sf_site ss
SET
    state = (
        SELECT
            location_addr.st
        FROM
            cmn_location_addr  location_addr,
            cmn_site_contact   site_contact,
            cmn_addr           addr
        WHERE
                site_contact.addr_id = addr.addr_id
            AND site_contact.addr_id = location_addr.location_addr_id
            AND site_contact.site_id = ss.mss_site_id
            AND ROWNUM = 1
    );