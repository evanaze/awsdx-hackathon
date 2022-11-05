WITH
    opportunity
    AS
    (
        SELECT zip_tract.zip, opp.kfr_rp_gp_pall as child_income
        FROM STG_OPP_ATLAS opp
            JOIN STG_ZIP_TRACT zip_tract
            ON opp.tract = zip_tract.tract
    )

CREATE TABLE HUB_MAP AS
SELECT *
FROM STG_ZILLOW_IDX zill
    JOIN opportunity opp
    ON opp.zip = zill.regionid;

ALTER TABLE HUB_MAP DROP regionid;