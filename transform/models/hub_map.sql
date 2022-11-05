CREATE TABLE
IF NOT EXISTS HUB_MAP AS
SELECT *
FROM STG_ZILLOW_IDX zill
    JOIN (SELECT zip_tract.zip, opp.kfr_rp_gp_pall as child_income
    FROM STG_OPP_ATLAS opp
        JOIN STG_ZIP_TRACT zip_tract
        ON opp.tract = zip_tract.tract) opp
    ON opp.zip = zill.regionid
    JOIN STG_LONG_LAT long
    ON zill.regionid = long.zip;

ALTER TABLE HUB_MAP DROP regionid;
ALTER TABLE HUB_MAP DROP "zip:1";