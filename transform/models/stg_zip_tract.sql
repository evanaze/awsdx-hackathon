CREATE TABLE STG_ZIP_TRACT AS
SELECT rtrim(ltrim(zip, '="'), '"') AS zip, rtrim(ltrim(tract, '="'), '"') AS tract,
    bus_ratio, oth_ratio, res_ratio, tot_ratio
FROM IN_ZIP_TRACT;
