-- ============================================================
-- SP_BUILD_DIM_DATES.sql
-- Builds the date spine from 2013-01-01 to 2030-12-31
-- Includes US federal + NY state holidays via hardcoded table
-- Writes to INT_UDM_NYC.DIM_DATES and clones to INT_UDM_JC
-- Run once; re-runnable via TRUNCATE + reload
-- ============================================================
 
CREATE OR REPLACE PROCEDURE CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.SP_BUILD_DIM_DATES(
    P_START_DATE DATE DEFAULT '2013-01-01',
    P_END_DATE   DATE DEFAULT '2030-12-31'
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_inserted INTEGER DEFAULT 0;
    V_START_DATE  DATE;
    V_END_DATE    DATE;
BEGIN
    -- Bind parameters to local variables so they resolve inside SQL statements
    V_START_DATE := P_START_DATE;
    V_END_DATE := P_END_DATE;
    -- ------------------------------------------------------------
    -- Clear and rebuild (idempotent)
    -- ------------------------------------------------------------
    TRUNCATE TABLE CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.DIM_DATES;
    -- ------------------------------------------------------------
    -- Step 1: Build holiday lookup (US Federal + NY State)
    -- Covers 2013–2030. Extend as needed.
    -- ------------------------------------------------------------
    CREATE OR REPLACE TEMPORARY TABLE TEMP_HOLIDAYS AS
    SELECT DATE_VAL::DATE AS HOLIDAY_DATE, HOLIDAY_NAME
    FROM VALUES
        -- New Year's Day
        ('2013-01-01','New Years Day'),('2014-01-01','New Years Day'),
        ('2015-01-01','New Years Day'),('2016-01-01','New Years Day'),
        ('2017-01-02','New Years Day (observed)'),('2018-01-01','New Years Day'),
        ('2019-01-01','New Years Day'),('2020-01-01','New Years Day'),
        ('2021-01-01','New Years Day'),('2022-01-17','New Years Day (observed)'),
        ('2023-01-02','New Years Day (observed)'),('2024-01-01','New Years Day'),
        ('2025-01-01','New Years Day'),('2026-01-01','New Years Day'),
        ('2027-01-01','New Years Day'),('2028-01-01','New Years Day (observed)'),
        ('2029-01-01','New Years Day'),('2030-01-01','New Years Day'),
        -- MLK Day (3rd Monday Jan)
        ('2013-01-21','MLK Day'),('2014-01-20','MLK Day'),('2015-01-19','MLK Day'),
        ('2016-01-18','MLK Day'),('2017-01-16','MLK Day'),('2018-01-15','MLK Day'),
        ('2019-01-21','MLK Day'),('2020-01-20','MLK Day'),('2021-01-18','MLK Day'),
        ('2022-01-17','MLK Day'),('2023-01-16','MLK Day'),('2024-01-15','MLK Day'),
        ('2025-01-20','MLK Day'),('2026-01-19','MLK Day'),('2027-01-18','MLK Day'),
        ('2028-01-17','MLK Day'),('2029-01-15','MLK Day'),('2030-01-21','MLK Day'),
        -- Presidents Day (3rd Monday Feb)
        ('2013-02-18','Presidents Day'),('2014-02-17','Presidents Day'),
        ('2015-02-16','Presidents Day'),('2016-02-15','Presidents Day'),
        ('2017-02-20','Presidents Day'),('2018-02-19','Presidents Day'),
        ('2019-02-18','Presidents Day'),('2020-02-17','Presidents Day'),
        ('2021-02-15','Presidents Day'),('2022-02-21','Presidents Day'),
        ('2023-02-20','Presidents Day'),('2024-02-19','Presidents Day'),
        ('2025-02-17','Presidents Day'),('2026-02-16','Presidents Day'),
        ('2027-02-15','Presidents Day'),('2028-02-21','Presidents Day'),
        ('2029-02-19','Presidents Day'),('2030-02-18','Presidents Day'),
        -- Memorial Day (last Monday May)
        ('2013-05-27','Memorial Day'),('2014-05-26','Memorial Day'),
        ('2015-05-25','Memorial Day'),('2016-05-30','Memorial Day'),
        ('2017-05-29','Memorial Day'),('2018-05-28','Memorial Day'),
        ('2019-05-27','Memorial Day'),('2020-05-25','Memorial Day'),
        ('2021-05-31','Memorial Day'),('2022-05-30','Memorial Day'),
        ('2023-05-29','Memorial Day'),('2024-05-27','Memorial Day'),
        ('2025-05-26','Memorial Day'),('2026-05-25','Memorial Day'),
        ('2027-05-31','Memorial Day'),('2028-05-29','Memorial Day'),
        ('2029-05-28','Memorial Day'),('2030-05-27','Memorial Day'),
        -- Juneteenth (June 19, from 2021)
        ('2021-06-19','Juneteenth'),('2022-06-19','Juneteenth'),
        ('2023-06-19','Juneteenth'),('2024-06-19','Juneteenth'),
        ('2025-06-19','Juneteenth'),('2026-06-19','Juneteenth'),
        ('2027-06-18','Juneteenth (observed)'),('2028-06-19','Juneteenth'),
        ('2029-06-19','Juneteenth'),('2030-06-19','Juneteenth'),
        -- Independence Day (July 4)
        ('2013-07-04','Independence Day'),('2014-07-04','Independence Day'),
        ('2015-07-04','Independence Day (observed)'),('2016-07-04','Independence Day'),
        ('2017-07-04','Independence Day'),('2018-07-04','Independence Day'),
        ('2019-07-04','Independence Day'),('2020-07-03','Independence Day (observed)'),
        ('2021-07-05','Independence Day (observed)'),('2022-07-04','Independence Day'),
        ('2023-07-04','Independence Day'),('2024-07-04','Independence Day'),
        ('2025-07-04','Independence Day'),('2026-07-03','Independence Day (observed)'),
        ('2027-07-05','Independence Day (observed)'),('2028-07-04','Independence Day'),
        ('2029-07-04','Independence Day'),('2030-07-04','Independence Day'),
        -- Labor Day (1st Monday Sep)
        ('2013-09-02','Labor Day'),('2014-09-01','Labor Day'),('2015-09-07','Labor Day'),
        ('2016-09-05','Labor Day'),('2017-09-04','Labor Day'),('2018-09-03','Labor Day'),
        ('2019-09-02','Labor Day'),('2020-09-07','Labor Day'),('2021-09-06','Labor Day'),
        ('2022-09-05','Labor Day'),('2023-09-04','Labor Day'),('2024-09-02','Labor Day'),
        ('2025-09-01','Labor Day'),('2026-09-07','Labor Day'),('2027-09-06','Labor Day'),
        ('2028-09-04','Labor Day'),('2029-09-03','Labor Day'),('2030-09-02','Labor Day'),
        -- Columbus Day / Indigenous Peoples Day (2nd Monday Oct)
        ('2013-10-14','Columbus Day'),('2014-10-13','Columbus Day'),
        ('2015-10-12','Columbus Day'),('2016-10-10','Columbus Day'),
        ('2017-10-09','Columbus Day'),('2018-10-08','Columbus Day'),
        ('2019-10-14','Columbus Day'),('2020-10-12','Columbus Day'),
        ('2021-10-11','Columbus Day'),('2022-10-10','Columbus Day'),
        ('2023-10-09','Columbus Day'),('2024-10-14','Columbus Day'),
        ('2025-10-13','Columbus Day'),('2026-10-12','Columbus Day'),
        ('2027-10-11','Columbus Day'),('2028-10-09','Columbus Day'),
        ('2029-10-08','Columbus Day'),('2030-10-14','Columbus Day'),
        -- Veterans Day (Nov 11)
        ('2013-11-11','Veterans Day'),('2014-11-11','Veterans Day'),
        ('2015-11-11','Veterans Day'),('2016-11-11','Veterans Day'),
        ('2017-11-10','Veterans Day (observed)'),('2018-11-12','Veterans Day (observed)'),
        ('2019-11-11','Veterans Day'),('2020-11-11','Veterans Day'),
        ('2021-11-11','Veterans Day'),('2022-11-11','Veterans Day'),
        ('2023-11-10','Veterans Day (observed)'),('2024-11-11','Veterans Day'),
        ('2025-11-11','Veterans Day'),('2026-11-11','Veterans Day'),
        ('2027-11-11','Veterans Day'),('2028-11-10','Veterans Day (observed)'),
        ('2029-11-12','Veterans Day (observed)'),('2030-11-11','Veterans Day'),
        -- Thanksgiving (4th Thursday Nov)
        ('2013-11-28','Thanksgiving'),('2014-11-27','Thanksgiving'),
        ('2015-11-26','Thanksgiving'),('2016-11-24','Thanksgiving'),
        ('2017-11-23','Thanksgiving'),('2018-11-22','Thanksgiving'),
        ('2019-11-28','Thanksgiving'),('2020-11-26','Thanksgiving'),
        ('2021-11-25','Thanksgiving'),('2022-11-24','Thanksgiving'),
        ('2023-11-23','Thanksgiving'),('2024-11-28','Thanksgiving'),
        ('2025-11-27','Thanksgiving'),('2026-11-26','Thanksgiving'),
        ('2027-11-25','Thanksgiving'),('2028-11-23','Thanksgiving'),
        ('2029-11-22','Thanksgiving'),('2030-11-28','Thanksgiving'),
        -- Christmas Day (Dec 25)
        ('2013-12-25','Christmas Day'),('2014-12-25','Christmas Day'),
        ('2015-12-25','Christmas Day'),('2016-12-26','Christmas Day (observed)'),
        ('2017-12-25','Christmas Day'),('2018-12-25','Christmas Day'),
        ('2019-12-25','Christmas Day'),('2020-12-25','Christmas Day'),
        ('2021-12-24','Christmas Day (observed)'),('2022-12-26','Christmas Day (observed)'),
        ('2023-12-25','Christmas Day'),('2024-12-25','Christmas Day'),
        ('2025-12-25','Christmas Day'),('2026-12-25','Christmas Day'),
        ('2027-12-24','Christmas Day (observed)'),('2028-12-25','Christmas Day'),
        ('2029-12-25','Christmas Day'),('2030-12-25','Christmas Day')
    AS T(DATE_VAL, HOLIDAY_NAME);
 
    -- ------------------------------------------------------------
    -- Step 2: Generate date spine and populate DIM_DATES
    -- ------------------------------------------------------------
    INSERT INTO CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.DIM_DATES
    WITH DATE_SPINE AS (
        SELECT
            DATEADD(DAY, SEQ4(), :V_START_DATE) AS FULL_DATE
        FROM TABLE(GENERATOR(ROWCOUNT => 6575))  -- covers ~18 years
        WHERE DATEADD(DAY, SEQ4(), :V_START_DATE) <= :V_END_DATE
    )
    SELECT
        TO_NUMBER(TO_CHAR(FULL_DATE, 'YYYYMMDD'))           AS DATE_SK,
        FULL_DATE,
 
        -- Day
        DAYOFWEEKISO(FULL_DATE)                             AS DAY_OF_WEEK_NUM,
        DAYNAME(FULL_DATE)                                  AS DAY_OF_WEEK_NAME,
        LEFT(DAYNAME(FULL_DATE), 3)                         AS DAY_OF_WEEK_SHORT,
        DAYOFWEEKISO(FULL_DATE) IN (6, 7)                   AS IS_WEEKEND,
        DAY(FULL_DATE)                                      AS DAY_OF_MONTH,
        DAYOFYEAR(FULL_DATE)                                AS DAY_OF_YEAR,
 
        -- Week / Month / Quarter / Year
        WEEKISO(FULL_DATE)                                  AS WEEK_OF_YEAR,
        MONTH(FULL_DATE)                                    AS MONTH_NUM,
        MONTHNAME(FULL_DATE)                                AS MONTH_NAME,
        LEFT(MONTHNAME(FULL_DATE), 3)                       AS MONTH_SHORT,
        QUARTER(FULL_DATE)                                  AS QUARTER_NUM,
        YEAR(FULL_DATE)                                     AS YEAR_NUM,
        TO_CHAR(FULL_DATE, 'YYYY-MM')                       AS YEAR_MONTH,
 
        -- Season (meteorological)
        CASE MONTH(FULL_DATE)
            WHEN 3  THEN 'Spring' WHEN 4  THEN 'Spring' WHEN 5  THEN 'Spring'
            WHEN 6  THEN 'Summer' WHEN 7  THEN 'Summer' WHEN 8  THEN 'Summer'
            WHEN 9  THEN 'Autumn' WHEN 10 THEN 'Autumn' WHEN 11 THEN 'Autumn'
            ELSE 'Winter'
        END                                                 AS SEASON,
        
        -- Holiday
        CASE WHEN H.HOLIDAY_DATE IS NOT NULL THEN TRUE ELSE FALSE END   AS IS_HOLIDAY,
        H.HOLIDAY_NAME,
 
        -- Month boundaries
        DATE_TRUNC('MONTH', FULL_DATE)::DATE                AS FIRST_DAY_OF_MONTH,
        LAST_DAY(FULL_DATE)::DATE                           AS LAST_DAY_OF_MONTH,
        DATE_TRUNC('YEAR', FULL_DATE)::DATE                 AS FIRST_DAY_OF_YEAR,
        DATEADD('YEAR', 1, DATE_TRUNC('YEAR', FULL_DATE)) - 1  AS LAST_DAY_OF_YEAR
 
    FROM DATE_SPINE
    LEFT JOIN TEMP_HOLIDAYS H ON H.HOLIDAY_DATE = FULL_DATE
    ORDER BY FULL_DATE;
 
    rows_inserted := SQLROWCOUNT;
 
    -- ------------------------------------------------------------
    -- Step 3: Sync to JC schema (same date spine, no differences)
    -- ------------------------------------------------------------
    TRUNCATE TABLE CITIBIKE_SYSTEM_DATA.INT_UDM_JC.DIM_DATES;
    INSERT INTO CITIBIKE_SYSTEM_DATA.INT_UDM_JC.DIM_DATES
    SELECT * FROM CITIBIKE_SYSTEM_DATA.INT_UDM_NYC.DIM_DATES;
    RETURN 'SP_BUILD_DIM_DATES: inserted ' || rows_inserted || ' rows into INT_UDM_NYC.DIM_DATES and synced to INT_UDM_JC.DIM_DATES';
 
END;
$$;