/*============================================================
Description: Evaluate if the incoming date falls on a holiday, or not.
The intent of this is to support a scheduling process where work can
only be scheduled on business days.
============================================================

------   Change Log   ------
DATE     NAME    DESCRIPTION
------------------------------------------------------------


============================================================*/
CREATE FUNCTION [dbo].[fn_GetDateIsolidayCheck]
(
    @DateToEval DATE = NULL
)
RETURNS CHAR(1)
AS
BEGIN
    -- Set result to 'No' by default
    DECLARE @ResultValue CHAR(1) = 'N';

    DECLARE @Year CHAR(4) = NULL;
    DECLARE @Date DATETIME = NULL;
    DECLARE @HolidayDate DATETIME = NULL;

    DECLARE @HolidaysTable TABLE
    (
        HolidayDate DATE,
        HolidayName NVARCHAR(MAX),
        DayOfWeekName NVARCHAR(15)
    );

    SET @Year = YEAR(@DateToEval);

    /* New Years */
    SET @Date = CONVERT(DATETIME, CONVERT(VARCHAR, YEAR(@Year)) + '-01-01');
    IF (DATENAME(dw, @Date) = 'Saturday')
        SET @Date = @Date - 1
    ELSE IF (DATENAME(dw, @Date) = 'Sunday')
        SET @Date = @Date + 1
    ; 
    INSERT INTO @HolidaysTable
    VALUES (@Date, 'New Years Day', DATENAME(dw, @Date));

    /* Martin L. King Jr's BDay (3rd Monday in January) */
    SET @Date = CONVERT(DATETIME, CONVERT(VARCHAR, YEAR(@Year)) + '-01-01');
    SET @HolidayDate = DATEADD(wk, DATEDIFF(wk, 0, DATEADD(dd, 18 - DATEPART(DAY, @Date), @Date)), 0) -- 3rd Monday of month
    INSERT INTO @HolidaysTable
    VALUES (@HolidayDate, 'Martin Luther King Jr BDay', DATENAME(dw, @HolidayDate));

    /* President's Day (3rd Monday in January) */
    SET @Date = CONVERT(DATETIME, CONVERT(VARCHAR, YEAR(@Year)) + '-02-01');
    SET @HolidayDate = DATEADD(wk, DATEDIFF(wk, 0, DATEADD(dd, 18 - DATEPART(DAY, @Date), @Date)), 0) -- 3rd Monday of month
    INSERT INTO @HolidaysTable
    VALUES (@HolidayDate, 'Presidents Day', DATENAME(dw, @HolidayDate));

    /* Memorial Day (Last Monday in May) */
    SET @Date = CONVERT(DATETIME, CONVERT(VARCHAR, YEAR(@Year)) + '-05-01');
    SET @HolidayDate = DATEADD(wk, DATEDIFF(wk, 0, DATEADD(dd, 18 - DATEPART(DAY, @Date), @Date)), 0) -- 5th Monday of month
    INSERT INTO @HolidaysTable
    VALUES (@HolidayDate, 'Memorial Day', DATENAME(dw, @HolidayDate));

    /* Independence Day (July 4th) */
    SET @Date = CONVERT(DATETIME, CONVERT(VARCHAR, YEAR(@Year)) + '-07-04');
    IF (DATENAME(dw, @Date) = 'Saturday')
        SET @Date = @Date - 1
    ELSE IF (DATENAME(dw, @Date) = 'Sunday')
        SET @Date = @Date + 1
    INSERT INTO @HolidaysTable
    VALUES (@Date, 'Independence Day', DATENAME(dw, @Date));

    /* Labor Day (1st Monday in September) */
    SET @Date = CONVERT(DATETIME, CONVERT(VARCHAR, YEAR(@Year)) + '-09-01');
    SET @HolidayDate = DATEADD(wk, DATEDIFF(wk, 0, DATEADD(dd, 6 - DATEPART(DAY, @Date), @Date)), 0) -- 1st Monday of month
    INSERT INTO @HolidaysTable
    VALUES (@HolidayDate, 'Memorial Day', DATENAME(dw, @HolidayDate));

    /* Columbus Day (2nd Monday in October) */
    SET @Date = CONVERT(DATETIME, CONVERT(VARCHAR, YEAR(@Year)) + '-10-01');
    SET @HolidayDate = DATEADD(wk, DATEDIFF(wk, 0, DATEADD(dd, 12 - DATEPART(DAY, @Date), @Date)), 0) -- 2wd Monday of month
    INSERT INTO @HolidaysTable
    VALUES (@HolidayDate, 'Columbus Day', DATENAME(dw, @HolidayDate));

    /* Veteran's Day (November 11th) */
    SET @Date = CONVERT(DATETIME, CONVERT(VARCHAR, YEAR(@Year)) + '-11-11');
    IF (DATENAME(dw, @Date) = 'Saturday')
        SET @Date = @Date - 1
    ELSE IF (DATENAME(dw, @Date) = 'Sunday')
        SET @Date = @Date + 1
    INSERT INTO @HolidaysTable
    VALUES (@Date, 'Veterans Day', DATENAME(dw, @Date));

    /* Thanksgiving Day (4th Thursday in November) */
    SET @Date = CONVERT(DATETIME, CONVERT(VARCHAR, YEAR(@Year)) + '-11-04');
    SET @HolidayDate = DATEADD(wk, DATEDIFF(wk, 0, DATEADD(dd, 22 - DATEPART(DAY, @Date), @Date)), 0) + 3 -- 2wd Monday of month
    INSERT INTO @HolidaysTable
    VALUES (@HolidayDate, 'Thanksgiving Day', DATENAME(dw, @HolidayDate));

    /* Christmas Day (December 25th) */
    SET @Date = CONVERT(DATETIME, CONVERT(VARCHAR, YEAR(@Year)) + '-12-25');
    IF (DATENAME(dw, @Date) = 'Saturday')
        SET @Date = @Date - 1
    ELSE IF (DATENAME(dw, @Date) = 'Sunday')
        SET @Date = @Date + 1
    INSERT INTO @HolidaysTable
    VALUES (@Date, 'Christmas Day', DATENAME(dw, @Date));

    /* New Years Eve */
    SET @Date = CONVERT(DATETIME, CONVERT(VARCHAR, YEAR(@Year)) + '-12-31');
    IF (DATENAME(dw, @Date) = 'Saturday')
        SET @Date = @Date - 1
    ELSE IF (DATENAME(dw, @Date) = 'Sunday')
        SET @Date = @Date + 1
    INSERT INTO @HolidaysTable
    VALUES (@Date, 'New Years Eve', DATENAME(dw, @Date));
    
    /* Return only the first result.
       1. If a date match was made then 'Y' (Yes) is returned
       2. If a date was NOT matched, nothing was added to @HolidaysTable so 'N' (No) is returned */
       SET @ResultValue =
           (SELECT TOP 1 ResultValue
              FROM (
                  SELECT 'Y' AS ResultValue
                    FROM @HolidaysTable
                   WHERE @DateToEval = HolidayDate
                  UNION ALL
                  SELECT 'N'
              ) dt
           )

    SELECT @ResultValue;

END