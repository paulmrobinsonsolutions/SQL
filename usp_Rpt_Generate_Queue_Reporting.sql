/*==========================================================
Description: Generate specific process/queue reports based on their
very on process/queue data which all have very different columns and
number of columns for their respective data. The cool thing... instead
of 1000 different queries to extract the queue-specific data we can
simply use dynamic sql to build the select statements. Then some cool
xml data type parsing with an unpivot (the coolest) to return all of 
the columns specific to the queue with no extra work. Pretty slick!
============================================================

------   Change Log   ------
DATE     NAME    DESCRIPTION
------------------------------------------------------------

------------------------------------------------------------
DECLARE @QueueName NVARCHAR(MAX) = 'Some_Queue_Name';
DECLARE @FromDate DATE = DATEADD(DAY, -10, GETDATE());
DECLARE @ToDate DATE = GETDATE()-1;

EXEC [dbo].[usp_GenerateReport_ByQueueAndDateRange] @FromDate, @ToDate, @BizArea

============================================================*/
ALTER PROCEDURE [dbo].[usp_GenerateReport_ByQueueAndDateRange]
	@QueueName NVARCHAR(MAX) = NULL,
	@FromDate DATETIME = NULL,
	@ToDate DATETIME = NULL
AS
BEGIN
   SET NOCOUNT ON;

	/* Quick and dirty way to get utc time difference from
	   another table which has this key field. Note that this
	   will return the record that has the most current time
	   basedf on the [StartDatetime] field */
	DECLARE @UtcHourDiff INT =
		(SELECT TOP 1 StartTimeZoneOffset / 60 / 60
		   FROM dbo.BPVSessionInfo si WITH (NOLOCK)
		  WHERE si.LastUpdated < @ToDate
		  ORDER BY StartDatetime DESC)
	;

	/*
	1. Get the first row of the set of data for the queue and time period we want
	2. Extract the XML column names from the XML per Column_1, Column_2, Column_n, etc.
	3. Flip the result column names into rows to then use with a cursor further down
	4. Put the result columns into a cursor to then work through further down
	   to build out actual SQL to query for all data
	*/
	DECLARE columnCursor CURSOR FOR
	SELECT unp.ColumnName
	  FROM (
			SELECT TOP 1 CAST([Data] AS XML).value('(/collection/row/field/@name)[1]', 'nvarchar(max)') AS Column_1
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[2]', 'nvarchar(max)') AS Column_2
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[3]', 'nvarchar(max)') AS Column_3
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[4]', 'nvarchar(max)') AS Column_4
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[5]', 'nvarchar(max)') AS Column_5
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[6]', 'nvarchar(max)') AS Column_6
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[7]', 'nvarchar(max)') AS Column_7
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[8]', 'nvarchar(max)') AS Column_8
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[9]', 'nvarchar(max)') AS Column_9
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[10]', 'nvarchar(max)') AS Column_10
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[11]', 'nvarchar(max)') AS Column_11
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[12]', 'nvarchar(max)') AS Column_12
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[13]', 'nvarchar(max)') AS Column_13
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[14]', 'nvarchar(max)') AS Column_14
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[15]', 'nvarchar(max)') AS Column_15
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[16]', 'nvarchar(max)') AS Column_16
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[17]', 'nvarchar(max)') AS Column_17
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[18]', 'nvarchar(max)') AS Column_18
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[19]', 'nvarchar(max)') AS Column_19
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[20]', 'nvarchar(max)') AS Column_20
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[21]', 'nvarchar(max)') AS Column_21
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[22]', 'nvarchar(max)') AS Column_22
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[23]', 'nvarchar(max)') AS Column_23
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[24]', 'nvarchar(max)') AS Column_24
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[25]', 'nvarchar(max)') AS Column_25
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[26]', 'nvarchar(max)') AS Column_26
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[27]', 'nvarchar(max)') AS Column_27
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[28]', 'nvarchar(max)') AS Column_28
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[29]', 'nvarchar(max)') AS Column_29
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[30]', 'nvarchar(max)') AS Column_30
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[31]', 'nvarchar(max)') AS Column_31
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[32]', 'nvarchar(max)') AS Column_32
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[33]', 'nvarchar(max)') AS Column_33
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[34]', 'nvarchar(max)') AS Column_34
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[35]', 'nvarchar(max)') AS Column_35
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[36]', 'nvarchar(max)') AS Column_36
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[37]', 'nvarchar(max)') AS Column_37
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[38]', 'nvarchar(max)') AS Column_38
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[39]', 'nvarchar(max)') AS Column_39
				 , CAST([Data] AS XML).value('(/collection/row/field/@name)[40]', 'nvarchar(max)') AS Column_40
			  FROM [dbo].[BPAWorkQueue] AS A WITH (NOLOCK)
					  INNER JOIN [dbo].[BPAWorkQueueItem] AS B WITH (NOLOCK)
						 ON A.id = B.queueid
			 WHERE A.[Name] = @QueueName
			   AND DATEADD(HOUR, @UtcHourDiff, B.Finished) BETWEEN @FromDate AND @ToDate
		 ) dt
	/* 
	Flip all columns into rows
	NOTE: THE UNPIVOT WILL AUTOMATICALLY REMOVE ANY 'NULL' VALUES. LESS WORK, GOOD STUFF!
	*/
	UNPIVOT
	(
	  ColumnName for c IN (Column_1,Column_2,Column_3,Column_4,Column_5,Column_6,Column_7,Column_8, Column_9,Column_10
						  ,Column_11,Column_12,Column_13,Column_14,Column_15,Column_16,Column_17,Column_18, Column_19,Column_20
				  ,Column_21,Column_22,Column_23,Column_24,Column_25,Column_26,Column_27,Column_28,Column_29,Column_30
						  ,Column_31,Column_32,Column_33,Column_34,Column_35,Column_36,Column_37,Column_38,Column_39,Column_40)
	) unp

	DECLARE @RowIdx INT = 1
	DECLARE @TempColumnName NVARCHAR(MAX)
	DECLARE @sql NVARCHAR(MAX) = 'SELECT '
  
	OPEN columnCursor

	FETCH NEXT FROM columnCursor
	INTO @TempColumnName

	-- If no data is returned then return some relavent message
	IF ISNULL(@TempColumnName, '') = ''
		SELECT 'No data was returned for the specified parameters.' AS NoDataMsg;
	ELSE
	BEGIN
		/* 5. Loop through cursor data to build out xml query string to pull out values from data */
		WHILE @@FETCH_STATUS = 0
		BEGIN
			IF @RowIdx > 1
				SET @sql = @sql + '
			 , '
			SET @sql = @sql + 'CAST([Data] AS XML).value(''(/collection/row/field/@value)['+ CONVERT(NVARCHAR(MAX), @RowIdx) 
								+']'', ''nvarchar(max)'') AS ['+ @TempColumnName +']'

			-- Increment row index to account for next column
			SET @RowIdx = @RowIdx + 1;

			FETCH NEXT FROM columnCursor
			INTO @TempColumnName
		END

		SET @sql = @sql + '
			, CASE WHEN B.status = '''' THEN ''Completed'' ELSE B.status END AS [Result]
			, CASE WHEN B.exceptionreasonvarchar IS NULL THEN '''' ELSE B.exceptionreasonvarchar END AS [Exception Detail]
			FROM [dbo].[BPAWorkQueue] AS A WITH (NOLOCK)
				INNER JOIN [dbo].[BPAWorkQueueItem] AS B WITH (NOLOCK)
					ON A.id = B.queueid
		WHERE A.[Name] = '''+ @QueueName +'''
			AND DATEADD(HOUR, '+ CONVERT(NVARCHAR(2), @UtcHourDiff) +', B.Finished) BETWEEN '''+ CONVERT(NVARCHAR(MAX), @FromDate, 120) +''' AND '''+ CONVERT(NVARCHAR(MAX), @ToDate, 120) +''''
		;

		/*	6. Execute the actual sql to get results */
		--PRINT @sql
		EXEC sp_executesql @sql;
	END

	/* 7. MUST ALWAYS close and deallocate cursors or resources get hosed up after awhile */
	CLOSE columnCursor;
	DEALLOCATE columnCursor;

	/* 8. Get the results to do what's needed, then go get a coffee or take a walk. You deserve it! */

END