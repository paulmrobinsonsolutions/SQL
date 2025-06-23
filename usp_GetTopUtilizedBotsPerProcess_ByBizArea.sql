/*==========================================================
Description: Get the top N active bots for each process under a given 
Business Area between a specified date range. This will return the 
total number of process runs and last start time for a bit more insight.
============================================================

------   Change Log   ------
DATE     NAME    DESCRIPTION
------------------------------------------------------------

------------------------------------------------------------

DECLARE @FromDate DATE = DATEADD(DAY, -10, GETDATE());
DECLARE @FromDate DATE = GETDATE()-1;
DECLARE @FromDate DATE = 'AMO';

EXEC [dbo].[usp_GetTopUtilizedBotsPerProcess_ByBizArea] @FromDate, @ToDate, @BizArea

============================================================*/
ALTER PROCEDURE [dbo].[usp_GetTopUtilizedBotsPerProcess_ByBizArea]
	@FromDate DATE = NULL,
	@To DATE = NULL,
	@BizArea NVARCHAR(10) = 'FIN'
AS
BEGIN
   SET NOCOUNT ON;
   
   /* A lot of things are going to come together with this table.
      The data from teh various joins will be put in this table
	  then the end results will pull from this table. */
	IF OBJECT_ID('tempdb..#TmpTbl') IS NOT NULL
		DROP TABLE #TmpTbl
	;

	/* Based on particular choice of folder structure, the 'Business Area'
	   is at the top level or top 'Group' per database terminology. This is
	   key to be able to filter data out based on @BizArea input parameter. */
	WITH CTE AS
	(
	SELECT ProcessName
	  FROM (
		SELECT ISNULL(gLvl2.[Name], g.[Name]) AS TopLvl_GroupName
			 , p.[Name] AS ProcessName
		  FROM dbo.BPAProcess p WITH (NOLOCK)
				  INNER JOIN dbo.BPAGroupProcess gp WITH (NOLOCK)
					 ON gp.ProcessId = p.ProcessId
				  INNER JOIN dbo.BPAGroup g WITH (NOLOCK)
					 ON g.Id = gp.GroupId
				   LEFT OUTER JOIN dbo.BPAGroupGroup gg WITH (NOLOCK)
					 ON gg.MemberId = gp.GroupId
				   LEFT OUTER JOIN dbo.BPAGroup gLvl2 WITH (NOLOCK)
					 ON gg.GroupId = gLvl2.Id
		 WHERE ProcessType = 'P'
		 ) dt
	/* Limit to a specified Business Area since it's highly
	   unlikely to receive a request across Business Areas. */
	WHERE TopLvl_GroupName = @BizArea
	)

	SELECT ProcessName
		 , CONVERT(DATE, StartDatetime) AS StartTime
		 , RuntimeSeconds
		 /* Get the last bot to log in to a particular machine.
		    Subquery to get the last session login it based on
			the last successful 'Login By Network Id' process. */
		 , (SELECT TOP 1 CAST(StartParamsXml AS XML).value('(/inputs/input/@value)[1]', 'nvarchar(max)')  AS BotId
			  FROM BPVSessionInfo l
			 WHERE l.ProcessName = 'Login By Network ID'
			   AND l.RunningResourceID = dt.RunningResourceId
			 ORDER BY StartDatetime DESC
			) AS LoggedInBot
		 , RowNum
	INTO #TmpTbl
	FROM (
		SELECT p.[Name] AS ProcessName
			 , sess.SessionNumber
			 , sess.StartDatetime
			 , sess.EndDatetime
			 , DATEDIFF(SECOND, sess.StartDatetime, sess.EndDatetime) AS WorkTimeSeconds
			 , sess.RunningResourceId
			 , ROW_NUMBER() OVER(PARTITION BY p.[Name], CONVERT(DATE, sess.StartDatetime)
									 ORDER BY sess.StartDatetime) AS RowNum
		  FROM dbo.BPASession sess WITH (NOLOCK)
				INNER JOIN dbo.BPAProcess p WITH (NOLOCK)
				   ON p.processid = sess.ProcessId
		WHERE sess.EndDatetime BETWEEN @FromDate AND @ToDate
		  AND p.[Name] IN (SELECT ProcessName
							 FROM CTE)
		) dt
	WHERE RowNum <= 2
	;

	/* Return the row with the most counts for a particular process/bot
	   to identify the "main worker bot", if you will, for each process.
	   TotalWorkTimeSeconds may also be of interest... */
	SELECT ProcessName
		 , LoggedInBot
		 , TotalNumberOfRuns
		 , FirstRun_InDatRange
		 , LastRun_InDatRange
	  FROM (
			SELECT ProcessName
				 , LoggedInBot
				 , SUM(RowNum) AS TotalNumberOfRuns
				 , SUM(WorkTimeSeconds) AS TotalWorkTimeSeconds
				 , MIN(StartTime) AS FirstRun_InDatRange
				 , MAX(StartTime) AS LastRun_InDatRange
				 , ROW_NUMBER() OVER(PARTITION BY ProcessName
										 ORDER BY SUM(RowNum) DESC) AS RowNum
			  FROM #TmpTbl
			 GROUP BY ProcessName
				 , LoggedInBot
		  ) dt
	 /* Return top 3 "worker" bots per process */
	 WHERE RowNum <= 3
	 ORDER BY ProcessName
		 , RowNum
	;

END