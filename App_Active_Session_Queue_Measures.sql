/*
Get live sessions (currently running) with details to project when each
process is estimated to complete. This can be calculated based on the 
number of pending/remaining work queue items, the number of machines/bots
processing a particular work queue's items, and time it takes to process
each item. Other very good to know data points to determine the health of
a process is included as well. Like number of items completed, number of
item failures (errors/exceptions), number of items remaining and average 
worktime (AHT) of items. This will enable one to project the estimated
completion time of each running process. Because knowing is half the battle.
============================================================

------   Change Log   ------
DATE     NAME    DESCRIPTION
------------------------------------------------------------
01/18/22 Paul R. Added more cool stuff.

============================================================*/
CREATE PROCEDURE [dbo].[usp_GetLiveSessions_WithQueueMeasures]

AS
BEGIN
	SET NOCOUNT ON;

	IF OBJECT_ID('tempdb..#QueueAverages') IS NOT NULL
		  DROP TABLE #QueueAverages;

	/* The underlying VW_ view needs to be updated but requires SNOW request
	   so in the meantime we'll go with this ugly join... 
   
	** VW_RPA_QueueToProcessLink is a custom view, NOT part of the application sql. The purpose
	   of this view was to create a link between a 'Process' (BPAProcess table) and it's related
	   'Work Queue (BPAWorkQueue table) in order to join this data for analysis. */
	SELECT vqpl.QueueId
		 , vqpl.QueueName
		 , qpl.ItemToItemProcessTimeSeconds AS AvgSeconds
	INTO #QueueAverages
	  FROM RPA_QueueToProcessLink qpl WITH (NOLOCK)
			  INNER JOIN VW_RPA_QueueToProcessLink vqpl
				ON qpl.QueueName = vqpl.QueueName
	;

	/* Get actively running sessions data only using temp table to quickly
	   return only the data columns needed to join data later on. */
	IF OBJECT_ID('tempdb..#CurrRunningSessions') IS NOT NULL
		  DROP TABLE #CurrRunningSessions;

	SELECT si.ProcessId
		 , si.SessionNumber
		 , si.SessionId
		 , si.RunningResourceName
		 , ISNULL(CONVERT(NVARCHAR(MAX), si.QueueId), l.QueueId) AS QueueId
	 INTO #CurrRunningSessions
	 FROM BPVSessionInfo si WITH (NOLOCK)
		   INNER JOIN dbo.VW_RPA_QueueToProcessLink l WITH (NOLOCK)
			  ON si.ProcessId = l.ProcessId
	WHERE RIGHT(si.RunningResourceName, 5) <> 'debug'  -- Exclude any debug runs
	  AND si.StatusId = 1
	  AND ISDATE(si.enddatetime) = 0
	;

	/* Get queue item detail data only using temp table to quickly
	   return only the data columns needed to join data later on. */
	IF OBJECT_ID('tempdb..#QueueItemDetail') IS NOT NULL
		  DROP TABLE #QueueItemDetail;

	/* Insert queue data for running processes */
	SELECT QueueId
		 , SUM(CASE WHEN ISDATE(Finished) = 0 THEN 1 ELSE 0 END) AS PendingCount
		 , SUM(CASE WHEN ISNULL([Status], '') IN ('System Exception','Internal') THEN 1 ELSE 0 END) AS ExceptionCount
		 , SUM(CASE WHEN [State] > 2 THEN 1 ELSE 0 END) AS CompletedCount
		 , MAX(LastUpdated) AS LastUpdated
		 , COUNT(DISTINCT SessionId) AS ResourceCount
	  INTO #QueueItemDetail
	  FROM (
		   SELECT qi.QueueId
				, qi.[Status]
				, qi.[State]
				, qi.Finished
				, qi.LastUpdated
				, crs.SessionId
			 FROM BPVWorkQueueItem qi WITH (NOLOCK)
					 INNER JOIN #CurrRunningSessions crs
						ON qi.QueueId = crs.QueueId
					   /* Get only running session queue items, you do not want everything! */
					 AND qi.SessionId = crs.SessionId
			WHERE qi.Loaded >= CONVERT(DATE, GETDATE()-2)
			UNION ALL
		   /* Insert queue data for remaining queue items to non-running processes */
		   SELECT qi.QueueId
				, qi.[Status]
				, qi.[State]
				, qi.Finished
				, qi.LastUpdated
				, crs.SessionId
			 FROM BPVWorkQueueItem qi WITH (NOLOCK)
					 LEFT OUTER JOIN #CurrRunningSessions crs
					   ON qi.QueueId = crs.QueueId
					  /* Get only running session queue items, you do not want everything! */
					  AND qi.SessionId = crs.SessionId
			WHERE ISDATE(qi.Finished) = 0
			  AND crs.SessionId IS NULL
			  /* Dude, if there are pending queue items over a month old... uhh why??
				 But for speed and efficiency we need to limit the results by some date. */
			  AND qi.Loaded >= CONVERT(DATE, GETDATE()-60)
		  ) dt
	 GROUP BY QueueId
	;

	/* Quick and dirty way to get utc time difference from
	   another table which has this key field. Note that this
	   will return the record that has the most current time
	   basedf on the [StartDatetime] field */
	DECLARE @UtcHourDiff INT =
		(SELECT TOP 1 StartTimeZoneOffset / 60 / 60
		   FROM dbo.BPVSessionInfo si WITH (NOLOCK)
		  WHERE StartTimeZoneOffset < 0
		  ORDER BY StartDatetime DESC)
	;

	SELECT ISNULL(qa.QueueName, q.[Name] + ' (Update link table: '+ CONVERT(NVARCHAR(MAX), qid.QueueId) +')') AS QueueName
		 , ISNULL(qid.ResourceCount, 0) AS Resources
		 , qid.PendingCount AS Remaining
		 , ISNULL(qid.ExceptionCount, 0) AS SysExceptions
		 , ISNULL(qid.CompletedCount, 0) AS Cmpl
		 , ISNULL(qid.ExceptionCount, 0) + ISNULL(qid.CompletedCount, 0) AS TotalProcessed
		 , FORMAT(DATEADD(HOUR, @UtcHourDiff, qid.LastUpdated), 'yyyy-MM-dd hh:mm tt')  AS LastUpdated
		 /* Outputs a nicely, consistent format -> Hours : Minutes : Seconds */
		 , REPLACE(CONVERT(VARCHAR, DATEADD(ms, qa.AvgSeconds * 1000, 2), 114),':000','') AS AvgWorktime
		 , REPLACE(CONVERT(VARCHAR, DATEADD(ms, qa.AvgSeconds * qid.PendingCount * 1000, 2), 114),':000','') AS EstTotalTimeRem
		 , CASE WHEN qid.ResourceCount = 0 
				THEN NULL
				ELSE FORMAT(DATEADD(HOUR, @UtcHourDiff, 
						  DATEADD(SECOND,ROUND((qid.PendingCount * qa.AvgSeconds) / ISNULL(qid.ResourceCount, 1),0), GETDATE()))
						  , 'yyyy-MM-dd hh:mm tt')
		   END AS EstEndTime
	  FROM #QueueItemDetail qid
	         /* There may not be enough data to calculate averages */
			  LEFT OUTER JOIN #QueueAverages qa
				ON qid.QueueId = qa.QueueId
			 INNER JOIN BPAWorkQueue q WITH (NOLOCK)
				ON qid.QueueId = q.Id
	 ORDER BY qid.ResourceCount
		 , qa.QueueName
	;

END