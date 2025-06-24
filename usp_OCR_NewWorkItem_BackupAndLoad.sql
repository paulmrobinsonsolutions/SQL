/*============================================================
Description: Load daily work items mto be worked each day. Items worked from prior
day should be moved to archive while those items not yet worked need to remain
in the 'current day' subset to be worked. A new set of items to work will be loaded
each day. Since the source of authority is another system, items from prior days
deemed not 'complete' will be brought back in.

--- Requirements ---
1. Check that new items were received. This may not happen every day.
2. Move items worked (based on completed date/time) from 'current day' to 'archive' table
3. Move 'current day' work items to 'backup' table.
4. Move new items into 'current day' table *excluding* items that have 
   already been worked (based on Id in 'archive' table)
============================================================
   
------   Change Log   ------
DATE     NAME    DESCRIPTION
------------------------------------------------------------


============================================================*/
ALTER PROCEDURE [dbo].[usp_OCR_NewWorkItem_BackupAndLoad]

AS
BEGIN
   SET NOCOUNT ON;

   BEGIN TRY;
      BEGIN TRANSACTION;

      /* 1. Was new data been received for today's work update,
            meaning there are records in dbo.tbl_WorkItemsToLoad. */
      IF EXISTS(SELECT TOP 1 Id FROM dbo.[tbl_WorkItemsToLoad] WITH (NOLOCK))
      BEGIN

         /* 2. Prepare the 'backup' table to backup the data in table, tbl_WorkItemsToProcess */
         TRUNCATE TABLE dbo.[tbl_Backup_WorkItemsToProcess];

         /* 3. Load data from tbl_WorkItemsToProcess into tbl_Backup_WorkItemsToProcess for safe keeping */
         INSERT INTO dbo.[tbl_Backup_WorkItemsToProcess]
              ( [Id]
              , [BusinessUnitId]
              , [CompanyName]
              , [Address1]
              , [Address2]
              , [PostalCode])
         SELECT [Id]
              , [BusinessUnitId]
              , [CompanyName]
              , [Address1]
              , [Address2]
              , [PostalCode]
           FROM dbo.[tbl_WorkItemsToProcess] WITH (NOLOCK)
         ;

         /* 4. In the event accounts were deactivated but then reactivated or something of the nature...
               Only update certain data fields. Its a rule. Check 4b (scroll down about 10 lines) */
          UPDATE dbo.tbl_Archive_WorkItemsToProcess
             SET [LastModifiedAt] = GETDATE()
               , [CompanyName] = bak.[CompanyName]
               , [Address1] = bak.[Address1]
               , [Address2] = bak.[Address2]
               , [PostalCode] = bak.[PostalCode]
            FROM dbo.tbl_Archive_WorkItemsToProcess arc
                   INNER JOIN dbo.[tbl_Backup_WorkItemsToProcess] bak
                      ON arc.[Id] = bak.[Id]
                     /* 4b. Only update 'Active' accounts.. or else... */
                     AND arc.[DeactivatedAt] IS NULL
         ;

         /* 5. Add new items not already in the archive table. There should just be one item per Id */
         INSERT INTO dbo.tbl_Archive_WorkItemsToProcess
              ( [Id]
              , [BusinessUnitId]
              , [CompanyName]
              , [Address1]
              , [Address2]
              , [PostalCode]
              , [CreatedAt])
           FROM dbo.[tbl_WorkItemsToProcess] src WITH (NOLCK)
          WHERE src.Id NOT IN (SELECT Id FROM dbo.tbl_Archive_WorkItemsToProcess WITH (NOLOCK))
         ;

         /* 6. Prepare tbl_WorkItemsToProcess for new data by deleting all data in the table */
         TRUNCATE TABLE dbo.[tbl_WorkItemsToProcess];

         /* 7. Extract, Transform/cleanse, and load from tbl_WorkItemsToLoad into tbl_WorkItemsToProcess */
         INSERT INTO dbo.[tbl_WorkItemsToProcess]
              ( [Id]
              , [BusinessUnitId]
              , [CompanyName]
              , [Address1]
              , [Address2]
              , [PostalCode])
         SELECT [AccountId
              , [BusinessUnitId]
              , [Name]
              /* PO Box should be saved here when there is no street address. Some business system reason. */
              , CASE WHEN ISNULL([Address1], '') = ''
                       THEN 'PO Box'
                END + [PoBoxNum]
              , [Address2]
              /* 6a. When there is a PO Box, pre-pend 'PO Box' text. Why? You wont believe it buuuut, The downstream 
                     system can only accept a text data type and because of this, I suspect, it wants try different
                     data types so when it sees "123" it wants to use it as an integer but as expected, it always
                     fail because it doesnt have the common to know it can only take in data as text. Soo to alleviate
                     this simply prepend 'PO Box' so it always knows the data type is text.
                 6b. When there is no PO Box the no need to do anything, just move on.
                 *NOTE* This example is only for demonstrational purposes (I wasnt there to verify this story).*/
              , CASE WHEN LEN(TRIM(CONVERT(NVARCHAR(5), [PoBoxNum]))) > 0
                       THEN 'PO Box ' + TRIM(CONVERT(NVARCHAR(5), [PoBoxNum]))
                END
              /* Pad postal code with zeroes. Lets keep a nice, clean standard. */
              , RIGHT('00000' + TRIM(CONVERT(NVARCHAR(5), [PoBoxNum])), 5)
           FROM dbo.[tbl_WorkItemsToLoad] WITH (NOLOCK)
          ;
      END

      COMMIT TRANSACTION;

   END TRY
   BEGIN CATCH
      IF XACT_STATE() <> 0
      BEGIN
         ROLLBACK TRANSACTION;
      END

	  /* If there is a failure, you really should capture some error details
	     to ensure issue info is saved with some details to investigate. */
      DECLARE @SrcObjName NVARCHAR(50) = OBJECT_NAME(@@PROCID);
      DECLARE @ErrMsg NVARCHAR(MAX) = ERROR_MESSAGE();
      DECLARE @ErrSeverity INT = ERROR_SEVERITY();

	  /* Stored Procedure to capture error details to a central table for analysis. */
	  EXEC dbo.[usp_CaptureDatabaseProcedureErrors] @SrcObjName, @ErrMsg, @ErrSeverity;

	END CATCH

END