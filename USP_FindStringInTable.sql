ALTER PROCEDURE dbo.USP_FindStringInTable @stringToFind VARCHAR(max), @schema sysname, @table sysname 
AS

SET NOCOUNT ON

BEGIN TRY
   DECLARE @sqlCommand varchar(max) = 'SELECT * FROM [' + @schema + '].[' + @table + '] WHERE ' 
	   
   SELECT @sqlCommand = @sqlCommand + '[' + COLUMN_NAME + '] LIKE ''' + @stringToFind + ''' OR '
   FROM INFORMATION_SCHEMA.COLUMNS 
   WHERE TABLE_SCHEMA = @schema
   AND TABLE_NAME = @table 
   AND DATA_TYPE IN ('char','nchar','ntext','nvarchar','text','varchar')

   SET @sqlCommand = left(@sqlCommand,len(@sqlCommand)-3)
   EXEC (@sqlCommand)
   PRINT @sqlCommand
END TRY

BEGIN CATCH 
   PRINT 'There was an error. Check to make sure object exists.'
   PRINT error_message()
END CATCH 
/*
After the stored procedure has been created it needs to be marked as a system stored procedure.

USE master
GO

EXEC sys.sp_MS_marksystemobject sp_FindStringInTable
GO

One thing to keep in mind is that if you are using the % in front of the value such as '%land' this will force SQL Server to scan the table and not use the indexes. Also, since you are searching through all columns using an OR clause it is a pretty good bet that you will do a table scan, so be aware of this on large tables.

-------------------------------

CREATE PROCEDURE dbo.sp_FindStringInTable_with_flag @stringToFind VARCHAR(max), @schema sysname, @table sysname 
AS

SET NOCOUNT ON

BEGIN TRY
   DECLARE @sqlCommand varchar(max) = 'SELECT ' 

   SELECT @sqlCommand = @sqlCommand + 'case when [' + COLUMN_NAME + '] LIKE ''' + @stringToFind + ''' then 1 else 0 end as ' + COLUMN_NAME + '_found, ' 
   FROM INFORMATION_SCHEMA.COLUMNS 
   WHERE TABLE_SCHEMA = @schema
   AND TABLE_NAME = @table 
   AND DATA_TYPE IN ('char','nchar','ntext','nvarchar','text','varchar')

   SELECT @sqlCommand = @sqlCommand + ' * FROM [' + @schema + '].[' + @table + '] WHERE '
	   
   SELECT @sqlCommand = @sqlCommand + '[' + COLUMN_NAME + '] LIKE ''' + @stringToFind + ''' OR '
   FROM INFORMATION_SCHEMA.COLUMNS 
   WHERE TABLE_SCHEMA = @schema
   AND TABLE_NAME = @table 
   AND DATA_TYPE IN ('char','nchar','ntext','nvarchar','text','varchar')

   SET @sqlCommand = left(@sqlCommand,len(@sqlCommand)-3)
   EXEC (@sqlCommand)
   PRINT @sqlCommand
END TRY

BEGIN CATCH 
   PRINT 'There was an error. Check to make sure object exists.'
   PRINT error_message()
END CATCH */


exec USP_FindStringInTable '1102701581','intdata','crm_acc_ex1'