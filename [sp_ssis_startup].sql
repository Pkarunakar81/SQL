USE [master]
GO
/****** Object:  StoredProcedure [dbo].[sp_ssis_startup]    Script Date: 03-01-2022 17:53:26 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


    ALTER PROCEDURE [dbo].[sp_ssis_startup]
    AS
    SET NOCOUNT ON
        /* Currently, the IS Store name is 'SSISDB' */
        IF DB_ID('SSISDB') IS NULL
            RETURN
       
        IF NOT EXISTS(SELECT name FROM [SSISDB].sys.procedures WHERE name=N'startup')
            RETURN
         
        /*Invoke the procedure in SSISDB  */
        /* Use dynamic sql to handle AlwaysOn non-readable mode*/
        DECLARE @script nvarchar(500)
        SET @script = N'EXEC [SSISDB].[catalog].[startup]'
        EXECUTE sp_executesql @script

Thanks & Regards,
Karunakar P,
DW-BI,
M:-+91-8688106525.