USE [MIS_DB]
GO
/****** Object:  UserDefinedFunction [dbo].[TRIM_Cama_DoubleQuotes]    Script Date: 03-01-2022 18:13:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER FUNCTION [dbo].[TRIM_Cama_DoubleQuotes](@string VARCHAR(8000))    
RETURNS VARCHAR(8000)    
BEGIN    
RETURN REPLACE( REPLACE(@string,',',''),'"','')
--REPLACE(@string, SUBSTRING(@string, PATINDEX('%[^a-zA-Z'''''']%', @string), 1), '')    
--LTRIM(RTRIM(REPLACE(@string, SUBSTRING(@string, PATINDEX('%[^a-zA-Z0-9 '''''']%', @string), 1), '')))    
--LTRIM(RTRIM(@string))    
END