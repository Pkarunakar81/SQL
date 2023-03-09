USE [GOLIVE_EOM]
GO
/****** Object:  UserDefinedFunction [dbo].[TRIM3_Names]    Script Date: 03-01-2022 18:22:30 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER FUNCTION [dbo].[TRIM3_Names](@string VARCHAR(8000))  
RETURNS VARCHAR(8000)  
BEGIN  
RETURN REPLACE(@string, SUBSTRING(@string, PATINDEX('%[^a-zA-Z0-9.() '''''']%', @string), 1), '')  
--LTRIM(RTRIM(REPLACE(@string, SUBSTRING(@string, PATINDEX('%[^a-zA-Z0-9 '''''']%', @string), 1), '')))  
--LTRIM(RTRIM(@string))  
END