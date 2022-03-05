USE [MISREPORT_DB]
GO
/****** Object:  UserDefinedFunction [dbo].[TRIM3]    Script Date: 03-01-2022 18:01:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER FUNCTION [dbo].[TRIM3](@string VARCHAR(8000))  
RETURNS VARCHAR(8000)  
BEGIN  
RETURN REPLACE(@string, SUBSTRING(@string, PATINDEX('%[^a-zA-Z0-9 '''''']%', @string), 1), '')  
--LTRIM(RTRIM(REPLACE(@string, SUBSTRING(@string, PATINDEX('%[^a-zA-Z0-9 '''''']%', @string), 1), '')))  
--LTRIM(RTRIM(@string))  
END

Thanks & Regards,
Karunakar P,
DW-BI,
M:-+91-8688106525.