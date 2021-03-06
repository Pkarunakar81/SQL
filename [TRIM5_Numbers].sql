USE [GOLIVE_EOM]
GO
/****** Object:  UserDefinedFunction [dbo].[TRIM5_Numbers]    Script Date: 03-01-2022 18:22:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER FUNCTION [dbo].[TRIM5_Numbers](@string VARCHAR(8000))  
RETURNS VARCHAR(8000)  
BEGIN  
RETURN REPLACE(@string, SUBSTRING(@string, PATINDEX('%[^a-zA-Z0-9#-_&@/\(){}[],.;: '''''']%', @string), 1), '')  
set @string =REPLACE(@string, SUBSTRING(@string, PATINDEX('%[^0-9 '''''']%', @string), 1), '')  
set @string =REPLACE(@string,'|','')
set @string =REPLACE(@string,'-','')
RETURN @string
END

--select * from t_Client where Zipcode ^6