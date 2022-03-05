USE [MISREPORT_DB]
GO
/****** Object:  UserDefinedFunction [dbo].[TRIM]    Script Date: 03-01-2022 18:01:35 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER FUNCTION [dbo].[TRIM](@string VARCHAR(MAX))
RETURNS VARCHAR(MAX)
BEGIN
RETURN LTRIM(RTRIM(@string))
END

Thanks & Regards,
Karunakar P,
DW-BI,
M:-+91-8688106525.