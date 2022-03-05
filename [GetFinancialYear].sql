

USE [MIS_DB]
GO
/****** Object:  UserDefinedFunction [dbo].[GetFinancialYear]    Script Date: 03-01-2022 18:13:06 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER FUNCTION [dbo].[GetFinancialYear] (@input DATETIME)
RETURNS VARCHAR(20)
AS BEGIN
    DECLARE @FinYear VARCHAR(20)

    DECLARE @YearOfDate INT

    IF (MONTH(@input) >= 4)
        SET @YearOfDate = YEAR(@input)  
    ELSE
        SET @YearOfDate = YEAR(@input) - 1

    SET @FinYear = RIGHT(CAST(@YearOfDate AS CHAR(4)), 2) + '-' + RIGHT(CAST((@YearOfDate + 1) AS CHAR(4)), 2)

    RETURN @FinYear
END

Thanks & Regards,
Karunakar P,
DW-BI,
M:-+91-8688106525.