USE [MIS_DB]
GO
/****** Object:  UserDefinedFunction [dbo].[GetAlphaOnly]    Script Date: 03-01-2022 18:14:18 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

 ALTER FUNCTION [dbo].[GetAlphaOnly] 
 (@pString VARCHAR(MAX))
RETURNS TABLE AS
 RETURN 
   WITH
--===== Generate up to 10,000 rows ("En" indicates the power of 10 produced)
  E1(N) AS (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL
            SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL
            SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1),
  E4(N) AS (SELECT 1 FROM E1 a,E1 b,E1 c,E1 d),
 cTally AS (SELECT TOP (LEN(@pString)) N = ROW_NUMBER() OVER (ORDER BY N) FROM E4)
 SELECT AlphaOnly = CAST(
                        (
                         SELECT SUBSTRING(@pString,t.N,1)
                           FROM cTally t
                          WHERE SUBSTRING(@pString,t.N,1) LIKE '[a-z]'
                          ORDER BY N
                            FOR XML PATH('')
                        ) 
                    AS VARCHAR(MAX))
;
