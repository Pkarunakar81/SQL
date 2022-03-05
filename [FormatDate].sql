USE [MIS_DB]
GO
/****** Object:  UserDefinedFunction [dbo].[FormatDate]    Script Date: 03-01-2022 18:12:40 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER FUNCTION [dbo].[FormatDate]
  (@d as sql_variant,@fOUT NVARCHAR(100))
RETURNS NVARCHAR(100) AS  
BEGIN
  DECLARE @rv as NVARCHAR(100),@d1 datetime,@dc NVARCHAR(100)
  DECLARE @s NVARCHAR(100),@n int
  SET @dc=CONVERT(NVARCHAR(100),@d,109)
  IF ISDATE(@dc)=0
    RETURN @dc
  SET @d1=CONVERT(datetime,@dc)

  DECLARE @aUpper tinyint,@aLower tinyint
  DECLARE @a NVARCHAR(2),@p NVARCHAR(2)
  SET @aUpper=ASCII('A')
  SET @aLower=ASCII('a')
  SET @a='a'
  SET @p='p'
  SET @n=CHARINDEX('a/p',@fOUT,1)
  IF @n>0
  BEGIN
    IF ASCII(SUBSTRING(@fOUT,@n,1))=@aUpper
    BEGIN
      SET @a='A'
      SET @p='P'
    END
  END
  ELSE
  BEGIN
    SET @a='am'
    SET @p='pm'
    SET @n=CHARINDEX('am/pm',@fOUT,1)
    IF @n>0
    BEGIN
      IF ASCII(SUBSTRING(@fOUT,@n,1))=@aUpper
      BEGIN
        SET @a='AM'
        SET @p='PM'
      END
    END
  END

  SET @rv=@fOUT
  SET @s=CASE WHEN CHARINDEX('a/p',@rv,1)=0 and
    CHARINDEX('am/pm',@rv,1)=0
    THEN CONVERT(NVARCHAR(2),DATEPART(hh,@d1))
    WHEN DATEPART(hh,@d1) between 1 and 12 THEN
      CONVERT(NVARCHAR(2),DATEPART(hh,@d1))
      WHEN DATEPART(hh,@d1)=0 THEN '12'
      ELSE CONVERT(NVARCHAR(2),DATEPART(hh,@d1)-12)
      END
  SET @rv=CASE WHEN CHARINDEX('a/p',@rv,1)>0 THEN
    CASE WHEN DATEPART(hh,@d1)<12 THEN REPLACE(@rv,'a/p',@a)
    ELSE REPLACE(@rv,'a/p',@p) END
    WHEN CHARINDEX('am/pm',@rv,1)>0 THEN
    CASE WHEN DATEPART(hh,@d1)<12 THEN REPLACE(@rv,'am/pm',@a)
    ELSE REPLACE(@rv,'am/pm',@p) END
    ELSE @rv
    END
  SET @rv=REPLACE(@rv,'hh',CASE WHEN len(@s)=2 THEN @s ELSE '0'+@s END)
  SET @s=CONVERT(NVARCHAR(2),DATEPART(n,@d1))
  SET @rv=REPLACE(@rv,'nn',CASE WHEN len(@s)=2 THEN @s ELSE '0'+@s END)
  SET @s=CONVERT(NVARCHAR(2),DATEPART(s,@d1))
  SET @rv=REPLACE(@rv,'ss',CASE WHEN len(@s)=2 THEN @s ELSE '0'+@s END)
  SET @s=CONVERT(NVARCHAR(3),DATEPART(ms,@d1))
  SET @rv=REPLACE(@rv,'ms',CASE WHEN len(@s)=3 THEN
    @s WHEN len(@s)=2 THEN '0'+@s ELSE '00'+@s END)
  SET @s=CONVERT(NVARCHAR(4),DATEPART(yyyy,@d1))
  SET @rv=REPLACE(@rv,'yyyy',@s)
  SET @s=RIGHT(@s,2)
  SET @rv=REPLACE(@rv,'yy',@s)
  SET @s=CONVERT(NVARCHAR(20),DATENAME(mm,@d1))
  SET @rv=REPLACE(@rv,'mmmm',@s)
  SET @s=LEFT(@s,3)
  SET @rv=REPLACE(@rv,'mmm',@s)
  SET @s=CONVERT(NVARCHAR(20),DATENAME(dw,@d1))
  SET @rv=REPLACE(@rv,'wdd',@s)
  SET @s=LEFT(@s,3)
  SET @rv=REPLACE(@rv,'wd',@s)
  SET @s=CONVERT(NVARCHAR(2),DATEPART(mm,@d1))
  SET @rv=REPLACE(@rv,'mm',CASE WHEN len(@s)=2 THEN @s ELSE '0'+@s END)
  SET @s=CONVERT(NVARCHAR(2),DATEPART(dd,@d1))
  SET @rv=REPLACE(@rv,'dd',CASE WHEN len(@s)=2 THEN @s ELSE '0'+@s END)
  RETURN_Rv:
  RETURN @rv
END




/*
mm = month
• dd = day
• yyyy or yy = year
• hh = hours
• nn = minutes
• ss = seconds
• ms = milliseconds
• mmmm = long month name (e.g., January)
• mmm = short (three characters) month name (e.g., Jan)
• wdd = long day-of-the-week name (e.g., Monday)
• wd = short (three characters) day-of-the-week name (e.g., Mon)
• AM/PM = AM or PM
• am/pm = am or pm
• A/P = A or P
• a/p = a or p


*/

Thanks & Regards,
Karunakar P,
DW-BI,
M:-+91-8688106525.