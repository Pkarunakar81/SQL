USE [MIS_DB]
GO
/****** Object:  UserDefinedFunction [dbo].[RemoveSpecialChars2]    Script Date: 03-01-2022 18:13:48 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



ALTER function [dbo].[RemoveSpecialChars2] (@s varchar(256)) RETURNS VARCHAR(8000)  
begin
DECLARE @regex INT,@string varchar(100)
SET @string=@s
SET @regex = PATINDEX('%[^a-zA-Z0-9 ]%', @string)
WHILE @regex > 0
BEGIN
SET @string = STUFF(@string, @regex, 1, ' ' )
SET @regex = PATINDEX('%[^a-zA-Z0-9 ]%', @string)
END
RETURN  @string
end