USE [MIS_DB]
GO
/****** Object:  UserDefinedFunction [dbo].[trimChars]    Script Date: 03-01-2022 18:13:56 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER FUNCTION [dbo].[trimChars] (
   @string varchar(max)
) 

RETURNS varchar(max) 
AS
BEGIN

DECLARE @es NVarChar(1) SET @es = ''
DECLARE @p22 NVarChar(1) SET @p22 = '^'
DECLARE @p23 NVarChar(1) SET @p23 = '&'
DECLARE @p24 NVarChar(1) SET @p24 = '*'
DECLARE @p25 NVarChar(1) SET @p25 = '('
DECLARE @p26 NVarChar(1) SET @p26 = '_'
DECLARE @p27 NVarChar(1) SET @p27 = ')'
DECLARE @p28 NVarChar(1) SET @p28 = '`'
DECLARE @p29 NVarChar(1) SET @p29 = '~'
DECLARE @p30 NVarChar(1) SET @p30 = '{'

DECLARE @p31 NVarChar(1) SET @p31 = '}'
DECLARE @p32 NVarChar(1) SET @p32 = ' '
DECLARE @p33 NVarChar(1) SET @p33 = '['
DECLARE @p34 NVarChar(1) SET @p34 = '?'
DECLARE @p35 NVarChar(1) SET @p35 = ']'
DECLARE @p36 NVarChar(1) SET @p36 = '\'
DECLARE @p37 NVarChar(1) SET @p37 = '|'
DECLARE @p38 NVarChar(1) SET @p38 = '<'
DECLARE @p39 NVarChar(1) SET @p39 = '>'
DECLARE @p40 NVarChar(1) SET @p40 = '@'
DECLARE @p41 NVarChar(1) SET @p41 = '-'

return   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
       @string, @p22, @es), @p23, @es), @p24, @es), @p25, @es), @p26, @es), @p27, @es), @p28, @es), @p29, @es), @p30, @es), @p31, @es), @p32, @es), @p33, @es), @p34, @es), @p35, @es), @p36, @es), @p37, @es), @p38, @es), @p39, @es), @p40, @es), @p41, @es)
END 
