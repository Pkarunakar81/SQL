USE [MIS_DB]
GO
/****** Object:  UserDefinedFunction [dbo].[RemoveSpecialChars1]    Script Date: 03-01-2022 18:13:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER function [dbo].[RemoveSpecialChars1] (@s varchar(256)) returns varchar(256)
   with schemabinding
begin
   if @s is null
      return null
   declare @s2 varchar(256)
   set @s2 = ''
   declare @l int
   set @l = len(@s)
   declare @p int
   set @p = 1
   while @p <= @l begin
      declare @c int
      set @c = ascii(substring(@s, @p, 1))
      if @c between 48 and 57 or @c between 65 and 90 or @c between 97 and 122 or @c between 32 and 32 or @c between 44 and 46
         set @s2 = @s2 + char(@c)
      set @p = @p + 1
      end
   if len(@s2) = 0
      return null
   return @s2
   end