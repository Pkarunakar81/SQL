USE [GOLIVE_EOM]
GO
/****** Object:  UserDefinedFunction [dbo].[RemoveAllSpaces]    Script Date: 03-01-2022 18:22:18 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER FUNCTION [dbo].[RemoveAllSpaces]
(
    @InputStr varchar(8000)
)
RETURNS varchar(8000)
AS
BEGIN
declare @ResultStr varchar(8000)
set @ResultStr = @InputStr
while charindex(' ', @ResultStr) > 0
    set @ResultStr = replace(@InputStr, ' ', '')

return @ResultStr
END