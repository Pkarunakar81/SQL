USE [GOLIVE_EOM]
GO
/****** Object:  UserDefinedFunction [dbo].[fn_RemoveNonAlphaCharacters]    Script Date: 03-01-2022 18:22:10 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER FUNCTION [dbo].[fn_RemoveNonAlphaCharacters]

(

    @String NVARCHAR(MAX) 

   )

RETURNS NVARCHAR(MAX)

AS

BEGIN

	 WHILE PatIndex('%[^a-z ]%', @String) > 0

        SET @String = Stuff(@String, PatIndex('%[^a-z ]%', @String), 1, '')



    RETURN @String



END
