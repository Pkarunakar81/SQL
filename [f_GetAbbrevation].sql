USE [GOLIVE_EOM]
GO
/****** Object:  UserDefinedFunction [dbo].[f_GetAbbrevation]    Script Date: 03-01-2022 18:18:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




ALTER FUNCTION [dbo].[f_GetAbbrevation]  
(	
	@String Varchar(200)
)
RETURNS VarChar(500)
AS
BEGIN
	
	DECLARE @TextXML XML

	SELECT @TextXML = CAST('<d>' + REPLACE(@String, ' ', '</d><d>') + '</d>' AS XML)

	DECLARE @Result VARCHAR(8000)

	SET @Result = ''

	SELECT  @Result = @Result + LEFT(T.split.value('.', 'nvarchar(max)'),1)
	FROM @TextXML.nodes('/d') T(split)
	
	RETURN @result
	
END



