USE [GOLIVE_EOM]
GO
/****** Object:  UserDefinedFunction [dbo].[f_CorrectString]    Script Date: 03-01-2022 18:16:53 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




/*
Non Alphabetic Characetres will remove
Tabe Change To Single Space ' '
Consecutive space will remove

--Not considers
List of alternative
Soundex
*/
--Select dbo.f_CorrectString('For1     Test  As$')
ALTER FUNCTION [dbo].[f_CorrectString]
(
	@Str	VarChar(500)
)
RETURNS VarChar(500)
AS
BEGIN
	DECLARE @TempStr VarChar(100)

	DECLARE @StrLen		Int,
			@Counter	Int,
			@CharAscii	Int

	DECLARE @PrevSpace	Int

	SELECT @Str = LTRIM(RTRIM(@Str)) --Remove Space

	--Replacing common mistake
	SELECT @Str = REPLACE(@Str,MatchingString , CorrectString )
	FROM t_StringMatch

	SELECT @Str = UPPER(@Str)

	SELECT @Str = REPLACE(@Str,'	',' ') --Remove Tab

	-- We have to break string into words
	--then look into MatchingString exists , if so replace with correct one
	--SELECT * from t_StringMatch

	SET @StrLen = LEN(@Str)

	SELECT @Counter =1,@PrevSpace = 0,@TempStr = @Str

	WHILE @StrLen >= @Counter
	BEGIN
		Select @CharAscii = ASCII(SUBSTRING(@Str,@Counter,1)) 

		IF @CharAscii  = 32 --Space 
		BEGIN
			IF @PrevSpace = 0
				SET @PrevSpace = @Counter

			SET @Counter = @Counter +1		
		END
		ELSE IF NOT (@CharAscii BETWEEN 65 AND 90)  --Non Alphabetic  -- we have to think whether we will allow Numeric
		BEGIN
			SET @TempStr = REPLACE(@TempStr,SUBSTRING(@TempStr,@Counter,1),'$')
			SET @Counter = @Counter +1
		END
		ELSE 
		BEGIN
			IF @PrevSpace > 0 
			BEGIN
				SET @TempStr = REPLACE (@TempStr,REPLICATE(' ',@Counter-@PrevSpace),' $')
				SET @PrevSpace = 0
			END

			SET @Counter = @Counter +1
		END
	END

	RETURN REPLACE(@TempStr,'$','')
END





