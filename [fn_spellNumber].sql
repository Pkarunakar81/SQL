USE [MIS_DB]
GO
/****** Object:  UserDefinedFunction [dbo].[fn_spellNumber]    Script Date: 03-01-2022 18:12:03 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER FUNCTION [dbo].[fn_spellNumber](@SpellNumber BIGINT,@SpellType VARCHAR(10) = 'UK',@AddComma     BIT = 1)
RETURNS VARCHAR(MAX)
BEGIN
 
/*
Parameters
 
@SpellNumber - The number to be spelled.
               Supports till 19 digits and accepts the range of BIG INT.
@SpellType     - US - For US Numbering system (in designation of Million,Billion,Trillion)
               UK - For UK Numbering system (in designation of Lakhs,Crores)
@AddComma     - To add comma seperator to the output or not.  
 
*/
 
 
    DECLARE @Res VARCHAR(MAX)
    DECLARE @TmpRes VARCHAR(MAX)
    DECLARE @CurrentNumber int
    DECLARE @PrevNumber int
    DECLARE @Comma        VARCHAR(1)
    DECLARE @Loopvar int
    DECLARE @VarSpellNumber VARCHAR(50)
 
    IF @SpellNumber < 0
        SELECT @SpellNumber = @SpellNumber * -1
 
    SELECT @VarSpellNumber = @SpellNumber
 
    SELECT @Loopvar = 1
    SELECT @Res = ''
 
    SELECT @Comma = CASE @AddComma
                    WHEN 1 THEN ','
                    ELSE ''
                    END
 
 
    WHILE LTRIM(RTRIM(@VarSpellNumber)) <> ''
    BEGIN
     
        SELECT @CurrentNumber = NULL
        SELECT @PrevNumber    = NULL
        SELECT @TmpRes          = ''
     
 
        SELECT @CurrentNumber = SUBSTRING(@VarSpellNumber,LEN(@VarSpellNumber),1)
     
        IF LEN(@VarSpellNumber) > 1  
            SELECT @PrevNumber = SUBSTRING(@VarSpellNumber,LEN(@VarSpellNumber)-1,1)
     
        IF (@Loopvar = 2 AND @SpellType = 'UK') OR (@Loopvar%2=0  AND @SpellType ='US')
            SELECT @PrevNumber = NULL
 
 
        IF @PrevNumber IS NULL OR @PrevNumber <> 1  
        BEGIN
            SELECT @TmpRes = CASE @CurrentNumber
                          WHEN 1 THEN 'One'
                          WHEN 2 THEN 'Two'
                          WHEN 3 THEN 'Three'
                          WHEN 4 THEN 'Four'
                          WHEN 5 THEN 'Five'
                          WHEN 6 THEN 'Six'
                          WHEN 7 THEN 'Seven'
                          WHEN 8 THEN 'Eight'
                          WHEN 9 THEN 'Nine'
                          WHEN 0 THEN ''
                          END  
        END
     
        IF @PrevNumber = 1
        BEGIN
            SELECT @TmpRes = CASE CONVERT(INT,CONVERT(VARCHAR(1),@PrevNumber) + CONVERT(VARCHAR(1),@CurrentNumber))
                            WHEN 10 THEN 'Ten'
                            WHEN 11 THEN 'Eleven'
                            WHEN 12 THEN 'Twelve'
                            WHEN 13 THEN 'Thirteen'
                            WHEN 14 THEN 'Fourteen'
                            WHEN 15 THEN 'Fiveteen'
                            WHEN 16 THEN 'Sixteen'
                            WHEN 17 THEN 'Seventeen'
                            WHEN 18 THEN 'Eighteen'
                            WHEN 19 THEN 'Nineteen'
                            END  
 
        END
 
        IF @PrevNumber > 1
        BEGIN
            SELECT @TmpRes = CASE @PrevNumber
                            WHEN 2 THEN 'Twenty'
                            WHEN 3 THEN 'Thirty'
                            WHEN 4 THEN 'Fourty'
                            WHEN 5 THEN 'Fivty'
                            WHEN 6 THEN 'Sixty'
                            WHEN 7 THEN 'Seventy'
                            WHEN 8 THEN 'Eighty'
                            WHEN 9 THEN 'Ninety'
                            WHEN 0 THEN ''
                            END  + ' ' + @TmpRes
        END
     
        SELECT @TmpRes = LTRIM(RTRIM(@TmpRes))
 
        IF  @SpellType ='UK' AND (@TmpRes <> '' OR ( @Loopvar = 5 AND CONVERT(INT,@VarSpellNumber)>0))
        BEGIN
            SELECT @TmpRes =  @TmpRes + ' '
                               + CASE @Loopvar  
                                 WHEN 2 THEN 'Hundred'  
                                 WHEN 3 THEN 'Thousand' + @Comma
                                 WHEN 4 THEN 'Lakh' + @Comma
                                 WHEN 5 THEN 'Crore' + @Comma
                                 ELSE ''
                                 END  
     
        END
 
 
 
        IF  @SpellType ='US' AND (@TmpRes <> '' OR ( @Loopvar%2 = 1 AND CONVERT(INT,SUBSTRING(@VarSpellNumber,LEN(@VarSpellNumber)-2,1))>0))
        BEGIN
            SELECT @TmpRes =  @TmpRes + ' '
                               + CASE @Loopvar  
                                 WHEN 2 THEN 'Hundred'
                                 WHEN 3 THEN 'Thousand' + @Comma
                                 WHEN 4 THEN 'Hundred'
                                 WHEN 5 THEN 'Million' + @Comma
                                 WHEN 6 THEN 'Hundred'  
                                 WHEN 7 THEN 'Billion' + @Comma
                                 WHEN 8 THEN 'Hundred'
                                 WHEN 9 THEN 'Trillion' + @Comma
                                 WHEN 10 THEN 'Hundred'
                                 WHEN 11 THEN 'Quadrillion' + @Comma
                                 WHEN 12 THEN 'Hundred'
                                 WHEN 13 THEN 'Quintillion' + @Comma
                                 ELSE ''
                                 END  
        END
 
        IF @Loopvar = 1 AND LEN(@VarSpellNumber) > 2  
            IF CONVERT(INT,SUBSTRING(CONVERT(VARCHAR(50),@SpellNumber),LEN(@VarSpellNumber)-1,2)) > 0
                SELECT @TmpRes = 'And ' + @TmpRes
 
        IF @TmpRes <> ''
            SELECT  @Res = @TmpRes + ' ' + @Res
     
        IF (@Loopvar = 2  AND @SpellType = 'UK')  or (@Loopvar%2=0  AND @SpellType ='US')
            SELECT @VarSpellNumber = SUBSTRING(@VarSpellNumber,1,LEN(@VarSpellNumber)-1)
        ELSE IF LEN(@VarSpellNumber) = 1
            SELECT @VarSpellNumber = SUBSTRING(@VarSpellNumber,1,LEN(@VarSpellNumber)-1)
        ELSE
            SELECT @VarSpellNumber = SUBSTRING(@VarSpellNumber,1,LEN(@VarSpellNumber)-2)
 
         
        SELECT @Loopvar = @Loopvar + 1
     
        IF @Loopvar = 6 AND @SpellType ='UK'
            SELECT @Loopvar = 2
     
    END
 
    SELECT @Res = LTRIM(RTRIM(@RES))
 
 
    RETURN (@Res)
 
END
 


Thanks & Regards,
Karunakar P,
DW-BI,
M:-+91-8688106525.