USE [MIS_DB]
GO
/****** Object:  UserDefinedFunction [dbo].[udfTrim]    Script Date: 03-01-2022 18:14:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*

SQL 2000 Version

2/12/2016 Ammanna

UDF to really trim the white spaces. 

When users copy and paste from Word, Excel, or some other application

into a text box, the special non printing whitespace characters 

like a line feed remain. This will replace all the non printing

whitespace characters with Character 32 which is the space bar then

perform an LTRIM and RTRIM

 

Declare 

@Seed as varchar(20),

@Test as varchar(50)

 

Set @Seed= ' Ammanna';--CValenzuela

Set @Test =  CHAR(0)+CHAR(9)+CHAR(10)+CHAR(11)+CHAR(12)+CHAR(13)+CHAR(14)+CHAR(160) + @Seed + CHAR(0)+CHAR(9)+CHAR(10)+CHAR(11)+CHAR(12)+CHAR(13)+CHAR(14)+CHAR(160)

 

Select

	@Seed as Seed,

	LTRIM(RTRIM(@SEED)) as Seed_Trimmed,	

	@Test as Test,

	LTRIM(RTRIM(@Test)) as Test_Trimmed,

	dbo.udfTrim(@Test) as Test_Trimmed2,

	

	Len(@Seed) as Seed_Length,

	DataLength(@Seed) as Seed_DataLength,

	LEN(LTRIM(RTRIM(@Seed))) as Seed_Trimmed_Length,

	DataLength(LTRIM(RTRIM(@Seed))) as Seed_Trimmed_DataLength,

 

	Len(@Test) as Test_Length,	

	LEN(LTRIM(RTRIM(@TEST))) as Test_Trimmed_Length,    	

	DataLength(LTRIM(RTRIM(@TEST))) as Test_Trimmed_DataLength,    	

	LEN(dbo.udfTrim(@Test)) as Test_UDFTrimmed_Length,

	DataLength(dbo.udfTrim(@Test)) as Test_UDFTrimmed_DataLength

	

 

*/

ALTER FUNCTION [dbo].[udfTrim] 

(

	@StringToClean as varchar(8000),

	@RemoveSpace bit =0 --'0'=NO and 1=Yes

)

RETURNS varchar(8000)

AS

BEGIN	

	--Replace all non printing whitespace characers with Characer 32 whitespace

	--NULL

	Set @StringToClean = Replace(@StringToClean,CHAR(0),CHAR(32));

	--Horizontal Tab

	Set @StringToClean = Replace(@StringToClean,CHAR(9),CHAR(32));

	--Line Feed

	Set @StringToClean = Replace(@StringToClean,CHAR(10),CHAR(32));

	--Vertical Tab

	Set @StringToClean = Replace(@StringToClean,CHAR(11),CHAR(32));

	--Form Feed

	Set @StringToClean = Replace(@StringToClean,CHAR(12),CHAR(32));

	--Carriage Return

	Set @StringToClean = Replace(@StringToClean,CHAR(13),CHAR(32));

	--Column Break

	Set @StringToClean = Replace(@StringToClean,CHAR(14),CHAR(32));

	--Non-breaking space

	Set @StringToClean = Replace(@StringToClean,CHAR(160),CHAR(32));

 

	Set @StringToClean = LTRIM(RTRIM(@StringToClean));

	if @RemoveSpace=1

	Set @StringToClean = Replace(@StringToClean,CHAR(32),'');

	Return @StringToClean

END
