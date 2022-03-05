USE [MISREPORT_DB]
GO
/****** Object:  UserDefinedFunction [dbo].[udfTrim]    Script Date: 03-01-2022 18:02:14 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

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

Thanks & Regards,
Karunakar P,
DW-BI,
M:-+91-8688106525.