USE [MIS_DB]
GO
/****** Object:  UserDefinedFunction [dbo].[f_Get_Weeks_of_1to7]    Script Date: 03-01-2022 18:10:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER FUNCTION [dbo].[f_Get_Weeks_of_1to7]

(  

 @Date   SmallDateTime,
 @NeedSerialization Varchar(1)=Null


)  

RETURNS Varchar(100)  

AS  

BEGIN  

 

 RETURN(

 

( Case
when  day(@Date) between 01 and 07  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'01 to 07'+' '+dbo.formatdate(@Date,'MMM-YY')
when  day(@Date) between 08 and 14  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'08 to 14'+' '+dbo.formatdate(@Date,'MMM-YY')
when  day(@Date) between 15 and 21  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'15 to 21'+' '+dbo.formatdate(@Date,'MMM-YY')
when  day(@Date) between 22 and 28  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'22 to 28'+' '+dbo.formatdate(@Date,'MMM-YY')
when  day(@Date) between 29 and 31  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'29 to EOM'+' '+dbo.formatdate(@Date,'MMM-YY')
Else 'NA'

End
)

 

 )  

END

Thanks & Regards,
Karunakar P,
DW-BI,
M:-+91-8688106525.