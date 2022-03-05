USE [MIS_DB]
GO
/****** Object:  UserDefinedFunction [dbo].[f_Get_Weeks_of_UJJ]    Script Date: 03-01-2022 18:10:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER FUNCTION [dbo].[f_Get_Weeks_of_UJJ]

(  

 @Date   SmallDateTime,
 @NeedSerialization Varchar(1)=Null


)  

RETURNS Varchar(100)  

AS  

BEGIN  

 

 RETURN(

 

( Case
when  day(@Date) between 01 and 03  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'01 to 03'+' '+dbo.formatdate(@Date,'MMM-YY')
when  day(@Date) between 04 and 09  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'04 to 09'+' '+dbo.formatdate(@Date,'MMM-YY')
when  day(@Date) between 10 and 16  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'10 to 16'+' '+dbo.formatdate(@Date,'MMM-YY')
when  day(@Date) between 17 and 23  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'17 to 23'+' '+dbo.formatdate(@Date,'MMM-YY')
when  day(@Date) between 24 and 31  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'24 to EOM'+' '+dbo.formatdate(@Date,'MMM-YY')
Else 'NA'

End
)

 

 )  

END

Thanks & Regards,
Karunakar P,
DW-BI,
M:-+91-8688106525.