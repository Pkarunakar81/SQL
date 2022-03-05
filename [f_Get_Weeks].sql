USE [MIS_DB]
GO
/****** Object:  UserDefinedFunction [dbo].[f_Get_Weeks]    Script Date: 03-01-2022 18:09:36 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER FUNCTION [dbo].[f_Get_Weeks]

(  

 @Date   SmallDateTime,
 @WeekType Nvarchar(3)='GEN',-- "GEN"=1to7 and soo on, "UJJ"=UjjivanWeeks
 @NeedSerialization Varchar(1)=Null


)  

RETURNS Varchar(100)  

AS  

BEGIN  

 Declare @Week NVarchar(100)

 

IF @WeekType='GEN'

Begin
select @Week=
( Case
when  day(@Date) between 01 and 07  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'01 to 07'+' '+dbo.formatdate(@Date,'MMM-YY')
when  day(@Date) between 08 and 14  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'08 to 14'+' '+dbo.formatdate(@Date,'MMM-YY')
when  day(@Date) between 15 and 21  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'15 to 21'+' '+dbo.formatdate(@Date,'MMM-YY')
when  day(@Date) between 22 and 28  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'22 to 28'+' '+dbo.formatdate(@Date,'MMM-YY')
when  day(@Date) between 29 and 31  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'29 to EOM'+' '+dbo.formatdate(@Date,'MMM-YY')
Else 'NA'

End
)

End


IF @WeekType='UJJ'

Begin
select @Week=
( Case
when  day(@Date) between 01 and 03  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'01 to 03'+' '+dbo.formatdate(@Date,'MMM-YY')
when  day(@Date) between 04 and 09  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'04 to 09'+' '+dbo.formatdate(@Date,'MMM-YY')
when  day(@Date) between 10 and 16  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'10 to 16'+' '+dbo.formatdate(@Date,'MMM-YY')
when  day(@Date) between 17 and 23  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'17 to 23'+' '+dbo.formatdate(@Date,'MMM-YY')
when  day(@Date) between 24 and 31  then case when @NeedSerialization='Y' then Cast(datepart(Week,@Date)as nvarchar(8))+'-' else '' end+'24 to EOM'+' '+dbo.formatdate(@Date,'MMM-YY')
Else 'NA'

End
)


End


RETURN @Week

   

END

Thanks & Regards,
Karunakar P,
DW-BI,
M:-+91-8688106525.