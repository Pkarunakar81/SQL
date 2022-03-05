ALTER FUNCTION [dbo].[f_GetFinYear]
(  
 @Date   SmallDateTime  
)  
RETURNS Varchar(20)  
AS  
BEGIN  
 RETURN(
 (case when  month(@Date)< '4'
then (Cast((year(@Date)-1)as varchar(4) ))+'-'+(Cast((right((year(@Date)),2))as varchar(2)))
else (Cast((year(@Date))as varchar(4) ))+'-'+(Cast((right((year(@Date)+1),2))as varchar(2)))
End )
 )  
END

Thanks & Regards,
Karunakar P,
DW-BI,
M:-+91-8688106525.