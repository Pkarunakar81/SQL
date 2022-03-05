ALTER FUNCTION [dbo].[f_GetFinQtr]
(  
 @Date   SmallDateTime  
)  
RETURNS Varchar(20)  
AS  
BEGIN  
 RETURN(
(case when  month(@Date)> '9'  then 'Q3' --For(Oct, Nov & Dec)
   when  month(@Date)> '6'  then 'Q2' --For (Jul, Aug & Sep)
   when  month(@Date)> '3'  then 'Q1' -- For (Apr, May & Jun)
   Else 'Q4' -- for (Jan, FEb & Mar)
End)
 )  
END

Thanks & Regards,
Karunakar P,
DW-BI,
M:-+91-8688106525.