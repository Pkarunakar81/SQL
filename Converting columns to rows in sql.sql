DECLARE @cols AS NVARCHAR(MAX), @query AS NVARCHAR(MAX),@OurBranchID Varchar(5),@cols1 AS NVARCHAR(MAX) ;

drop table ##A select top 10 b.RegionID_Fin,a.* into ##A from v_t_userfielddata a
inner join v_Regions_Branches b on a.OurBranchID=b.OurBranchID where b.OurBranchID='1102'

SET @cols1 = STUFF((SELECT distinct ',' + QUOTENAME(c.FieldName,'''')
FROM v_t_UserField c FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,1,'')
SELECT @cols1
SET @cols = STUFF((SELECT distinct ',' + QUOTENAME(c.FieldName) FROM v_t_UserField c FOR XML PATH(''), TYPE).value
('.', 'NVARCHAR(MAX)'),1,1,'')
SELECT @cols
set @query = 'SELECT RegionID_Fin RegionID,OurBranchID,ModuleTypeID,RelevantID,RelevantID1,LoanSeries, ' + @cols +
'from (select * from ##A c where FieldName in (' + @cols1 + ')) x
pivot(max(fieldvalue)for fieldname in (' + @cols + ')) p order by ModuleTypeID,RelevantID '
--select @query
execute(@query)