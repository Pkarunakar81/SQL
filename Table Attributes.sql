DROP TABLE ##1
SELECT  DISTINCT T.name,I.name INDEX_NAME,I.type_desc
INTO ##1
FROM    sys.tables t  
        INNER JOIN sys.schemas sc ON sc.schema_id = t.schema_id  
        INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id  
        INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID  
                                            AND i.index_id = p.index_id  
        INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id  
WHERE  sc.name='INTDATA' and --AND t.type_desc = 'USER_TABLE'   AND
         t.NAME IN (select CASE WHEN TABLE_NAME ='C_LOAN_MANDATE_MAINT_TABLE' THEN 'CBS_C_LOAN_MANDATE_MAINT_TABLE'
		WHEN TABLE_NAME='CBS_INT_CMG' THEN 'CBS_CMG' ELSE TABLE_NAME END  FROM INTDATA.ETL_TABLE_DETAILS WHERE SOURCE_SYSTEM='CBS')

DROP TABLE ##3
SELECT DISTINCT TABLE_NAME,COLUMN_NAME
,cast(null as varchar(100)) 'Attribute Description'
,DATA_TYPE,case 
when DATA_TYPE in ('nvarchar','varchar','binary','varbinary') then cast(CHARACTER_MAXIMUM_LENGTH as varchar(50))
when DATA_TYPE in ('numeric','decimal','float','real') then cast(NUMERIC_PRECISION as varchar(50))
when DATA_TYPE in ('datetime','date') then cast(DATETIME_PRECISION as varchar(50))
else null end Data_Length
--,CHARACTER_MAXIMUM_LENGTH 'Data_Length'
,is_nullable 'Is Null ( Y/N )'
--,cast('N' as varchar(10)) 'Is Null [ Y/N ]'
,cast('N' as varchar(10)) 'Is PK [ Y/N]'
,cast('N' as varchar(10))  'Is Foreign Key [ Y/N]'
,cast('N' as varchar(10)) 'Clustered Index'
,cast('N' as varchar(10)) 'Non-Clustered Index'
INTO ##3
FROM (SELECT  DISTINCT T.name--,I.name INDEX_NAME,I.type_desc
FROM    sys.tables t  
        INNER JOIN sys.schemas sc ON sc.schema_id = t.schema_id  
WHERE  sc.name='INTDATA' and --AND t.type_desc = 'USER_TABLE'   AND
         t.NAME IN (select distinct CASE WHEN TABLE_NAME ='C_LOAN_MANDATE_MAINT_TABLE' 
		 THEN 'CBS_C_LOAN_MANDATE_MAINT_TABLE' WHEN TABLE_NAME='CBS_INT_CMG' THEN 'CBS_CMG' ELSE TABLE_NAME END  
		 FROM INTDATA.ETL_TABLE_DETAILS WHERE SOURCE_SYSTEM='CBS')) A 
		 inner join ( SELECT TABLE_NAME,COLUMN_NAME,IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,DATETIME_PRECISION
FROM INFORMATION_SCHEMA.COLUMNS  where TABLE_SCHEMA='INTDATA') b on A.name=b.TABLE_NAME
order by TABLE_NAME

update a set a.[Clustered Index]='Y'
--select distinct a.TABLE_NAME,b.name
from ##3 a,(SELECT distinct b.NAME,type_desc
FROM ##1 B where type_desc='CLUSTERED') b where A.TABLE_NAME=B.NAME

update a set a.[Non-Clustered Index]='Y'
--select distinct a.TABLE_NAME,b.name
from ##3 a,(SELECT distinct b.NAME,type_desc
FROM ##1 B where type_desc='NONCLUSTERED') b where A.TABLE_NAME=B.NAME

--select distinct DATA_TYPE FROM ##3 where Data_Length is null
select TABLE_NAME,count(distinct COLUMN_NAME) FROM ##3 --order by TABLE_NAME,
group by TABLE_NAME
order by TABLE_NAME
COLUMN_NAME
/*
SELECT OBJECT_NAME(OBJECT_ID) AS NameofConstraint,
SCHEMA_NAME(schema_id) AS SchemaName,
OBJECT_NAME(parent_object_id) AS TableName,
type_desc AS ConstraintType
FROM sys.objects 
WHERE type_desc IN ('FOREIGN_KEY_CONSTRAINT','PRIMARY_KEY_CONSTRAINT')
GO
*/