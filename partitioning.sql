ALTER DATABASE PartSample
ADD FILEGROUP January
GO
ALTER DATABASE PartSample
ADD FILEGROUP February
GO
ALTER DATABASE PartSample
ADD FILEGROUP March
GO
ALTER DATABASE PartSample
ADD FILEGROUP April
GO
ALTER DATABASE PartSample
ADD FILEGROUP May
GO
ALTER DATABASE PartSample
ADD FILEGROUP June
GO
ALTER DATABASE PartSample
ADD FILEGROUP July
GO
ALTER DATABASE PartSample
ADD FILEGROUP August
GO
ALTER DATABASE PartSample
ADD FILEGROUP September
GO
ALTER DATABASE PartSample
ADD FILEGROUP October
GO
ALTER DATABASE PartSample
ADD FILEGROUP November
GO
ALTER DATABASE PartSample
ADD FILEGROUP December
GO


SELECT * FROM Sys.filegroups

SELECT name AS [File Group Name] 
FROM Sys.filegroups
WHERE type = 'FG'



SELECT * FROM sys.database_files




ALTER DATABASE [PartSample]
    ADD FILE 
    (
    NAME = [PartJan],
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\PartJan.ndf',
        SIZE = 5080 KB, 
        MAXSIZE = UNLIMITED, 
        FILEGROWTH = 2040 KB
    ) TO FILEGROUP [January]

	-- Adding ndf for February File Group
ALTER DATABASE [PartSample]
    ADD FILE 
    (
    NAME = [PartFeb],
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\PartFeb.ndf',
        SIZE = 5080 KB, 
        MAXSIZE = UNLIMITED, 
        FILEGROWTH = 2040 KB
    ) TO FILEGROUP [February]

-- Adding ndf for March File Group
ALTER DATABASE [PartSample]
    ADD FILE 
    (
    NAME = [PartMarch],
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\PartMarch.ndf',
        SIZE = 5080 KB, 
        MAXSIZE = UNLIMITED, 
        FILEGROWTH = 2040 KB
    ) TO FILEGROUP [March]

-- Adding ndf for April File Group
ALTER DATABASE [PartSample]
    ADD FILE 
    (
    NAME = [PartApril],
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\PartApril.ndf',
        SIZE = 5080 KB, 
        MAXSIZE = UNLIMITED, 
        FILEGROWTH = 2040 KB
    ) TO FILEGROUP [April]

/-- Adding ndf for May File Group
ALTER DATABASE [PartSample]
    ADD FILE 
    (
    NAME = [PartMay],
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\PartMay.ndf',
        SIZE = 5080 KB, 
        MAXSIZE = UNLIMITED, 
        FILEGROWTH = 2040 KB
    ) TO FILEGROUP [May]

-- Adding ndf for June File Group
ALTER DATABASE [PartSample]
    ADD FILE 
    (
    NAME = [PartJune],
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\PartJune.ndf',
        SIZE = 5080 KB, 
        MAXSIZE = UNLIMITED, 
        FILEGROWTH = 2040 KB
    ) TO FILEGROUP [June]

-- Adding ndf for July File Group
ALTER DATABASE [PartSample]
    ADD FILE 
    (
    NAME = [PartJuly],
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\PartJuly.ndf',
        SIZE = 5080 KB, 
        MAXSIZE = UNLIMITED, 
        FILEGROWTH = 2040 KB
    ) TO FILEGROUP [July]

-- Adding ndf for August File Group
ALTER DATABASE [PartSample]
    ADD FILE 
    (
    NAME = [PartAug],
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\PartAug.ndf',
        SIZE = 5080 KB, 
        MAXSIZE = UNLIMITED, 
        FILEGROWTH = 2040 KB
    ) TO FILEGROUP [August]

-- Adding ndf for September File Group
ALTER DATABASE [PartSample]
    ADD FILE 
    (
    NAME = [PartSept],
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\PartSept.ndf',
        SIZE = 5080 KB, 
        MAXSIZE = UNLIMITED, 
        FILEGROWTH = 2040 KB
    ) TO FILEGROUP [September]

-- Adding ndf for October File Group
ALTER DATABASE [PartSample]
    ADD FILE 
    (
    NAME = [PartOct],
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\PartOct.ndf',
        SIZE = 5080 KB, 
        MAXSIZE = UNLIMITED, 
        FILEGROWTH = 2040 KB
    ) TO FILEGROUP [October]

-- Adding ndf for November File Group
ALTER DATABASE [PartSample]
    ADD FILE 
    (
    NAME = [PartNov],
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\PartNov.ndf',
        SIZE = 5080 KB, 
        MAXSIZE = UNLIMITED, 
        FILEGROWTH = 2040 KB
    ) TO FILEGROUP [November]

-- File Group for December
ALTER DATABASE [PartSample]
    ADD FILE 
    (
    NAME = [PartDec],
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA\PartDec.ndf',
        SIZE = 5080 KB, 
        MAXSIZE = UNLIMITED, 
        FILEGROWTH = 2040 KB
    ) TO FILEGROUP [December]


	USE PartSample
GO
drop table [dbo].[SQL Insert]
drop PARTITION SCHEME MonthWisePartition
drop PARTITION FUNCTION [MonthlyPartition] 


CREATE PARTITION FUNCTION [MonthlyPartition] (datetime)
AS RANGE RIGHT FOR VALUES ('20210701','20210801','20210901','20211001','20211101','20211201','20220101','20220201',
'20220301','20220401','20220501'
--,'20220601'--,'20220701','20220801','20220901','20221001'--,'20221101'--,'20221201'
);
USE PartSample
GO

CREATE PARTITION SCHEME MonthWisePartition
AS PARTITION MonthlyPartition
		TO (January, February, March, April, May, June, July, 
			August, September, October, November, December);



--			Create a Table with Table Partitioning
--Let me create a table using the newly created SQL Server Table partitioning schema. I suggest you refer Create table, and Identity Column to understand the below code.

USE [PartSample]
GO

CREATE TABLE [dbo].[SQL Insert](
	[EmpID] [int] IDENTITY(1,1) NOT NULL,
	[FirstName] [nvarchar](255) NULL,
	[LastName] [nvarchar](255) NULL,
	[Occupation] [nvarchar](255) NULL,
	[YearlyIncome] [float] NULL,
	[Sales] [float] NULL,
	[InsertDate] [datetime] NULL
) ON MonthWisePartition (InsertDate);


INSERT INTO [dbo].[SQL Insert] 
VALUES --('Imran', 'Khan', 'Skilled Professional', 15900, 100, GETDATE()),
      ('Doe', 'Lara', 'Management', 15000, 60, GETDATE())
      ,('Ramesh', 'Kumar', 'Professional', 65000, 630, DATEADD(month, 1, GETDATE()))

	  select* from [dbo].[SQL Insert] 

INSERT INTO [dbo].[SQL Insert] 
VALUES ('Tutorial', 'Gateway', 'Masters', 14500, 200, DATEADD(month, 4, GETDATE()))
      ,('Joe', 'Root', 'Management', 10000, 160, DATEADD(month, 3, GETDATE()))
	  ,('SQL', 'Tutorial', 'Management', 15000, 120, DATEADD(month, 2, GETDATE()))
	  ,('Jhon', 'Wick', 'Software Sales', 21000, 1160, DATEADD(month, -7, GETDATE()))
	  ,('Steve', 'Smith', 'App Sale', 13000, 2160, DATEADD(month, -6, GETDATE()))
	  ,('Kishore', 'Kumar', 'Admin', 120500, 310, DATEADD(month, -5, GETDATE()))
	  ,('Demi', 'Lovato', 'Professional', 193000, 1260, DATEADD(month, -4, GETDATE()))
	  ,('Madison', 'De', 'Management', 90000, 1090, DATEADD(month, -3, GETDATE()))
	  ,('Wang', 'Chung', 'Software Sale', 15000, 1560, DATEADD(month, -2, GETDATE()))
      ,('Dave', 'Jhones', 'Professional', 55000, 630, DATEADD(month, -1, GETDATE()))


	  select* from [dbo].[SQL Insert] 
	  order by insertdate desc


USE PartSample
SELECT part.partition_number AS [Partition Number],
		fle.name AS [Partition Name],
		part.rows AS [Number of Rows]
FROM sys.partitions AS part
JOIN SYS.destination_data_spaces AS dest ON
part.partition_number = dest.destination_id
JOIN sys.filegroups AS fle ON
dest.data_space_id = fle.data_space_id
WHERE OBJECT_NAME(OBJECT_ID) = 'SQL Insert'
