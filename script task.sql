--alter table Customer add Village VARCHAR(100)
--truncate table Customer
--truncate table tmp_upi_kar

drop table Customer
drop table tmp_upi_kar

select * into tmp_upi_kar
from 
QR_UPI_TRAN_DETAILS
where 1=2


Create table dbo.Customer(
id int,
name VARCHAR(100),
dob date,
FileName VARCHAR(100),
SheetName VARCHAR(100))

create table test_kaR (Name varchar(50),Value int)

insert into test_kaR values('Karuna','100')

select * from test_kaR



