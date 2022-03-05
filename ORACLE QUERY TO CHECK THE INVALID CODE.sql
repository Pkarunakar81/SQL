
select ACID,RCRE_TIME,to_char(RCRE_TIME ,'YYYY-MM-DD HH:MI:SS')
,to_char(RCRE_TIME ,'YYYY')
from tbaadm.alh 
where to_char(RCRE_TIME ,'YYYY')<1900;
--where acid='NL10029274';


select cif_id,to_char(date_of_birth ,'YYYY-MM-DD HH:MI:SS')
from TBAADM.CMG
--where to_char(date_of_birth ,'YYYY')=0001 and rownum<=5;
WHERE CIF_ID  IN ('1119203376','3554204815','3516700314');