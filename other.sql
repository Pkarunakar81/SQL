AS
BEGIN



declare @alpha INT

--set @var = '##2gh78554'
set @alpha = PATINDEX('%[a-zA-Z]%',@VAR)
BEGIN
while (@alpha >0)
BEGIN
set @alpha=stuff(@var,@alpha,1,'')
set @alpha =PATINDEX('%[a-zA-Z]%',@VAR)
END
END
RETURN ISNULL(@VAR,0)
END

