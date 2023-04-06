-- declare @date_from date,
--  		@date_to date,
--  		@reportcode_id int = 32;

if @date_from is null 
begin
	set @date_from = datefromparts(year(getdate()), month(getdate()), 1);
end;

if @date_to is null 
begin
	set @date_to = getdate();
end;

select 
	@date_from = cast(@date_from as date),
	@date_to = cast(@date_to as date);
-----------------------------------------------------------------------------------
declare @JoinClause nvarchar(2000),
		@WhereClause nvarchar(2000);

select 
	@JoinClause = JoinClause,
	@WhereClause = WhereClause
from Reports.DIM.Reportcode 
where Id = iif(@reportcode_id = 0 or @reportcode_id is null, 35, @reportcode_id);

--select 
--	@JoinClause,
--	@WhereClause;

declare @SQL nvarchar(max) = concat(N'
select 
	reg.Id,
	reg.[Name],
	count(sub.Id) as [Value]
from General.DIM.Regions reg 
outer apply 
(
	select 
		claim.Id
	from EAR_CRM.dbo.Claim claim ', 
	@JoinClause, N'
	where claim.Informational = 0 
	and cast(claim.RegistrationDatetime as date) between @date_from and @date_to
	and claim.Region_Id = reg.Id ',
	N' and ' + @WhereClause, N'
) sub
group by 
	reg.Id,
	reg.[Name]
')

--print @SQL;

exec sp_executesql @SQL, N'@date_from date, @date_to date', @date_from = @date_from, @date_to = @date_to;

