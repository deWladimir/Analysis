use CRM_Jurist_Analitics;

-- declare 
--  	@param nvarchar(7) = N'day',
--  	@date_from date = N'2022-10-10',
--  	@date_to date = N'2022-12-01',
--  	@districts_id nvarchar(max),
-- 	    @sources_id nvarchar(max) = N'1';

declare @max_district int = (select max(Id) from CRM_1551_Analitics.dbo.Districts) + 1,
		@all_district int = (select max(Id) from CRM_1551_Analitics.dbo.Districts) + 2;

select 
	@date_from = cast(@date_from as date),
	@date_to = cast(@date_to as date);

if object_id(N'tempdb..#periods') is not null 
begin
	drop table #periods;
end;

create table #periods
(
	Id nvarchar(10), 
	[Value] nvarchar(20)
);

if @param = N'day'
begin
	declare @date date = @date_from;
	--------------------------------
	while @date <= @date_to
	begin
		insert into #periods
		(
			[Id],
			[Value]
		)
		select 
			@date, 
			convert(nvarchar(10), @date, 104);

		set @date = dateadd(dd, 1, @date);
	end;
end;

if @param = N'month'
begin
	insert into #periods
	(
			[Id],
			[Value]
	)
	values 
		(1, N'Січень'),
		(2, N'Лютий'),
		(3, N'Березень'),
		(4, N'Квітень'),
		(5, N'Травень'),
		(6, N'Червень'),
		(7, N'Липень'),
		(8, N'Серпень'),
		(9, N'Вересень'),
		(10, N'Жовтень'),
		(11, N'Листопад'),
		(12, N'Грудень')
end;

if @param = N'year'
begin
	declare @min_year int,
			@max_year int;
	--------------------------------------
	select 
		@min_year = min(year(created_at)),
		@max_year = max(year(created_at))
	from dbo.Appeals;
	--------------------------------------
	while @min_year <= @max_year
	begin
		insert into #periods
		(
			Id, 
			[Value]
		)
		select 
			@min_year,
			@min_year;

		set @min_year = @min_year + 1;
	end;
end;


declare @SQL nvarchar(max) = concat(N'
select 
	periods.Id,
	periods.[Value] as [Name],
	count(appeal.[Value]) as Quantity
from #periods periods 
left join
(
	select ',
		case @param
			when N'day' then N' cast(appeal.created_at as date) '
			when N'month' then N' month(appeal.created_at) '
			when N'year' then N' year(appeal.created_at) '
		end, N' as [Value]
	from dbo.Appeals appeal
	left join dbo.Applicants cant on appeal.Applicant_Id = cant.Id 
	left join CRM_1551_Analitics.dbo.Buildings builds on cant.building_id = builds.id 
	where isnull(appeal.is_without_applicant, 0) <>  1
	and appeal.source_id not in (6, 7)
	and 
	(
		appeal.source_id in (select [value] from string_split(@sources_id, N'',''))
		or @sources_id is null
	)
	and 
	(
		isnull(builds.district_id, @max_district) in (select [value] from string_split(@districts_id, N'',''))
		or @districts_id is null
		or @all_district in (select [value] from string_split(@districts_id, N'',''))
	)
	and 
	(
		', case @param
				when N'day' then N' cast(appeal.created_at as date) between @date_from and @date_to '
				when N'month' then N' year(appeal.created_at) = year(getutcdate()) '
				when N'year' then N' 1 = 1 '
			end, 
	N'
	)

	union all 

	select ',
		case @param
			when N'day' then N' cast(appeal.created_at as date) '
			when N'month' then N' month(appeal.created_at) '
			when N'year' then N' year(appeal.created_at) '
		end, N' as [Value]
	from dbo.Appeals appeal
	inner join CRM_1551_Analitics.dbo.Districts distr on appeal.district_id = distr.Id 
	where isnull(appeal.is_without_applicant, 0) <>  1
	and appeal.source_id = 7
	and 
	(
		appeal.source_id in (select [value] from string_split(@sources_id, N'',''))
		or @sources_id is null
	)
	and 
	(
		isnull(distr.id, @max_district) in (select [value] from string_split(@districts_id, N'',''))
		or @districts_id is null
		or @all_district in (select [value] from string_split(@districts_id, N'',''))
	)
	and 
	(
		', case @param
				when N'day' then N' cast(appeal.created_at as date) between @date_from and @date_to '
				when N'month' then N' year(appeal.created_at) = year(getutcdate()) '
				when N'year' then N' 1 = 1 '
			end, 
	N'
	)
) appeal on periods.Id = appeal.[Value]
group by  
   periods.Id
  ,periods.[Value]
order by ', iif(@param = N'month', N'cast(periods.Id as int)', N'periods.Id')
);
--print @SQL;
exec sp_executesql @SQL, N'@max_district int, @all_district int, @date_from date, @date_to date, @sources_id nvarchar(max), @districts_id nvarchar(max)',
						   @max_district = @max_district,
						   @all_district = @all_district,
						   @date_from = @date_from,
						   @date_to = @date_to,
						   @sources_id = @sources_id,
						   @districts_id = @districts_id;