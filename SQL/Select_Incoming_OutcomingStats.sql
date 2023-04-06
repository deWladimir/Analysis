-- declare
-- 	@date_from date = N'2022-04-04',
-- 	@date_to date = N'2022-12-02',
-- 	@param nvarchar(7) = N'year';

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
	pers.Id,
	pers.[Value] as [Name],
	isnull(incoming.Quantity, 0) as Incoming,
	isnull(outcoming.Quantity, 0) as Outcoming,
	isnull(missed.Quantity, 0) as Missed
from #periods pers 
left join 
(
 select ',
	case @param
		when N'day' then N' calls_stats.[Date] '
		when N'month' then N' month(calls_stats.[Date]) '
		when N'year' then N' year(calls_stats.[Date]) '
	 end, N' as [Value],
	 sum(calls_stats.Quantity) as Quantity
 from dbo.Incoming_Outcoming_Calls calls_stats
 where calls_stats.DirectionId = 1
 and isnull(calls_stats.StatusId, 0) <> 9
 and datepart(weekday, calls_stats.[Date]) not in (7, 1)
 and ',case @param
		when N'day' then N' calls_stats.[Date] between @date_from and @date_to '
		when N'month' then N' year(calls_stats.[Date]) = year(getutcdate()) '
		else N' 1 = 1 '
	   end, N'
 group by ',
	case @param
		when N'day' then N' calls_stats.[Date] '
		when N'month' then N' month(calls_stats.[Date]) '
		when N'year' then N' year(calls_stats.[Date]) '
	 end, N'
) incoming on pers.Id = incoming.[Value]
left join 
(
 select ',
	case @param
		when N'day' then N' calls_stats.[Date] '
		when N'month' then N' month(calls_stats.[Date]) '
		when N'year' then N' year(calls_stats.[Date]) '
	 end, N' as [Value],
	 sum(calls_stats.Quantity) as Quantity
 from dbo.Incoming_Outcoming_Calls calls_stats
 where calls_stats.DirectionId = 2
 and datepart(weekday, calls_stats.[Date]) not in (7, 1)
 and ',case @param
		when  N'day' then N' calls_stats.[Date] between @date_from and @date_to '
		when  N'month' then N' year(calls_stats.[Date]) = year(getutcdate()) '
		else N' 1 = 1 '
	   end, N'
 group by ',
	case @param
		when N'day' then N' calls_stats.[Date] '
		when N'month' then N' month(calls_stats.[Date]) '
		when N'year' then N' year(calls_stats.[Date]) '
	 end, N'
) outcoming on pers.Id = outcoming.[Value]
left join 
(
 select ',
	case @param
		when N'day' then N' calls_stats.[Date] '
		when N'month' then N' month(calls_stats.[Date]) '
		when N'year' then N' year(calls_stats.[Date]) '
	 end, N' as [Value],
	 sum(calls_stats.Quantity) as Quantity
 from dbo.Incoming_Outcoming_Calls calls_stats
 where calls_stats.DirectionId = 1
 and isnull(calls_stats.StatusId, 0) = 9
 and datepart(weekday, calls_stats.[Date]) not in (7, 1)
 and ',case @param
		when N'day' then N' calls_stats.[Date] between @date_from and @date_to '
		when N'month' then N' year(calls_stats.[Date]) = year(getutcdate()) '
		else N' 1 = 1 '
	   end, N'
 group by ',
	case @param
		when N'day' then N' calls_stats.[Date] '
		when N'month' then N' month(calls_stats.[Date]) '
		when N'year' then N' year(calls_stats.[Date]) '
	 end, N'
) missed on pers.Id = missed.[Value] ',
iif(@param = N'day', N' where datepart(weekday, convert(datetime, pers.[Value], 104)) not in (7, 1) ', N''), N'
order by ', iif(@param = N'month', N'cast(pers.Id as int)', N'pers.Id')
);
--print @SQL;
exec sp_executesql @SQL, N'@date_from date, @date_to date', @date_from = @date_from, @date_to = @date_to;