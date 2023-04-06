USE [CRM_Jurist_Analitics]
GO
/****** Object:  StoredProcedure [dbo].[sp_Load_Data_Incoming_OutcomingCalls]    Script Date: 4/6/2023 1:23:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER proc [dbo].[sp_Load_Data_Incoming_OutcomingCalls]
as 
begin
	-- SERVER NAME 
		declare @ServerCallWay nvarchar(200)
		select @ServerCallWay = [ServerLink]
		from [DIM].[Settings]
		where [ServerType] = N'CallWay';
	-- SERVER NAME

	-- FIND END DATE
	declare 
		@Direction1Status4 datetime,
		@Direction1Status9 datetime, 
		@Direction2Status4 datetime;

	select 
		@Direction1Status4 = D1S4.EndDate,
		@Direction1Status9 = D1S9.EndDate,
		@Direction2Status4 = D2S4.EndDate
	from 
	(
		select 
			max(EndDate) as EndDate
		from CRM_Jurist_Analitics.dbo.Incoming_Outcoming_Calls
		where DirectionId = 1 
		and StatusId = 4 
	) D1S4
	full join 
	(
		select 
			max(EndDate) as EndDate
		from CRM_Jurist_Analitics.dbo.Incoming_Outcoming_Calls
		where DirectionId = 1 
		and StatusId = 9
	) D1S9 on 1 = 1
	full join 
	(
		select 
			max(EndDate) as EndDate
		from CRM_Jurist_Analitics.dbo.Incoming_Outcoming_Calls
		where DirectionId = 2 
		and isnull(StatusId, 4) = 4 
	) D2S4 on 1 = 1;


	if @Direction1Status4 is null 
	begin
		declare @D1S4UI nvarchar(128);
		select 
			@D1S4UI = max(UniqueId) 
		from CRM_Jurist_Analitics.dbo.Incoming_Outcoming_Calls
		where DirectionId = 1 
		and StatusId = 4;

		declare @D1S4 table (EndDate datetime);
		declare @SQL_D1S4 nvarchar(max) = ' select 
												EndDate 
											from [' + @ServerCallWay + N'].[CallWay3].[dbo].CallStatistic 
											where UniqueId = ''' + @D1S4UI + '''';
												
		--print @SQL_D1S4;

	   insert into @D1S4 (EndDate) 
	   exec sp_executesql @SQL_D1S4;
	   select 
			@Direction1Status4 = EndDate
	   from @D1S4;
	end;

	if @Direction1Status9 is null 
	begin
		declare @D1S9UI nvarchar(128);
		select 
			@D1S9UI = max(UniqueId) 
		from CRM_Jurist_Analitics.dbo.Incoming_Outcoming_Calls
		where DirectionId = 1 
		and StatusId = 9;

		declare @D1S9 table (EndDate datetime);
		declare @SQL_D1S9 nvarchar(max) = ' select 
												EndDate 
											from [' + @ServerCallWay + N'].[CallWay3].[dbo].CallStatistic 
											where UniqueId = ''' + @D1S9UI + '''';
	   insert into @D1S9 (EndDate) 
	   exec sp_executesql @SQL_D1S9;
	   select 
			@Direction1Status9 = EndDate
	   from @D1S9;
	end;

	if @Direction2Status4 is null 
	begin
		declare @D2S4UI nvarchar(128);
		select 
			@D2S4UI = max(UniqueId) 
		from CRM_Jurist_Analitics.dbo.Incoming_Outcoming_Calls
		where DirectionId = 2 
		and isnull(StatusId, 4) = 4;

		declare @D2S4 table (EndDate datetime);
		declare @SQL_D2S4 nvarchar(max) = ' select 
												EndDate 
											from [' + @ServerCallWay + N'].[CallWay3].[dbo].CallStatistic 
											where UniqueId = ''' + @D2S4UI + '''';

	   insert into @D2S4 (EndDate) 
	   exec sp_executesql @SQL_D2S4;
	   select 
			@Direction2Status4 = EndDate
	   from @D2S4;
	end;


	--OLD
	--if @UniqueId is null 
	--begin
	--	declare @UniqueId_OUT table ([UniqueId] nvarchar(50))
	--	declare @sql nvarchar(max) = N' select min(UniqueId) as min_UniqueId from ['+@ServerCallWay+N'].[CallWay3].[dbo].CallStatistic '

	--	insert into @UniqueId_OUT ([UniqueId])
	--	exec sp_executesql @sql
	--	select @UniqueId = [UniqueId] from @UniqueId_OUT
	--end;
	--OLD


	
	declare @SQL_1_4 nvarchar(max) = N'
		select 
			sub.DirectionId,
			sub.StartDate,
			max(sub.EndDate),
			count(sub.Id),
			sub.StatusId
		from 
		(
			select 
				call_stat.DirectionId,
				cast(call_stat.StartDate as date) as StartDate,
				call_stat.EndDate,
				call_stat.Id,
				call_stat.StatusId
			from 
			(
				select 
					max(CallStatisticId) as CallStatisticId
				from ['+@ServerCallWay+N'].[CallWay3].[dbo].CallQueue sub 
				where sub.QueueId = 403
				group by 
					CallStatisticId
			) call_queue
			inner join ['+@ServerCallWay+N'].[CallWay3].[dbo].CallStatistic call_stat on call_queue.CallStatisticId = call_stat.Id 
			where call_stat.DirectionId  = 1
			and
			(
				call_stat.StatusId = 4
			)
			and call_stat.EndDate > cast(''' + convert(nvarchar(100), @Direction1Status4, 113) + ''' as datetime)
		) sub
		group by 
			sub.DirectionId,
			sub.StatusId,
			sub.StartDate;
	';

	declare @SQL_1_9 nvarchar(max) = N'
		select 
			sub.DirectionId,
			sub.StartDate,
			max(sub.EndDate),
			count(sub.Id),
			sub.StatusId
		from 
		(
			select 
				call_stat.DirectionId,
				cast(call_stat.StartDate as date) as StartDate,
				call_stat.EndDate,
				call_stat.Id,
				call_stat.StatusId
			from 
			(
				select 
					max(CallStatisticId) as CallStatisticId
				from ['+@ServerCallWay+N'].[CallWay3].[dbo].CallQueue sub 
				where sub.QueueId = 403
				group by 
					CallStatisticId
			) call_queue
			inner join ['+@ServerCallWay+N'].[CallWay3].[dbo].CallStatistic call_stat on call_queue.CallStatisticId = call_stat.Id 
			where call_stat.DirectionId  = 1
			and
			(
				call_stat.StatusId = 9
			)
			and call_stat.EndDate > cast(''' + convert(nvarchar(100), @Direction1Status9, 113) + ''' as datetime)
		) sub
		group by 
			sub.DirectionId,
			sub.StatusId,
			sub.StartDate;
	'

	declare @SQL_2_4 nvarchar(max) = N'
		select 
			sub.DirectionId,
			sub.StartDate,
			max(sub.EndDate),
			count(sub.Id),
			sub.StatusId
		from 
		(
			select 
				call_stat.DirectionId,
				cast(call_stat.StartDate as date) as StartDate,
				call_stat.EndDate,
				call_stat.Id,
				call_stat.StatusId
			from 
			(
				select 
					max(CallStatisticId) as CallStatisticId
				from ['+@ServerCallWay+N'].[CallWay3].[dbo].CallQueue sub 
				where sub.QueueId = 403
				group by 
					CallStatisticId
			) call_queue
			inner join ['+@ServerCallWay+N'].[CallWay3].[dbo].CallStatistic call_stat on call_queue.CallStatisticId = call_stat.Id 
			where call_stat.DirectionId  = 2
			and
			(
				call_stat.StatusId = 4
			)
			and call_stat.EndDate > cast(''' + convert(nvarchar(100), @Direction2Status4, 113) + ''' as datetime)
		) sub
		group by 
			sub.DirectionId,
			sub.StatusId,
			sub.StartDate;
	'

	--print @SQL_1_4;

	insert into dbo.Incoming_Outcoming_Calls
		(
			DirectionId,
			[Date], 
			EndDate,
			Quantity,
			StatusId
		)
	exec sp_executesql @SQL_1_4;

	insert into dbo.Incoming_Outcoming_Calls
		(
			DirectionId,
			[Date], 
			EndDate,
			Quantity,
			StatusId
		)
	exec sp_executesql @SQL_1_9;

	insert into dbo.Incoming_Outcoming_Calls
		(
			DirectionId,
			[Date], 
			EndDate,
			Quantity,
			StatusId
		)
	exec sp_executesql @SQL_2_4;
end

