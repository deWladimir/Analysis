USE [CRM_Jurist_Analitics]
GO
/****** Object:  StoredProcedure [dbo].[sp_Load_Employees_Phone_Stats]    Script Date: 4/6/2023 1:24:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER proc [dbo].[sp_Load_Employees_Phone_Stats]
as 
begin

	if object_id(N'tempdb..#emps_phone_max_date') is not null
	begin
		drop table #emps_phone_max_date;
	end;
	else 
	begin
		create table #emps_phone_max_date
		(
			Phone nvarchar(14) collate Cyrillic_General_CI_AS,
			EndDate datetime
		);

		insert into #emps_phone_max_date
		(
			Phone,
			EndDate
		)
		select 
			phones.Phone_CallWay,
			isnull(max(phone_stats.EndDate), GETDATE())
		from CRM_Jurist_Analitics.dbo.Employees_Phone phones
		left join CRM_Jurist_Analitics.dbo.Employees_Phone_Stats phone_stats on phones.Phone_CallWay collate Cyrillic_General_CI_AS = phone_stats.Phone
		group by 
			phones.Phone_CallWay
	end;

	declare @Phones_CallWay nvarchar(max) = stuff((select N','+''''+[Phone_CallWay]+''''
												from dbo.Employees_Phone 
												group by Phone_CallWay
											for xml path('')), 1,1,N'')
	--select @Phones_CallWay

	declare @ServerCallWay nvarchar(200)
	select @ServerCallWay = [ServerLink]
	from [DIM].[Settings]
	where [ServerType] = N'CallWay'



	declare @sql_result nvarchar(max) = N'
		select --top 7500
			oper_crm.Phone,
			call_queue.QueueId,
			call_stat.StartDate,
			call_stat.EndDate,
			call_stat.UniqueId,
			call_stat.TalkTime,
			case 
				when directed.Id <> isnull(call_queue.Id, directed.Id) then 1
				else 0
			end as IsDirected,
			case 
				when directed.Id <> isnull(call_queue.Id, directed.Id) then directed.QueueId
				else null
			end as DirectedQueueId
		from ['+@ServerCallWay+N'].[CallWay3].[dbo].OperatorCrm oper_crm 
		inner join ['+@ServerCallWay+N'].[CallWay3].[dbo].CallQueue call_queue on oper_crm.Id = call_queue.OperatorId 
																				  and oper_crm.Phone in (
																									   '+@Phones_CallWay+'
																									    )
																				  and call_queue.QueueId in (397, 403)
		inner join #emps_phone_max_date emps_max_date on oper_crm.Phone = emps_max_date.Phone
		outer apply 
		(
			select top 1
				CallQueue.QueueId,
				CallQueue.Id,
				CallQueue.OperatorId
			from ['+@ServerCallWay+N'].[CallWay3].[dbo].CallQueue
			where CallQueue.CallStatisticId = call_queue.CallStatisticId
			and CallQueue.QueueId <> call_queue.QueueId
			order by CallQueue.Id desc
		) directed
		inner join ['+@ServerCallWay+N'].[CallWay3].[dbo].CallStatistic call_stat on call_queue.CallStatisticId = call_stat.Id
																					 and call_stat.StatusId = 4
																					 and call_stat.DirectionId = 1
																					 and call_stat.EndDate > emps_max_date.EndDate
																		
		order by call_stat.UniqueId asc;
	'

	insert into dbo.Employees_Phone_Stats
	(
		[Phone],
		QueueId,
		StartDate,
		EndDate,
		UniqueId,
		TalkTime,
		IsDirected,
		DirectedQueueId
	)
	exec sp_executesql @sql_result

end;

