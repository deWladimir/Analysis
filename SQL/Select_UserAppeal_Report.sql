-- declare 
-- 		@date_from date = N'2023-01-01',
-- 		@date_to date,
-- 		@appeal_type_ids nvarchar(250) = N'1',
-- 		@FakeList_QueueTypeId int = 3,
-- 		@AppealProcessing_Id int = 1;

----------------------------------------------------------------------------------------------------------------------
if @date_from is null 
begin
	set @date_from = dateadd(dd, -7, GETUTCDATE())
end;

if @date_to is null
begin
	set @date_to = GETUTCDATE();
end;
-------------------------------------------INSERT FILTERED TASKS--------------------------------------------------------------
declare @Tasks table 
(
	Id int, 
	executor_by_user_id nvarchar(128),
	status_id int,
	planned_date_end datetime,
	fact_date_start datetime
);

declare @TasksSQL nvarchar(max) = concat(N'
	select 
		tasks.Id,
		tasks.executor_by_user_id,
		tasks.status_id,
		tasks.planned_date_end,
		tasks.fact_date_start
	from CRM_Jurist_Analitics.dbo.Tasks tasks 
	inner join CRM_Jurist_Analitics.dbo.Employees emps on tasks.executor_by_user_id = emps.system_user_id
	left join CRM_Jurist_Analitics.dbo.Appeals appeals on tasks.appeal_id = appeals.Id 
	left join CRM_Jurist_Analitics.dbo.Sources sources on appeals.source_id = sources.id 
	where cast(tasks.created_at as date) between @date_from and @date_to ',
	case 
		when @appeal_type_ids is not null then 
		N' and sources.appeal_type_id in (select [value] from string_split(@appeal_type_ids, N'','')) '
		else SPACE(0)
	end,
	case 
		when isnull(@AppealProcessing_Id, 1) = 1 then SPACE(0)
		when @AppealProcessing_Id = 2 then 
		N' and 
		   (
			   isnull(appeals.is_without_applicant, 0) = 1
			   or 
			   (
				   appeals.source_id <> 4
				   and tasks.[type_id] = 5 
			   ) 
		   )'
		when @AppealProcessing_Id = 3 then 
		N' and not (
						isnull(appeals.is_without_applicant, 0) = 1
						or 
						(
							tasks.[type_id] = 5
							and appeals.source_id <> 4
						)
				   ) '
	end);

insert into @Tasks
(
	Id, 
	executor_by_user_id,
	status_id,
	planned_date_end,
	fact_date_start
)
exec sp_executesql @TasksSQL, N'@appeal_type_ids nvarchar(100), @date_from date, @date_to date', @appeal_type_ids, @date_from, @date_to;

--select * from @Tasks
-------------------------------------------COUNT TOTAL, PROCESSED AND OVERDUED TASKS--------------------------------------------------------------
declare 
	@total int = 0,
	@processed int = 0,
	@overdued int = 0;

select
	@total = total.Quantity,
	@processed = processed.Quantity,
	@overdued = overdued.Quantity
from 
(
	select 
		count(Id) as Quantity
	from @Tasks
) total 
full join 
(
	select 
		count(Id) as Quantity 
	from @Tasks
	where status_id = 4
) processed on 1 = 1
full join 
(
	select 
		count(Id) as Quantity 
	from @Tasks
	where status_id = 4
	and planned_date_end < ISNULL(fact_date_start, getdate())
) overdued on 1 = 1;

----------------------------------------------CALL-WAY DATA--------------------------------------------------------------------------------------------
declare @Employee_CallData table
(
	Employee_Id int,
	TalkTime decimal(19, 3),
	QueueId int,
	Id int,
	StartDate date
);

declare @SQL_CallData nvarchar(max) = concat(N'
select 
	emp_phones.Employees_Id,
	phone_stats.TalkTime,
	phone_stats.QueueId,
	phone_stats.Id,
	cast(phone_stats.StartDate as date) as StartDate
from CRM_Jurist_Analitics.dbo.Employees_Phone_Stats phone_stats with (nolock)
inner join CRM_Jurist_Analitics.dbo.Employees_Phone emp_phones on phone_stats.Phone = emp_phones.Phone_CallWay collate Cyrillic_General_CI_AS
where cast(phone_stats.StartDate as date) >= @date_from
and cast(phone_stats.EndDate as date) <= @date_to 
and datepart(weekday, phone_stats.StartDate) not in (7, 1)',
case 
	when isnull(@FakeList_QueueTypeId, 1) = 1 then SPACE(0)
	when @FakeList_QueueTypeId = 2 then N' and phone_stats.QueueId = 397 '
	when @FakeList_QueueTypeId = 3 then N' and phone_stats.QueueId = 403 '
end,
case 
	when @FakeList_QueueTypeId = 3 and @AppealProcessing_Id = 2 then 
		 N' and phone_stats.IsDirected  = 1
		    and phone_stats.DirectedQueueId = 397 '
	when @FakeList_QueueTypeId = 3 and @AppealProcessing_Id = 3 then 
		N' and isnull(phone_stats.IsDirected, 0) <> 1 '
	else 
		SPACE(0)
end);

insert into @Employee_CallData
(
	Employee_Id,
	TalkTime,
	QueueId,
	Id,
	StartDate
)
exec sp_executesql @SQL_CallData, N'@date_from date, @date_to date', @date_from, @date_to;

-------------------------------------------------------------MAIN OUTPUT----------------------------------------------------------
select 
	emps.PIB,
	total.Quantity as total,
	processed.Quantity as processed,
	case 
		when overdued.Quantity = 0 then concat(overdued.Quantity, N' (0.00%)')
		else 
			concat(overdued.Quantity, 
				   iif(total.Quantity = 0, N' (0.00%)', concat(N' (', cast(overdued.Quantity / 1.0 / total.Quantity * 100 as decimal(5, 2)), N'%)'))
			      )
	end as overdued,
	calldata.Quantity as calls,
	calldata.TalkTime as talktime,
	isnull(avg_call_data.AVG_QUANTITY, 0) as avg_quantity,
	1 as [Index]
from CRM_Jurist_Analitics.dbo.Employees emps 
outer apply 
(
	select 
		count(total.Id) as Quantity 
	from @Tasks total
	where total.executor_by_user_id = emps.system_user_id
) total 
outer apply 
(
	select 
		count(processed.Id) as Quantity 
	from @Tasks processed
	where processed.executor_by_user_id = emps.system_user_id
	and processed.status_id = 4
) processed
outer apply 
(
	select 
		count(overdued.Id) as Quantity 
	from @Tasks overdued
	where overdued.executor_by_user_id = emps.system_user_id
	and overdued.status_id = 4
	and overdued.planned_date_end < isnull(overdued.fact_date_start, GETDATE())
) overdued
outer apply 
(
	select 
		isnull(sum(sub.Quantity), 0) as Quantity,
		isnull(avg(sub.TalkTime), 0) as TalkTime
	from 
	(
		select 
			isnull(count(callway.Id), 0) as Quantity,
			isnull(avg(callway.TalkTime), 0) as TalkTime
		from @Employee_CallData callway
		where callway.Employee_Id = emps.Id
		group by 
			callway.QueueId
	) sub 
) calldata
outer apply 
(
	select 
		isnull(avg(main.AVG_QUANTITY), 0) as AVG_QUANTITY
	from 
	(
		select 
			avg(sub.Quantity) as AVG_QUANTITY,
			sub.QueueId
		from 
		(
			select 
				count(calldata.Id) as Quantity,
				calldata.QueueId
			from @Employee_CallData calldata
			where calldata.Employee_Id = emps.Id 
			group by 
				calldata.StartDate,
				calldata.QueueId
		) sub 
		group by 
			sub.QueueId
	) main
) avg_call_data

union all

select 
	N'Усього',
	@total,
	@processed,
	cast(@overdued as nvarchar(50)),
	null,
	null,
	null,
	0

order by [Index], total desc;