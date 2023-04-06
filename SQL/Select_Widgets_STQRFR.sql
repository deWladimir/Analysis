-- declare @date_from date,
-- 		@date_to date,
-- 		@region_id int,
-- 		@widget nvarchar(20) = N'registration';

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
------------------------------------------------------------------------------
declare 
	@sum_val int;

if @widget = N'statuses'
begin 
	select 
		@sum_val = count(claim.Id)
	from EAR_CRM.dbo.Claim claim 
	left join EAR_CRM.dbo.Task task on claim.Id = task.Claim_Id
	where cast(claim.RegistrationDatetime as date) between @date_from and @date_to
	and claim.Region_Id = isnull(@region_id, claim.Region_Id)
	and claim.Informational = 0
	and task.TaskState_EKey in (1, 2, 3, 4, 5, 6);


	select 
		dim.Id,
		dim.Reportcode as [Name],
		isnull(sub.[Value], 0) as [Value],
		case 
			when dim.Id <> 35 
			then 
				case 
					when @sum_val <> 0 then cast(isnull(sub.[Value], 0) / 1.0 / @sum_val * 100 as decimal(5, 2)) 
					else 0 
				end 
			else null
		end as [Percent],
		case 
			when dim.Id = 35 then 0 
			when dim.Id = 6 then 2
			else 1 
		end as [Index]
	from 
	(
		select 
			case task_states.TaskState_EKey
				when 6 then 36
				else task_states.TaskState_EKey
			end as TaskState_EKey,
			task_states.[Value]
		from 
		(
			select 
				task.TaskState_EKey,
				count(claim.Id) as [Value]
			from EAR_CRM.dbo.Claim claim 
			left join EAR_CRM.dbo.Task task on claim.Id = task.Claim_Id
			where cast(claim.RegistrationDatetime as date) between @date_from and @date_to
			and task.TaskState_EKey in (1, 2, 3, 4, 5, 6)
			and claim.Region_Id = isnull(@region_id, claim.Region_Id)
			and claim.Informational = 0
			group by 
				task.TaskState_EKey
		) task_states

	union all 

	select 
		6,
		count(claim.Id)
	from EAR_CRM.dbo.Claim claim 
	left join EAR_CRM.dbo.Task task on claim.Id = task.Claim_Id 
	where cast(claim.RegistrationDatetime as date) between @date_from and @date_to
	and task.TaskState_EKey between 1 and 6
	and claim.ControlDatetime < GETDATE()
	and claim.Region_Id = isnull(@region_id, claim.Region_Id)
	and claim.Informational = 0

	union all 

	select 
		35,
		@sum_val
	) sub 
	right join [Reports].[DIM].[Reportcode] dim on sub.TaskState_EKey = dim.Id
	where dim.Id between 1 and 6
	or dim.Id in (35, 36)
	order by [Index] asc;

end;

else if @widget = N'types'
begin
	select 
		@sum_val = count(claim.Id)
	from EAR_CRM.dbo.Claim claim 
	left join EAR_CRM.dbo.ClaimType claim_type on claim.ClaimType_Id = claim_type.Id 
	where claim_type.Parent_Id in (1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 77, 78, 4, 88)
	and claim.Region_Id = isnull(@region_id, claim.Region_Id)
	and claim.Informational = 0
	and cast(claim.RegistrationDatetime as date) between @date_from and @date_to;

	select 
		dim.Id,
		dim.[Reportcode] as [Name],
		isnull(sub.[Value], 0) as [Value],
		case
			when @sum_val <> 0 then cast(isnull(sub.[Value], 0) / 1.0 / @sum_val * 100 as decimal(5, 2)) 
			else 0 
		end as [Percent]
	from
	(
		select 
			case 
				when claims.Parent_Id = 1 then 7
				when claims.Parent_Id = 2 then 8
				when claims.Parent_Id = 3 then 9
				when claims.Parent_Id = 5 then 10
				when claims.Parent_Id = 6 then 11
				when claims.Parent_Id = 7 then 12
				when claims.Parent_Id = 8 then 13
				when claims.Parent_Id = 9 then 14
				when claims.Parent_Id = 10 then 15
				when claims.Parent_Id = 11 then 16
				when claims.Parent_Id = 77 then 17
				when claims.Parent_Id = 78 then 18
				when claims.Parent_Id = 4 then 33
				when claims.Parent_Id = 88 then 34
			end as Reportcode_Id,
			claims.[Value]
		from 
		(
			select 
				claim_type.Parent_Id,
				count(claim.Id) as [Value]
			from EAR_CRM.dbo.Claim claim 
			left join EAR_CRM.dbo.ClaimType claim_type on claim.ClaimType_Id = claim_type.Id 
			where claim_type.Parent_Id in (1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 77, 78, 4, 88)
			and claim.Region_Id = isnull(@region_id, claim.Region_Id)
			and claim.Informational = 0
			and cast(claim.RegistrationDatetime as date) between @date_from and @date_to
			group by 
				claim_type.Parent_Id
		) claims
	) sub
	right join Reports.DIM.Reportcode dim on sub.Reportcode_Id = dim.Id
	where dim.Id between 7 and 18
	or dim.Id in (33, 34);
	
end;

else if @widget = N'quality'
begin
	select 
		@sum_val = count(claim.Id)
	from EAR_CRM.dbo.Claim claim 
	left join EAR_CRM.dbo.ApplicantFeedback appl_feed on claim.Id = appl_feed.Claim_Id
	where appl_feed.Grade in (1, 2, 3, 4, 5)
	and appl_feed.ConnectionState_EKey = 2
	and claim.Region_Id = isnull(@region_id, claim.Region_Id)
	and claim.Informational = 0
	and cast(claim.RegistrationDatetime as date) between @date_from and @date_to;

	select 
		dim.Id,
		dim.[Reportcode] as [Name],
		isnull(sub.[Value], 0) as [Value],
		case
			when @sum_val <> 0 then cast(isnull(sub.[Value], 0) / 1.0 / @sum_val * 100 as decimal(5, 2)) 
			else 0 
		end as [Percent]
	from 
	(
		select 
			case 
				when claims.Grade = 1 then 28
				when claims.Grade = 2 then 29
				when claims.Grade = 3 then 30
				when claims.Grade = 4 then 31
				when claims.Grade = 5 then 32
			end as Reportcode_Id,
			claims.[Value]
		from 
		(	
			select 
				appl_feed.Grade,
				count(claim.Id) as [Value]
			from EAR_CRM.dbo.Claim claim 
			left join EAR_CRM.dbo.ApplicantFeedback appl_feed on claim.Id = appl_feed.Claim_Id
			where appl_feed.Grade in (1, 2, 3, 4, 5)
			and appl_feed.ConnectionState_EKey = 2
			and claim.Region_Id = isnull(@region_id, claim.Region_Id)
			and claim.Informational = 0
			and cast(claim.RegistrationDatetime as date) between @date_from and @date_to
			group by 
				appl_feed.Grade
		) claims
	) sub
	right join Reports.DIM.Reportcode dim on sub.Reportcode_Id = dim.Id
	where dim.Id in (28, 29, 30, 31, 32);
end;

else if @widget = N'result'
begin
	select
		@sum_val = count(claim.Id)
	from EAR_CRM.dbo.Claim claim 
	left join EAR_CRM.dbo.ApplicantFeedback appl_feed on claim.Id = appl_feed.Claim_Id
	where appl_feed.ConnectionState_EKey in (1, 2)
	and claim.Region_Id = isnull(@region_id, claim.Region_Id)
	and claim.Informational = 0
	and cast(claim.RegistrationDatetime as date) between @date_from and @date_to;

	select 
		dim.Id,
		dim.[Reportcode] as [Name],
		isnull(sub.[Value], 0) as [Value],
		case
			when @sum_val <> 0 then cast(isnull(sub.[Value], 0) / 1.0 / @sum_val * 100 as decimal(5, 2)) 
			else 0 
		end as [Percent]
	from 
	(
		select 
			case 
				when claims.ConnectionState_EKey = 1 then 27
				when claims.ConnectionState_EKey = 2 then 26
			end as Reportcode_Id,
			claims.[Value]
		from 
		(	
			select 
				appl_feed.ConnectionState_EKey,
				count(claim.Id) as [Value]
			from EAR_CRM.dbo.Claim claim 
			left join EAR_CRM.dbo.ApplicantFeedback appl_feed on claim.Id = appl_feed.Claim_Id
			where appl_feed.ConnectionState_EKey in (1, 2)
			and claim.Region_Id = isnull(@region_id, claim.Region_Id)
			and claim.Informational = 0
			and cast(claim.RegistrationDatetime as date) between @date_from and @date_to
			group by 
				appl_feed.ConnectionState_EKey
		) claims
	) sub
	right join Reports.DIM.Reportcode dim on sub.Reportcode_Id = dim.Id
	where dim.Id in (27, 26);
end;

else if @widget = N'feedback'
begin
	select
		@sum_val = count(claim.Id)
	from EAR_CRM.dbo.Claim claim 
	where claim.Region_Id = isnull(@region_id, claim.Region_Id)
	and claim.Informational = 0
	and cast(claim.RegistrationDatetime as date) between @date_from and @date_to
	and claim.AnswerForm_EKey in (1, 2, 3);

	select 
		dim.Id,
		dim.[Reportcode] as [Name],
		isnull(sub.[Value], 0) as [Value],
		case
			when @sum_val <> 0 then cast(isnull(sub.[Value], 0) / 1.0 / @sum_val * 100 as decimal(5, 2)) 
			else 0 
		end as [Percent]
	from 
	(
		select 
			case 
				when claims.AnswerForm_EKey = 1 then 19
				when claims.AnswerForm_EKey = 2 then 20
				when claims.AnswerForm_EKey = 3 then 21
			end as Reportcode_Id,
			claims.[Value]
		from 
		(
			select 
				claim.AnswerForm_EKey,
				count(claim.Id) as [Value]
			from EAR_CRM.dbo.Claim claim 
			where claim.Region_Id = isnull(@region_id, claim.Region_Id)
			and claim.Informational = 0
			and cast(claim.RegistrationDatetime as date) between @date_from and @date_to
			and claim.AnswerForm_EKey in (1, 2, 3)
			group by 
				claim.AnswerForm_EKey
		) claims
	) sub 
	right join Reports.DIM.Reportcode dim on sub.Reportcode_Id = dim.Id
	where dim.Id in (19, 20, 21);
end;

else if @widget = N'registration'
begin
	select 
		@sum_val = count(claim.Id)
	from EAR_CRM.dbo.Claim claim 
	left join EAR_CRM.dbo.Appeal appeal on claim.Appeal_Id = appeal.Id 
	where  claim.Region_Id = isnull(@region_id, claim.Region_Id)
	and claim.Informational = 0
	and cast(claim.RegistrationDatetime as date) between @date_from and @date_to
	and appeal.ReceiptSource_Ekey in (1, 4, 7, 6);

	select 
		dim.Id,
		dim.[Reportcode] as [Name],
		isnull(sub.[Value], 0) as [Value],
		case
			when @sum_val <> 0 then cast(isnull(sub.[Value], 0)  / 1.0 / @sum_val * 100 as decimal(5, 2)) 
			else 0 
		end as [Percent]
	from 
	(
		select 
			case 
				when claims.ReceiptSource_Ekey = 1 then 22
				when claims.ReceiptSource_Ekey = 4 then 23
				when claims.ReceiptSource_Ekey = 7 then 24
				when claims.ReceiptSource_Ekey = 6 then 25
			end as Reportcode_Id,
			claims.[Value]
		from 
		(
			select 
				appeal.ReceiptSource_Ekey,
				count(claim.Id) as [Value]
			from EAR_CRM.dbo.Claim claim 
			left join EAR_CRM.dbo.Appeal appeal on claim.Appeal_Id = appeal.Id 
			where  claim.Region_Id = isnull(@region_id, claim.Region_Id)
			and claim.Informational = 0
			and cast(claim.RegistrationDatetime as date) between @date_from and @date_to
			and appeal.ReceiptSource_Ekey in (1, 4, 7, 6)
			group by 
				appeal.ReceiptSource_Ekey
		) claims 
	) sub 
	right join Reports.DIM.Reportcode dim on dim.Id = sub.Reportcode_Id
	where dim.Id in (22, 23, 24, 25);
end;

