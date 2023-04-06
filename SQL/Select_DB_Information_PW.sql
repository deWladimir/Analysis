--declare @road_ids nvarchar(max),
--		@object_distance_ids nvarchar(max),
--		@object_type_id int = 1,
--		@work_type_ids nvarchar(max),
--		@contractor_ids nvarchar(max),
--		@region_ids nvarchar(max),
--		@date_from date,
--		@date_to date,
--		@finance_year int = 2022;



declare @ObjectRealizationDistance table 
(
	Id int,
	ObjectType_Id int,
	ObjectRealization_Id int
);

declare @maxFrontOrder int;
select 
	@maxFrontOrder = max(FrontOrder)
from EAR.LINK.ObjectTypeSubWorkType;
--getting all correct object_distances due to filters and plan_version logic
insert into @ObjectRealizationDistance
(
	Id,
	ObjectType_Id,
	ObjectRealization_Id
)
select 
	obj_dist.Id,
	obj_dist.ObjectType_Id,
	obj_dist.ObjectRealization_Id
from EAR.dbo.ObjectRealizationDistance obj_dist
inner join EAR.dbo.ObjectRealization obj_real on obj_dist.ObjectRealization_Id = obj_real.Id 
inner join EAR.dbo.ObjectProposal obj_prop on obj_dist.ObjectProposal_Id = obj_prop.Id
inner join EAR.dbo.ObjectInPlan obj_plan on obj_prop.Id = obj_plan.ObjectProposal_Id 
inner join EAR.dbo.PlanVersion plan_ver on obj_plan.PlanVersion_Id = plan_ver.Id 
outer apply 
(
	select top 1 
		dist_plan.Id
	from EAR.dbo.RealizationDistanceSubWorkPlan dist_plan 
	inner join EAR.dbo.RealizationDistanceSubWorkFact dist_fact on dist_plan.Id = dist_fact.RealizationDistanceSubWorkPlan_Id
	where dist_plan.ObjectRealizationDistance_Id = obj_dist.Id 
	and dist_fact.ReportDate between @date_from and @date_to
) rep_date
outer apply 
(
	select top 1
		contr.Id
	from EAR.LINK.ContractRoadSegment contr_link
	inner join EAR.dbo.[Contract] contr on contr_link.Contract_Id = contr.Id
	where contr_link.ObjectRealization_Id = obj_real.Id 
	and contr.Contractor_Id in (select [value] from string_split(@contractor_ids, N','))
) contractor
where plan_ver.Id = (select 
						max(sub.Id) 
					 from EAR.dbo.PlanVersion sub 
					 where year(sub.FinanceYear) = @finance_year
					 and sub.State_EKey = 5)
and 
(
	rep_date.Id is not null
	or 
	(
		@date_from is null
		and @date_to is null
	)
)
and 
(
	contractor.Id is not null
	or @contractor_ids is null
)
and 
(
	obj_prop.Road_Id in (select [value] from string_split(@road_ids, N','))
	or @road_ids is null
)
and 
(
	obj_dist.Id in (select [value] from string_split(@object_distance_ids, N','))
	or @object_distance_ids is null
)
and 
(
	obj_dist.ObjectType_Id = @object_type_id
	or @object_type_id is null
)
and 
(
	obj_real.WorkType_Id in (select [value] from string_split(@work_type_ids, N','))
	or @work_type_ids is null
)
and 
(
	obj_prop.Region_Id in (select [value] from string_split(@region_ids, N','))
	or @region_ids is null
)
and obj_dist.DistanceState_EKey <> 3;



--for DB information
select 
		subwork.[Name] as SubWork_Name,
		Unit.[Name] as Unit_Name,
		null as PlanKMU,
		work_plan.AmountPlan1 as [Plan],
		work_fact.AmountFact1 as Fact,
		by_day_f.Quantity as ByDay,
		case 
			when by_day.Quantity is null then null
			else 
				case when by_day_f.Quantity = 0 then 0 
				else  round((work_plan.AmountPlan1 - work_fact.AmountFact1) / 1.0 / (by_day_f.Quantity), 0) 
				end
		end as DiffDays,
		1 as [Index],
		isnull(objtype_subworktype.FrontOrder, @maxFrontOrder + 1) as FrontOrder
from 
(
	select 
		ObjectType_Id,
		SubWorkType_Id,
		min(FrontOrder) as FrontOrder
	from EAR.LINK.ObjectTypeSubWorkType
	where ObjectType_Id in (select 
								sub.ObjectType_Id
							from @ObjectRealizationDistance sub
							group by 
								sub.ObjectType_Id)
	group by 
		ObjectType_Id,
		SubWorkType_Id
) objtype_subworktype
inner join EAR.DIM.SubWorkType subwork on objtype_subworktype.SubWorkType_Id = subwork.Id
									   and subwork.WorkDirection_Id = 1
left join EAR.DIM.Unit on subwork.Unit1_Id = Unit.Id
outer apply 
(
		select 
			case 
				when subwork.Unit1_Id = 6 
					then isnull(avg(work_plan.AmountPlan1), 0) 
				else 
					isnull(sum(work_plan.AmountPlan1), 0) 
			end as AmountPlan1
		from EAR.dbo.RealizationDistanceSubWorkPlan work_plan
		inner join @ObjectRealizationDistance obj_dist_short on work_plan.ObjectRealizationDistance_Id = obj_dist_short.Id 
															 and work_plan.SubWorkType_Id = subwork.Id 
		where obj_dist_short.ObjectType_Id = objtype_subworktype.ObjectType_Id
) work_plan
outer apply 
(
		select 
			case 
				when subwork.Unit1_Id = 6 
					then isnull(avg(work_fact.AmountFact1), 0) 
				else 
					isnull(sum(work_fact.AmountFact1), 0) 
			end as AmountFact1
		from EAR.dbo.RealizationDistanceSubWorkPlan work_plan
		inner join @ObjectRealizationDistance obj_dist_short on work_plan.ObjectRealizationDistance_Id = obj_dist_short.Id 
															 and work_plan.SubWorkType_Id = subwork.Id 
		inner join EAR.dbo.RealizationDistanceSubWorkFact work_fact on work_plan.Id = work_fact.RealizationDistanceSubWorkPlan_Id
		where obj_dist_short.ObjectType_Id = objtype_subworktype.ObjectType_Id
) work_fact
outer apply 
(
		select 
			case 
				when subwork.Unit1_Id = 6 then 
					isnull(avg(by_day.Quantity), 0) 
				else 
					isnull(sum(by_day.Quantity), 0)
			end as Quantity
		from EAR.dbo.RealizationDistanceSubWorkPlan work_plan
		inner join @ObjectRealizationDistance obj_dist_short on work_plan.ObjectRealizationDistance_Id = obj_dist_short.Id 
															 and work_plan.SubWorkType_Id = subwork.Id 
		inner join EAR.dbo.ObjectPlanKMUbyDay by_day on work_plan.Id = by_day.RealizationDistanceSubWorkPlan_Id
		where obj_dist_short.ObjectType_Id = objtype_subworktype.ObjectType_Id
		and by_day.WorkDate between @date_from and @date_to
) by_day
outer apply 
(
	select 
		case 
			when work_plan.AmountPlan1 = 0 then 0
			else by_day.Quantity * (work_plan.AmountPlan1 - work_fact.AmountFact1) / work_plan.AmountPlan1 
		end as Quantity
) by_day_f

union all

select 
		N'План КМУ',
		null,
		isnull(sum(obj_plan.AmountPlan), 0.000) as PlanKMU,
		null,
		null,
		null,
		null,
		0,
		0
from
(
		select 
			obj_plan.AmountPlan
		from EAR.dbo.ObjectRealization obj_real
		inner join @ObjectRealizationDistance obj_dist on obj_real.Id = obj_dist.ObjectRealization_Id
		inner join EAR.dbo.ObjectInPlan obj_plan on obj_real.ObjectProposal_Id = obj_plan.ObjectProposal_Id
		where obj_dist.ObjectType_Id = @object_type_id
		and obj_plan.PlanVersion_Id in (select 
											max(sub.Id) 
										from EAR.dbo.PlanVersion sub 
										where year(sub.FinanceYear) = @finance_year
										and sub.State_EKey = 5)
		group by 
			obj_real.Id,
			obj_plan.AmountPlan
) obj_plan

order by [Index], FrontOrder;

