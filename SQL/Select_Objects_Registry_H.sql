drop table if exists filtered_objects;
create temp table filtered_objects
(
	id int,
	is_kid smallint
);

with recursive tree_objects 
as
(
	select 
		objects.id,
		objects.parent_object_id,
		0 as main
	from public.objects
	left join dev_general.organization cust on objects.customer_id = cust.id
	left join dev_general.organization contr on objects.contractor_id = contr.id
	left join lateral 
	(
		select 
			obj.id
		from public.fn_get_object_children_h(objects.id) obj(id)
		inner join public.stages on obj.id = stages.object_id
		inner join link.stage_project_role proj_roles on proj_roles.stage_id = stages.id 
														and proj_roles.project_role_id = 2
														and proj_roles.user_id = @current_user_id
															
		order by obj.id asc
		limit 1
	) checkObj on true
	where (objects.object_state_id in(select unnest(concat('{', @object_states_str, '}')::int[]))
		   or @object_states_str is null)
	and (objects.object_type_id in(select unnest(concat('{', @object_types_str, '}')::int[]))
	 	 or @object_types_str is null)
	and (cust.id in(select unnest(concat('{', @customers_str, '}')::int[]))
	 	 or @customers_str is null)
	and (contr.id in(select unnest(concat('{', @contractors_str, '}')::int[]))
		 or @contractors_str is null)
	and 
		(
			checkObj.id is not null
			or not exists (
							select 
								stages.id
							from public.stages 
							where stages.object_id = objects.id
						  )
		)
	
	union
	
	select 
		kids.id,
		kids.parent_object_id,
		1 
	from public.objects kids
	inner join tree_objects parent on kids.parent_object_id = parent.id
	left join lateral 
	(
		select 
			obj.id
		from public.fn_get_object_children_h(kids.id) obj(id)
		inner join public.stages on obj.id = stages.object_id
		inner join link.stage_project_role proj_roles on proj_roles.stage_id = stages.id 
														and proj_roles.project_role_id = 2
														and proj_roles.user_id = @current_user_id
		order by obj.id asc
		limit 1
	) checkObj on true
	where checkObj.id is not null
	or not exists (
					select 
						stages.id
					from public.stages 
					where stages.object_id = kids.id
				  )
)

insert into filtered_objects
(
	id,
	is_kid
)
select
	sub.id,
	case 
		when isnot_main > 0 then 1
		when isnot_main = 0 then 0
	end as is_kid
from
(
	select 
		tree.id,
		sum(tree.main) as isnot_main
	from tree_objects tree
	group by
		tree.id
) sub;




select 
	objects.id,
	
	case 
		when filtered_objects.is_kid = 1 then objects.parent_object_id
		when filtered_objects.is_kid = 0 then null
	end as parent_id,
	
	objects.code,
	
	objects.short_name,
	
	object_types.name as object_type_name,
	
	object_states.name as object_state_name,
	
	object_states.style as object_state_style,
	
	objects.building_address,
	
	case 
		when customer.shortname is not null then customer.shortname
		else customer.name
	end as customer,
	
	case 
		when contractor.shortname is not null then contractor.shortname
		else contractor.name
	end as contractor,
	
case 
		when objects.object_state_id in(1, 2)
		then 
			json_build_object('date', plan_dates.plan_start_date,
							  'color', case 
							  			 when plan_dates.plan_start_date is null 
										 then null

							 			 when now()::timestamp without time zone > plan_dates.plan_start_date
							  				  and plan_dates.start_state_id = 1
							 		     then '#CB2121'

							  			 when split_part(diff_workday.plan_st_date, ' ', 1)::int = 0
							 				 and split_part(diff_workday.plan_st_date, ' ', 2)::int < 8 
							 				 and split_part(diff_workday.plan_st_date, ' ', 2)::int >= 0
							  				 and plan_dates.start_state_id = 1
							 			 then '#F97316'
										 
							 			 else 
							 				null
							 			end)
	   else 
	  		json_build_object('date', plan_dates.plan_start_date,
							  'color', null)
	end as plan_start_date, 
	  
	case 
		when objects.object_state_id in(1, 2)
		then 
			json_build_object('date', plan_dates.plan_perform_date,
							  'color', case 
							  			 when plan_dates.plan_start_date is null 
										 then null

							 			 when now()::timestamp without time zone > plan_dates.plan_perform_date
							  				  and plan_dates.end_state_id = 2
							 		     then '#CB2121'

							  			 when split_part(diff_workday.plan_perf_date, ' ', 1)::int = 0
							 				 and split_part(diff_workday.plan_perf_date, ' ', 2)::int < 8 
							 				 and split_part(diff_workday.plan_perf_date, ' ', 2)::int >= 0
							  				 and plan_dates.end_state_id = 2
							 			 then '#F97316'

							 			 else 
							 				null
							 			end)
		else 
	  		json_build_object('date', plan_dates.plan_perform_date,
							  'color', null)
	end as plan_perform_date, 
	
	fin_fact.money_value,
	current_tasks.current_tasks,
	
	json_build_object('qnt', current_tasks_stats.qnt,
					  'good_qnt', current_tasks_stats.qnt - current_tasks_stats.red_flags,
					  'color_index', case 
					  					when current_tasks_stats.max_index = 0 and current_tasks_stats.min_index = 0
					  					then 'green'
					  					when current_tasks_stats.max_index = 1 and current_tasks_stats.min_index = 1
					  					then 'red'
					  					else 'orange'
					  				 end
					 ) as current_tasks_stats,
	
	coalesce(realization.percentage, 0) as percentage,

	objects.name as object_name
	
from public.objects
inner join filtered_objects on objects.id = filtered_objects.id
left join dim.object_types on objects.object_type_id = object_types.id
left join dim.object_states on objects.object_state_id = object_states.id
left join dev_general.organization customer on objects.customer_id = customer.id
left join dev_general.organization contractor on objects.contractor_id = contractor.id
left join lateral
(
	select
		start_d.plan_start_date,
		start_d.task_state_id as start_state_id,
		end_d.plan_perform_date,
		end_d.task_state_id as end_state_id
	from
	(
		select 
			tasks.plan_start_date,
			tasks.task_state_id
		from public.stages 
		inner join public.tasks on stages.main_task_id = tasks.id
		where stages.object_id = objects.id
		order by tasks.plan_start_date asc
		limit 1
	) start_d
	left join
	(
		select 
			public.get_workday_date(tasks.plan_start_date,
								   	coalesce(tasks.perform_duration / 8, 0),
								    coalesce(tasks.perform_duration % 8, 0)) as plan_perform_date,
			tasks.task_state_id
		from public.stages 
		inner join public.tasks on stages.main_task_id = tasks.id
		where stages.object_id = objects.id
		order by 1 desc
		limit 1
	) end_d on 1 = 1
) plan_dates on true
left join lateral
(
	select
		sum(money_value) as money_value
	from public.stages 
	inner join public.fn_select_all_children_tasks(stages.main_task_id) tasks(id) on 1 = 1
	inner join public.finance_fact on tasks.id = finance_fact.task_id
	where stages.object_id = objects.id
) fin_fact on true
left join lateral
(
	select 
		sum(fin_fact.money_value) / 1.0 / sum(tasks.comercial_price) * 100 as percentage,
		array_agg(tasks.id) as task_ids
	from public.stages
	inner join public.tasks on stages.id = tasks.stage_id
							and tasks.task_type_id = 1
	inner join 
	(
		select 
			task_id
		from public.document
		group by 
			task_id
	) docs on tasks.id = docs.task_id
	left join lateral
	(
		select 
			sum(money_value) as money_value
		from public.finance_fact fin_fact
		where fin_fact.task_id = tasks.id
	) fin_fact on true
	where stages.object_id = objects.id
) realization on true
left join lateral
(
	select 
		json_agg(json_build_object('id', tasks.id,
								   'doc_name', doc_types.name,
								   'color_index', case 
													when tasks.plan_start_date < tasks.fact_start_date 
														  or tasks.fact_start_date is null
													then 'red'
													else 'green'
												  end) 
				 ) 	as current_tasks,
		array_agg(tasks.id) as tasks_ids
	from public.stages
	inner join public.tasks on tasks.stage_id = stages.id
							and tasks.task_type_id = 1
							and tasks.task_state_id in(1, 2)
	inner join 
	(
		select
			document_type_id,
			task_id
		from public.document
		group by 
			document_type_id,
			task_id
	) docs on tasks.id = docs.task_id
	left join dim.document_types doc_types on docs.document_type_id = doc_types.id
	where stages.object_id = objects.id
) current_tasks on true
left join lateral
(
	select 
		count(stats.id) as qnt,
		coalesce(sum(stats._index), 0) as red_flags,
		max(stats._index) as max_index,
		min(stats._index) as min_index
	from
	(
		select 
			tasks.id,
			case 
				when tasks.plan_start_date < tasks.fact_start_date
					 or tasks.fact_start_date is null 
				then 1
				else 0
			end		as _index
		from public.tasks
		where tasks.id in(select unnest(current_tasks.tasks_ids))
	) stats
) current_tasks_stats on true
left join lateral
(
	select 
		public.fn_difference_workday(now()::timestamp without time zone, 
									 plan_dates.plan_start_date) as plan_st_date,
		public.fn_difference_workday(now()::timestamp without time zone, 
									 plan_dates.plan_perform_date) as plan_perf_date
) diff_workday on true
where 
(
	(
		objects.Id = @Id 
		and @new_project = 'new_project'
	)
	or @new_project is null
)
order by id asc, 
		 parent_id asc nulls first, 
		 short_name asc;