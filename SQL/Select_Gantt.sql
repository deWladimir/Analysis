drop table if exists filtered_tasks;
create temp table filtered_tasks
(
	task_id int,
	is_kid int,
	stage_id int
);

insert into filtered_tasks
(
	task_id,
	is_kid,
	stage_id
)
select 
	sub.task_id,
	
	case 
		when sub.qnt = 1 
			and original_task.executor_user_id = @current_user_id
			and (original_task.task_state_id in(select unnest(concat('{', @task_states_arr, '}')::int[]))
				 or @task_states_arr is null)
			and (original_task.task_type_id = @IsMainTask
			     or @IsMainTask is null)
			and 
				case 
					when original_task.fact_start_date is null
					then original_task.plan_start_date
					
					else original_task.fact_start_date 
				end <= @datetime_to
			and
				case 
					when original_task.fact_start_date is null
					then public.get_workday_date(original_task.plan_start_date,
												 coalesce(original_task.perform_duration / 8, 0),
												 coalesce(original_task.perform_duration % 8, 0)
												)
					
					else 
						case 
							when original_task.fact_end_date is null
							then now()::timestamp without time zone
							else original_task.fact_end_date 
						end
				end >= @datetime_from
		then 0
	end as is_kid,
	
	original_task.stage_id
from
(
	select 
		kids.task_id,
		count(kids.task_id) as qnt
	from public.tasks filter_tasks
	inner join public.fn_select_all_children_tasks(filter_tasks.id) kids(task_id)
			   on filter_tasks.executor_user_id = @current_user_id
			   and (filter_tasks.task_state_id in(select unnest(concat('{', @task_states_arr, '}')::int[]))
			   		or @task_states_arr is null)
			   and (filter_tasks.task_type_id = @IsMainTask
			   		or @IsMainTask is null)
			   and case 
					when filter_tasks.fact_start_date is null
						then filter_tasks.plan_start_date
					else filter_tasks.fact_start_date
				   end <= @datetime_to
			   and case
					when filter_tasks.fact_start_date is null
						then public.get_workday_date(filter_tasks.plan_start_date,
													  coalesce(filter_tasks.perform_duration / 8, 0),
													  coalesce(filter_tasks.perform_duration % 8, 0)
													)
					else 
						case
						when filter_tasks.fact_end_date is null 
							then now()::timestamp without time zone
						else filter_tasks.fact_end_date
						end 
					end >= @datetime_from
	group by kids.task_id
	) sub
inner join public.tasks original_task on sub.task_id = original_task.id
where original_task.task_type_id = @IsMainTask
or @IsMainTask is null;
---------------------------------------------------------------------------------------------------------------------------------------

/*with recursive objects_tree 
as
(
	select 
		id,
		parent_object_id,
		short_name as name
		--id as main_parent_id
	from public.objects
	where parent_object_id is null
	
	union
	
	select 
		kidos.id,
		kidos.parent_object_id,
		kidos.short_name
		--tree.main_parent_id
	from public.objects kidos
	inner join objects_tree tree on kidos.parent_object_id = tree.id	
)
*/

with temp_objects
as
(
	select 
 		object_id
 	from public.stages 
 	where stages.id in(select stage_id from filtered_tasks group by stage_id) 
	and (stage_code_id in (select unnest(concat('{', @stage_codes_str, '}')::int[]))
 		 or @stage_codes_str is null)
	group by object_id
)

select 
	'_' || objects.id as id,
	case 
		when objects.parent_object_id in(select object_id from temp_objects)
		then '_' || objects.parent_object_id 
		when @object_id is not null 
			 and objects.parent_object_id not in(select object_id from temp_objects)
			 and objects.id <> @object_id then '_' || @object_id
		else null
	end as parent_id,
 	objects.name,
 	null as project_role_name,
 	null as executor,
 	null::int as state_id,
 	null as state_style,
 	null as plan_start_date,
 	null as plan_end_date,
 	null as fact_start_date,
 	null as fact_end_date,
 	null as dependency,
 	null as stage_id,
 	null as is_fact_end_date_null,
 	null as task_type_id,
 	null as plan_duration_sec,
 	0 as sort_index
from public.objects
where (objects.id in(select o_h.id from public.fn_get_object_children_h(@object_id::int) o_h(id))
 	   or @object_id::int is null)
and objects.id in(select object_id from temp_objects)

union 

select 
	'#' || stages.id as id,
	'_' || stages.object_id as parent_id,
	stage_codes.code as name,
	null as project_role_name,
	null as executor,
	null::int as state_id,
	null as state_style,
	null as plan_start_date,
	null as plan_end_date,
	null as fact_start_date,
	null as fact_end_date,
	null as dependency,
	null as stage_id,
	null as is_fact_end_date_null,
	null as task_type_id,
	null as plan_duration_sec,
	1 as sort_index
from public.stages
left join dim.stage_codes on stages.stage_code_id = stage_codes.id
where (stages.object_id in(select o_h.id from public.fn_get_object_children_h(@object_id::int) o_h(id))
	  or @object_id::int is null)
and stages.id in(select stage_id from filtered_tasks group by stage_id)
and (stages.stage_code_id in(select unnest(concat('{', @stage_codes_str, '}')::int[]))
	 or @stage_codes_str is null)

union 

select 
	tasks.id::text,
	
	case 
		when tasks.parent_task_id is null or filters.is_kid = 0 
			then '#' || tasks.stage_id
		else tasks.parent_task_id::text
	end 		as parent_id,
	
	tasks.theme as name,
	
	project_roles.name as project_role_name,
	
	users.user_info::json ->> 'lastname' || ' ' ||
    left(users.user_info::json ->> 'firstname', 1) || '. ' ||
	case 
	  when users.user_info::json ->> 'patronymic' is not null
	  	then left(users.user_info::json ->> 'patronymic', 1) || '.'
	  else ''
	end			 as executor,
		   
	task_states.id::int as state_id,
	task_states.style::text as state_style,
	
	tasks.plan_start_date::text,
	
	public.get_workday_date(tasks.plan_start_date,
						   	coalesce(tasks.plan_duration / 8, 0),
						    coalesce(tasks.plan_duration % 8, 0))::text as plan_end_date,
	tasks.fact_start_date::text,
	
	case 
		when tasks.fact_end_date is null then (now()::timestamp without time zone)::text
		else tasks.fact_end_date::text
	end as fact_end_date,
	
	task_to_task.ids as dependency,
	
	tasks.stage_id::text,
	
	case 
		when tasks.fact_end_date is null then '1'
		else '0'
	end as is_fact_end_date_null,
	
	tasks.task_type_id::text,
	
	(tasks.plan_duration * 3600)::text as plan_duration_sec, 
	2 as sort_index
from public.tasks 
left join dim.project_roles on tasks.project_role_id = project_roles.id
left join dim.task_states on tasks.task_state_id = task_states.id
left join public.stages on tasks.stage_id = stages.id
left join public.get_all_users() users on tasks.executor_user_id = users.user_id
left join lateral
(
	select 
		-- string_agg(t_to_t.id::text, ',') as ids
		concat('[',string_agg(concat('{"id":"', t_to_t.id::text,'","previous_task_id":"', t_to_t.previous_task_id::text, '","next_task_id":"', t_to_t.next_task_id::text, '"}'), ','), ']') as ids
	from
	(
		select
			next_task_id,
			previous_task_id,
			id
		from link.task_to_task
		where previous_task_id = tasks.id

		-- union 

		-- select 
		-- 	previous_task_id as id
		-- from link.task_to_task
		-- where next_task_id = tasks.id
	) t_to_t
) task_to_task on true
inner join filtered_tasks filters on tasks.id = filters.task_id
where (stages.object_id in(select o_h.id from public.fn_get_object_children_h(@object_id::int) o_h(id))
       or @object_id::int is null)
and (stages.stage_code_id in(select unnest(concat('{', @stage_codes_str, '}')::int[]))
	 or @stage_codes_str is null)

order by plan_start_date, sort_index, parent_id asc;

