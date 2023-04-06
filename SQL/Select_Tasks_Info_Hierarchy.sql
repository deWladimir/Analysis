with recursive task_tree 
as 
(
	select 
		tasks.id,
		tasks.parent_task_id,
		0 as layer
	from public.tasks 
	inner join public.stages on stages.main_task_id = tasks.id
						and stages.id = @stage_id
						and tasks.stage_id = @stage_id
	
	union all 
	
	select 
		tasks.id,
		tasks.parent_task_id,
		parent.layer + 1
	from public.tasks 
	inner join task_tree parent on tasks.parent_task_id = parent.id
                                --and tasks.stage_id = @stage_id
)

select 
	tasks.id,
	tasks.parent_task_id,
	tasks.theme as task_theme,
	work_rel.name as work_relation_name,
	coalesce(tasks.comercial_price, 0)::decimal(19, 2) as comercial_price,
	case 
		when parent.comercial_price <> 0 then 
			coalesce(tasks.comercial_price / 1.0 / parent.comercial_price, 0) * 100::decimal(19, 2) 
		else 
			null
	end as parent_percent_comercial_price,
	case 
		when tasks.comercial_price <> 0 then
			case 
				when kids.comercial_price > tasks.comercial_price then 
					(100 - coalesce(kids.comercial_price / 1.0 / tasks.comercial_price, 0) * 100)::decimal(19, 2) 
				else 
					(coalesce(kids.comercial_price / 1.0 / tasks.comercial_price, 0) * 100)::decimal(19, 2) 
			end 
		else 
			0
	end as segregated_percent,
	case 
		when coalesce(tasks.budget, 0) = 0 then 1 
		when coalesce(tasks.budget, 0) <> 0 then 0
	end as is_updatable,
	main_tree.layer,
	tasks.work_relation_type_id,
	concat(task_codes.code, ' ', tasks.further_code) as task_code
from task_tree main_tree
inner join public.tasks on main_tree.id = tasks.id 
left join lateral
(
	select 
		sum(coalesce(kids.comercial_price, 0)) as comercial_price
	from public.tasks kids
	where kids.parent_task_id = tasks.id
) kids on true
left join public.tasks parent on tasks.parent_task_id = parent.id
left join dim.work_relation_type work_rel on tasks.work_relation_type_id = work_rel.id
left join dim.task_codes on tasks.task_code_id = task_codes.id
order by tasks.parent_task_id asc nulls first;