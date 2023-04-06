--  declare 
--  	@Year int = 2023,
--  	@MonthIds nvarchar(100);
--------------------------------------------------Month Logic---------------------------------------------------------------
declare @Months table 
(
	Id int, 
	[Name] nvarchar(20)
);

insert into @Months
(
	Id,
	[Name]
)
select 
	MonthsAndMonths.Id, 
	MonthsAndMonths.[Name]
from 
(
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

) MonthsAndMonths (Id, [Name])
where MonthsAndMonths.Id in (select [value] from string_split(@MonthIds, N','))
or @MonthIds is null;

--------------------------------------------------Start of Main Select--------------------------------------------------------------
select 
	emps.Id, 
	emps.PIB,
	months.months_data
from CRM_Jurist_Analitics.dbo.Employees emps 
outer apply 
(
	select JSON_QUERY((
		select
			months.[Name],
			isnull(abs_data.DayOff, 0) as N'В',
			ISNULL(abs_data.SickDay, 0) as N'Л',
            isnull(abs_data.DayOff, 0) + ISNULL(abs_data.SickDay, 0) as SumAbsentDays,
			days_data.all_days
		from @Months months 
		outer apply 
		(
			select 
				sum(iif(emp_abs.absent_type_id = 1, 1, 0)) as DayOff,
				sum(IIF(emp_abs.absent_type_id = 2, 1, 0)) as SickDay
			from 
			(
				select 
					max(emp_abs.Id) as employee_absent_id
				from CRM_Jurist_Analitics.dbo.Employee_Absent emp_abs
				inner join CRM_1551_Analitics.dbo.WorkDaysCalendar work_days on cast(emp_abs.start_absent_date as date) <= work_days.[date]
																			 and cast(emp_abs.end_absent_date as date) >= work_days.[date]
																			 and work_days.[Month] = months.Id
																			 and work_days.[Year] = @Year

				where emp_abs.employee_id = emps.Id
				group by 
					work_days.[date]
			) distinct_employee_absent
			inner join CRM_Jurist_Analitics.dbo.Employee_Absent emp_abs on distinct_employee_absent.employee_absent_id = emp_abs.Id
		) abs_data
		outer apply 
		(
			select json_query((
				select 
					convert(nvarchar(10), all_days.[date], 104) as [Date],
					right(concat(N'0', DAY(all_days.[date])), 2) as [Day],
					emp_abs.absent_type_id,
					case 
						when emp_abs.absent_type_id = 1 then N'В'
						when emp_abs.absent_type_id = 2 then N'Л'
						else N''
					end as absent_type_letter,
					all_days.is_work,
					emp_abs.id as employee_absent_id,
					emp_abs.employee_id
				from
				(
					select 
						all_days.[date],
						max(emp_abs.Id) as employee_absent_id
					from CRM_1551_Analitics.dbo.WorkDaysCalendar all_days 
					left join CRM_Jurist_Analitics.dbo.Employee_Absent emp_abs on all_days.[date] >= cast(emp_abs.start_absent_date as date)
																			   and all_days.[date] <= cast(emp_abs.end_absent_date as date)
																			   and emp_abs.employee_id = emps.Id
					where all_days.[Month] = months.Id
					and all_days.[Year] = @Year
					group by 
						all_days.[date]
				) sub 
				inner join CRM_1551_Analitics.dbo.WorkDaysCalendar all_days on sub.[date] = all_days.[date]
				left join CRM_Jurist_Analitics.dbo.Employee_Absent emp_abs on sub.employee_absent_id = emp_abs.Id 
				order by all_days.[date]
				for json path, 
				include_null_values
			)) as all_days
		) days_data
		for json path, 
		include_null_values
	)) as months_data
) months
order by emps.PIB;
