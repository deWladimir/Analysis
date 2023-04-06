select 
	extract(month from date_modified) as month,
	extract(year from date_modified) as year,
	count(id) as qnt
from public.list_tenders 
where extract(month from date_modified) not in (1, 2)
and extract(year from date_modified) between 2020 and 2023
group by 
	extract(month from date_modified),
	extract(year from date_modified);