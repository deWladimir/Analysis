--declare @resultColumn nvarchar(max);
--
--if @reportYear > 2020
--begin
--	set @resultColumn = N'(select top 1 iif(o.ShortName is null, o.[Name], o.ShortName) from General.DIM.Organization o where o.EDRPOU = el.ElementName)';
--end
--
--else if @reportYear < = 2020
--begin
--	set @resultColumn = N'el.ElementName';
--end


if @procType is null
begin
	set @procType = (select STRING_AGG(t.ProcurementType_EKey, ',') from (select ProcurementType_EKey from Reports.dbo.TenderInfo where DataType_EKey = 4 group by ProcurementType_EKey)t );
end

declare @filterColumn nvarchar(100);

if @regionId is not null
begin
	set @filterColumn = concat(' = ', cast(@regionId as nvarchar(4)));
end

else if @regionId is null
begin
	set @filterColumn = N' is null';
end

declare @sqlCommand nvarchar(max) = N'
select top 7
			case when o.ShortName is not null and o.[Name] is not null then o.ShortName
				 when o.ShortName is null and o.[Name] is not null then o.[Name]
			  else el.ElementName end as ElementName
			,sum(q.ElementAmount) as ElementAmount
			,cast(sum(q.ElementAmountPercent) / (select count(*) from string_split(''' + @procType  + ''', '','')) as decimal(4, 1)) as ElementAmountPercent
	from
	(
	select cast(value as int) as procType
	from string_split(''' + @procType + ''', '','')
	) pr
	cross apply
	(
	select distinct ElementName
	from Reports.dbo.TenderInfo ti
	where ti.DataType_EKey = 4
	and ti.ProcurementType_EKey = pr.procType
	) el cross apply
	(
		select top 1
			ElementAmount,
			ElementAmountPercent
		from Reports.dbo.TenderInfo ti
		where Region_Id ' + @filterColumn +'
		and ReportYear = ' + cast(@reportYear as nvarchar(4)) + '
		and ti.ElementName = el.ElementName
		and ti.ProcurementType_EKey = pr.procType
		and ti.DataType_EKey = 4
		order by CalcDateTime desc
	) as q
	left join General.DIM.Organization o on o.EDRPOU = el.ElementName	
	where #filter_columns#
	#sort_columns#
	group by case when o.ShortName is not null and o.[Name] is not null then o.ShortName
				 when o.ShortName is null and o.[Name] is not null then o.[Name]
			  else el.ElementName end
	order by ElementAmountPercent desc
	'

exec sp_executesql @sqlCommand;