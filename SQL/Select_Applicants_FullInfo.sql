--  declare 
--  	@date_from date,
--  	@date_to date,
--  	@OVA_Licensers nvarchar(10) = N'OVA',
--  	@Top5_All nvarchar(4) = N'Top5';
----------------------------------------------------------------------------------------------------------------------------------------
if @date_from is null
begin
	set @date_from = DATEFROMPARTS(year(getdate()), month(getdate()), 1);
end;

if @date_to is null 
begin
	set @date_to = getdate();
end;

select 
	@date_from = cast(@date_from as date),
	@date_to = cast(@date_to as date);
----------------------------------------------------------------------------------------------------------------------------------------
declare @SQL nvarchar(max);
select 
	@SQL = concat 
	(N'
		select ', iif(@Top5_All = N'Top5', N' top 5 ', N''),
		  ' names.Name1 as [Name], 
			general.qnt as General,
			departures.qnt as Departures,
			entrances.qnt as Entrances,
			notReturned.qnt as NotReturned, ',
			iif(@OVA_Licensers = N'OVA', N' null ', N' closed.qnt '), ' as Closed, ',
			iif(@OVA_Licensers = N'OVA', N' null ', N' overdued.qnt '), ' as Overdued', ',
			names.[Name] as NameCode
		from 
		(
			select 
				', iif(@OVA_Licensers = N'OVA', N' [Name] as Name1, Id  ', N' [Name] as [Name1],  Id '), ' as [Name]
			from ', iif(@OVA_Licensers = N'OVA', N' DSBT_Permits.DIM.OVAOrganization ', N' DSBT_Permits.DIM.Carrier '),'
			/*where cast(', iif(@OVA_Licensers = N'OVA', N' CreatedOn ', N' CreateDate '),' as date) between @date_from and @date_to OLD*/
			/*group by ',iif(@OVA_Licensers = N'OVA', N' MCAName ', N' carrierCode '),'*/
		) names
		outer apply
		(
			select 
				count(Id) as qnt
			from ', iif(@OVA_Licensers = N'OVA', N' DSBT_Permits.dbo.RequestBorderEx ', N' DSBT_Permits.dbo.RequestBorder '),'
			where cast(', iif(@OVA_Licensers = N'OVA', N' CreatedOn ', N' CreateDate '),' as date) between @date_from and @date_to
			and ', iif(@OVA_Licensers = N'OVA', N' OVAOrganization_Id ', N' Carrier_Id '), ' = names.[Name] ',
			iif(@OVA_Licensers = N'Licensers', ' and year(crossDate) <> 9999 ', ''), '
		) general
		outer apply 
		(
			select 
				count(logs.Id) as qnt
			from DSBT_Permits.dbo.RequestBorderLogs logs 
			inner join ', iif(@OVA_Licensers = N'OVA', N' DSBT_Permits.dbo.RequestBorderEx ', N' DSBT_Permits.dbo.RequestBorder '), ' t 
										  on ', iif(@OVA_Licensers = N'OVA', N' logs.RequestBorderEx_Id ', N' logs.RequestBorder_Id '),' = t.Id
			where ', iif(@OVA_Licensers = N'OVA', N' OVAOrganization_Id ', N' Carrier_Id '), ' = names.[Name]
			and cast(logs.DepartureDate as date) between @date_from and @date_to 
		) departures
		outer apply 
		(
			select 
				count(logs.Id) as qnt
			from DSBT_Permits.dbo.RequestBorderLogs logs 
			inner join ' ,iif(@OVA_Licensers = N'OVA', N' DSBT_Permits.dbo.RequestBorderEx ', N' DSBT_Permits.dbo.RequestBorder '), ' t 
										  on ', iif(@OVA_Licensers = N'OVA', N' logs.RequestBorderEx_Id ', N' logs.RequestBorder_Id '),' = t.Id
			where ', iif(@OVA_Licensers = N'OVA', N' OVAOrganization_Id ', N' Carrier_Id '), ' = names.[Name]
			and cast(logs.EntranceDate as date) between @date_from and @date_to 
		) entrances
		outer apply 
		(
			select 
				count(logs.Id) as qnt
			from DSBT_Permits.dbo.RequestBorderLogs logs 
			inner join ',iif(@OVA_Licensers = N'OVA', N' DSBT_Permits.dbo.RequestBorderEx ', N' DSBT_Permits.dbo.RequestBorder '),' t 
										  on ', iif(@OVA_Licensers = N'OVA', N' logs.RequestBorderEx_Id ', N' logs.RequestBorder_Id '),' = t.Id
			outer apply 
			(
				select top 1
					sub.Id
				from DSBT_Permits.dbo.RequestBorderLogs sub 
				where ', iif(@OVA_Licensers = N'OVA', N' sub.RequestBorderEx_Id ', N' sub.RequestBorder_Id '), ' = ', iif(@OVA_Licensers = N'OVA', N' logs.RequestBorderEx_Id ', N' logs.RequestBorder_Id '),'
				and sub.EntranceDate is not null
			) sub
			where ', iif(@OVA_Licensers = N'OVA', N' OVAOrganization_Id ', N' Carrier_Id '),' = names.[Name]
			and cast(logs.DepartureDate as date) < cast(dateadd(dd, -30, getdate()) as date)
			and sub.Id is null
		) notReturned',
		iif(@OVA_Licensers = N'OVA', N'', N'
			outer apply 
			(
				select 
					count(reqBorder.Id) as qnt
				from DSBT_Permits.dbo.RequestBorder reqBorder
				where reqBorder.Carrier_Id = names.[Name]
				and cast(reqBorder.crossDate as date) = cast(dateadd(dd, -10, getdate()) as date)
			) overdued 
		'),
		iif(@OVA_Licensers = N'OVA', N'', N'
			outer apply 
			(
				select 
					count(distinct reqBorder.Id) as qnt
				from DSBT_Permits.dbo.RequestBorder reqBorder
				inner join DSBT_Permits.dbo.RequestBorderLogs logs on logs.RequestBorder_Id = reqBorder.Id 
				where reqBorder.Carrier_Id = names.[Name]
				and cast(logs.DepartureDate as date) between @date_from and @date_to
			) closed
		'), '
		where general.qnt > 0
		or departures.qnt > 0
		or notReturned.qnt > 0
		or entrances.qnt > 0 ',
		iif(@OVA_Licensers = N'OVA', N'', N' or overdued.qnt > 0'), 
		iif(@OVA_Licensers = N'OVA', N'', N' or closed.qnt > 0'), '
		order by 2 desc
		'
	);

	--print @SQL;

exec sp_executesql @SQL, N'@date_from date, @date_to date', @date_from = @date_from, @date_to = @date_to;

