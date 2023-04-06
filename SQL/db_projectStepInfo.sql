/*
declare @from datetime = N'2022-10-01';
declare @to datetime = N'2022-10-31';

declare @ContractNumber nvarchar(max)
declare @ContractResult nvarchar(max)
declare @StepNumber nvarchar(max)
declare @Organization nvarchar(max)
declare @ProjectState nvarchar(max)
*/
--AVZCRM-309
------------------------------------------------------------------------------------------------------------------------
declare @step_proj_state table 
(
    Id int
);

insert into @step_proj_state
(
    Id 
)
select 
    ps.Id
from CRM_UAD.dbo.ProjectStepInfo ps 
where 
    case 
	  	when isnull(ps.Is_Ready, 0) = 0 and ps.Is_Confirm is null and ps.StartDate <= GETDATE() then 2
		when ps.Is_Ready = 1 and isnull(ps.Is_Confirm, 0) = 0 then 3
		when ps.Is_Confirm = 1 then 4
		when ps.Is_Confirm = 0 then 5
		when ps.StartDate > GETDATE() then 1
	end in (select [value] from string_split(@ProjectState, N','))
or @ProjectState is null;
------------------------------------------------------------------------------------------------------------------------
select 
	stepInfo.Id,
	contr.ContractNumber,--номер договору
	title, --назва договору
	contr.GeneralContrctSum,--вартість
	case 
	  	when isnull(stepInfo.Is_Ready, 0) = 0 and stepInfo.Is_Confirm is null and stepInfo.StartDate <= GETDATE() then N'В роботі'
		when stepInfo.Is_Ready = 1 and isnull(stepInfo.Is_Confirm, 0) = 0 then N'На перевірці'
		when stepInfo.Is_Confirm = 1 then N'Підтверджено'
		when stepInfo.Is_Confirm = 0 then N'Повернуто на доопрацювання'
		when stepInfo.StartDate > GETDATE() then N'Плановий'
	end as ProjectState_Name,
	contr_res.ContractResult_Name,
	iif(contractor.ShortName is not null, contractor.ShortName, contractor.[Name]) as Contractor,--виконавець
	concat(stepInfo.StepNumber, N'/', (select count(1) n from CRM_UAD.dbo.ProjectStepInfo where Project_Id=project.Id)) StepNumber, --Етап (всього/поточний)
	isnull(stepInfo.[Value], 0) as stepValue, --вартість етапу
	concat(isnull(paidSum.PaidSum, 0), N'/', isnull(paidSumAll.PaidSum, 0)) PaidSum, --Оплачено (всього/за етап)
	--isnull(stepInfo.[Value], 0) - isnull(paidSum.PaidSum, 0) as Obligation,--Борг
	filesQnt.Qnt as FileQuantity,-- К-сть доданих файлів поточного етапу - замість Кількість доданих файлів
	project.Id as IdForProjectCard,
	pemp.ResponsibleEmps
from CRM_UAD.dbo.ProjectStepInfo stepInfo
inner join CRM_UAD.dbo.Project project on stepInfo.Project_Id = project.Id and project.ProjectType_Id = 4
inner join EAR.dbo.[Contract] contr on project.Contract_Id = contr.Id 
left join CRM_UAD_Prozorro.dbo.TenderLots lots on contr.Lot_Id = lots.id
left join General.DIM.Organization contractor on contr.Contractor_Id = contractor.Id
left join General.DIM.Organization customer on contr.Customer_Id = customer.Id
left join [CRM_UAD].[DIM].[ProjectState] ps on project.ProjectState_Id=ps.Id
outer apply 
(
	select count(*) as Qnt
	from CRM_UAD.dbo.ProjectFile projFile
	where projFile.ProjectStepInfo_Id = stepInfo.Id
) filesQnt
outer apply 
(
	select 
		sum(finFact.PaidSum) as PaidSum
	from EAR.dbo.ContractFinancesFact finFact
	where finFact.Contract_Id = contr.Id 
	and finFact.PaimentDetails like concat(N'%', N'етап ', stepInfo.StepNumber, N';%')
	and finfact.PaimentDetails like('%'+replace(contr.ContractNumber,'/','-')+'%')
) paidSum
outer apply 
(
	select 
		sum(cff.PaidSum) as PaidSum
	from [EAR].[dbo].[ContractFinancesFact] cff
	where cff.Contract_Id = contr.Id
	and cff.PaimentDetails like('%'+replace(contr.ContractNumber,'/','-')+'%')
) paidSumAll
outer apply 
(
	select 
		string_agg(trim(emp.[Name]), N', ') as ResponsibleEmps
	from CRM_UAD.LINK.ProjectEmployee pemp 
	inner join General.DIM.Employee emp on pemp.Project_Employee_Id = emp.Id
	where pemp.Project_Id = stepInfo.Project_Id
	and pemp.IsMain = 0
) pemp
outer apply 
(
	select 
		string_agg(contr_res.[Name], N', ') as ContractResult_Name
	from CRM_UAD.dbo.ProjectResultInfo proj_res
	inner join CRM_UAD.DIM.ContractResult contr_res on proj_res.ContractResult_Id = contr_res.Id 
	where proj_res.Project_Id = project.Id
	and 
	(
		contr_res.Id in (select value from string_split(@ContractResult, N',')) 
		or @ContractResult is null
	)
) contr_res
where (cast(stepInfo.StartDate as date) between @from and @to
or (cast(stepInfo.StartDate as date) < @from and (stepInfo.EndDate is null
									or cast(stepInfo.EndDate as date) between @from and @to
									or cast(stepInfo.EndDate as date) > @to))
)
and (project.ContractNumber in  (select value from string_split(@ContractNumber, N',')) or @ContractNumber is null)
and ((contr_res.ContractResult_Name is not null and @ContractResult is not null) or @ContractResult is null)
and (stepInfo.StepNumber in  (select value from string_split(@StepNumber, N',')) or @StepNumber is null)
and (contractor.Id in  (select value from string_split(@Organization, N',')) or @Organization is null)
and stepInfo.Id in (select Id from @step_proj_state)


