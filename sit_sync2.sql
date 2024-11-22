--Version 1.2 SIT (2 DATABASES NO STAGING) 
-- Author: Jack Villiyam

declare @jobName varchar(100) = 'DWH_AUX_SYNC_SIT',
		@jobId int,
		@jobStepId int,
		@row_count int
;

insert into T24_DWH_LANDING_SIT.AUX.sync_job_log (sync_job,start_date)
values (@jobName,GETDATE())
;

select @jobId = SCOPE_IDENTITY();

declare
	@sql nvarchar(max) = '', 
    @sql0 nvarchar(max) = '',
	@sqlDBs nvarchar(max) = '',
	@sqlColumnsList nvarchar(max) = '',
	@i int
;

declare
    @serverName varchar(max) = '[MSDWH-21\SIT].',
	@serverDBName varchar(200)= 'DWH_AUX_DATA_SIT',
	@dbName varchar(200) = 'T24_DWH_LANDING_SIT',
	@sch_name varchar(200),
	@sch_name_des varchar(200) = 'AUX',
	@t_name varchar(200)
;

insert into T24_DWH_LANDING_SIT.AUX.sync_job_step_log (sync_job_id, event_name) 
values (@jobId, 'Create ##AUXtempTables')
----;
select @jobStepId = SCOPE_IDENTITY();

if object_id('tempdb..##AUXtempTables') is not null drop table ##AUXtempTables;

SET @sqlDBs = '
    SELECT
        sch.name sch_name,
        t.name t_name,
        c.column_id,
        c.name c_name
    INTO ##AUXtempTables
    FROM [' + @serverDBName + '].sys.columns c
    JOIN [' + @serverDBName + '].sys.tables t ON c.object_id = t.object_id
    JOIN [' + @serverDBName + '].sys.schemas sch ON t.schema_id = sch.schema_id
    WHERE 1 = 1
    AND sch.name = ''ANACREDIT''
    AND t.name NOT IN (SELECT conf_value FROM config.config WHERE conf_key = ''NonSyncTables'')
    AND t.name IN (SELECT name FROM ' + @serverName + @serverDBName + '.sys.tables)
    ORDER BY sch_name, t_name, c.column_id;'
;

exec sp_executesql @sqlDBs;
SELECT * FROM ##AUXtempTables

select @row_count = @@ROWCOUNT;

update T24_DWH_LANDING_SIT.AUX.sync_job_step_log
set row_count = @row_count,
	end_date = getdate()
where id = @jobStepId
;


insert into T24_DWH_LANDING_SIT.AUX.sync_job_step_log (sync_job_id, event_name)
values (@jobId, 'Create ##AUXtempKeys')
;
select @jobStepId = SCOPE_IDENTITY();

if object_id('tempdb..##AUXtempKeys') is not null drop table ##AUXtempKeys;

select @sqlDBs = '
	select
		sch.name sch_name,
		t.name t_name,
		ic.key_ordinal,
		ci.name ic_name
	into ##AUXtempKeys
	from
		[' + @dbName + '].sys.objects o
		join [' + @dbName + '].sys.tables t on t.object_id = o.parent_object_id
		join [' + @dbName + '].sys.schemas sch on sch.schema_id = t.schema_id
		left join [' + @dbName + '].sys.key_constraints kc
			join [' + @dbName + '].sys.indexes i on i.object_id = kc.parent_object_id and i.index_id = kc.unique_index_id
			join [' + @dbName + '].sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id
			join [' + @dbName + '].sys.columns ci on ci.object_id = ic.object_id and ci.column_id = ic.column_id
			on kc.object_id = o.object_id
	where 1 = 1
	and sch.name = ''ANACREDIT''
	and o.type in (''PK'')
	and t.name not in (select conf_value from config.config where conf_key = ''NonSyncTables'')
	and t.name in (select name from ' + @serverName + @serverDBName + '.sys.tables)	
order by sch_name, t_name, key_ordinal
	;
'
;

exec sp_executesql @sqlDBs;
SELECT * FROM ##AUXtempKeys;

select @row_count = @@ROWCOUNT;

update T24_DWH_LANDING_SIT.AUX.sync_job_step_log
set row_count = @row_count,
	end_date = getdate()
where id = @jobStepId
;

DECLARE insert_staging_crsr CURSOR FOR
select distinct sch_name, t_name
from ##AUXtempTables
;
OPEN insert_staging_crsr;
FETCH NEXT FROM insert_staging_crsr INTO @sch_name, @t_name;
WHILE @@FETCH_STATUS = 0
BEGIN
	
	
	insert into T24_DWH_LANDING_SIT.AUX.sync_job_step_log (sync_job_id, event_name)
	values (@jobId, 'Processing table ' + @sch_name + '.' + @t_name)
	;

	select @jobStepId = SCOPE_IDENTITY();

	select @sql = '';
	select @sqlColumnsList = '';

	select @sqlColumnsList = @sqlColumnsList + '[' + replace(t.c_name, '.', '_') + ']'
	from ##AUXtempTables t where sch_name = @sch_name and t_name = @t_name
	order by t.column_id
	;

	select @sqlColumnsList = replace(@sqlColumnsList, '][', '],[');

	select @sql = @sql + 'truncate table [' + @dbName + '].[' + @sch_name_des + '].[' + @t_name + '];';

	select @sql = @sql + char(13) + 'insert into [' + @dbName + '].[' + @sch_name_des + '].[' + @t_name + '] ('
			+ @sqlColumnsList + ')
				select ' + @sqlColumnsList + '
				from ' + @serverName + @serverDBName +'.['
			+@sch_name + '].[' + @t_name + '];'
	;

	EXEC sp_executesql @stmt = @sql;

	select @sql0  = @sql0  + @sql  ;
		
	update T24_DWH_LANDING_SIT.AUX.sync_job_step_log
	set end_date = getdate()
	where id = @jobStepId
	;
	FETCH NEXT FROM insert_staging_crsr INTO @sch_name, @t_name;
END;
CLOSE insert_staging_crsr;
DEALLOCATE insert_staging_crsr;

UPDATE T24_DWH_LANDING_SIT.AUX.sync_job_log SET end_date = GETDATE() where id = @jobId;

SELECT @sql0;

SELECT * from sys.servers

SELECT * FROM sys.tables

--select * into T24_DWH_LANDING_SIT.AUX.FACS FROM DWH_AUX_DATA_SIT.ANACREDIT.FACS

--CREATE SCHEMA ANACREDIT

--select * from sys.schemas

--select * from T24_DWH_LANDING_SIT.AUX.ACCS

--DELETE FROM T24_DWH_LANDING_SIT.AUX.ACCS