/*
**Version 1.0 PROD
*Author:
*VERSION HISTORY
*/


/*
** NOTES
*--CREATE TABLE config.unify --init insert records not to be unified
*--CREATE SCHEMA UNIFY
*/
--SELECT TOP 10 [FIRST_NAME],* FROM V_FBNK_CUSTOMER_TEST -- SELECT * FROM ##UNIFYTEMPTABLET24

declare @jobName varchar(100) = 'DWH_UNIFICATION_SIT',
		@jobId int,
		@jobStepId int,
		@row_count int
;

insert into T24_DWH_LANDING_SIT.UNIFY.unify_job_log (sync_job,start_date)
values (@jobName,GETDATE())
;

select @jobId = SCOPE_IDENTITY();

declare
	@sql nvarchar(max) = '', 
    @sql0 nvarchar(max) = '',
	@sqlDBsT24 nvarchar(max) = '',
    @sqlDBsTARVOS nvarchar(max) = '',
	@sqlDBsTEMP nvarchar(max) = ''
;

declare
    @serverName varchar(200) = '[MSDWH-21\SIT].',
	@serverDBName varchar(200)= 'T24_DWH_LANDING_SIT',
	@dbName varchar(200), --= --'SET_TARGET_DATABASE',
	@sch_name_t24 varchar(200) = 'dbo',
	@sch_name_tarvos varchar(200) = 'dbo',
	@t_name_t24 varchar(200) = 'V_FBNK_CUSTOMER_TEST',
	@t_name_tarvos varchar(200) = 'V_FBNK_CUSTOMER_TEST'
;

insert into T24_DWH_LANDING_SIT.UNIFY.unify_job_step_log (sync_job_id, event_name) 
values (@jobId, 'Create ##UNIFYTEMPTABLE - T24_Data')

select @jobStepId = SCOPE_IDENTITY();

if object_id('tempdb..##UNIFYTEMPTABLET24') is not null drop table ##UNIFYTEMPTABLET24;

SET @sqlDBsT24 = '
    SELECT RECID AS CUSTOMER_ID, 
    LEGAL_DOC_NAME, 
    L_TAX_RESIDENCE AS DOCUMENT_COUNTRY, 
    L_TAX_ID_NO AS LEGAL_DOC_ID, 
    FULL_NAME AS COMPANY_NAME, 
    STREET
    INTO ##UNIFYTEMPTABLET24
    FROM [' + @serverDBName + '].[' + @sch_name_t24 + '].[' + @t_name_t24 + ']
    WHERE 1 = 1
    AND TARGET = ''7''
    AND RECID NOT IN (SELECT conf_value FROM config.unify WHERE conf_key = ''NonSyncCustomers'');'
;

exec sp_executesql @sqlDBsT24;

select @row_count = @@ROWCOUNT;

update T24_DWH_LANDING_SIT.UNIFY.unify_job_step_log
set row_count = @row_count,
	end_date = getdate()
where id = @jobStepId
;

insert into T24_DWH_LANDING_SIT.UNIFY.unify_job_step_log (sync_job_id, event_name) 
values (@jobId, 'Create ##UNIFYTEMPTABLE - Tarvos_Data')

select @jobStepId = SCOPE_IDENTITY();

if object_id('tempdb..##UNIFYTEMPTABLETARVOS') is not null drop table ##UNIFYTEMPTABLETARVOS;

SET @sqlDBsTARVOS = '
    SELECT DISTINCT
    p.ID AS CUSTOMER_ID,
    pdd.ID AS LEGAL_DOC_NAME
    s.KOD AS DOCUMENT_COUNTRY,
    ppo.ICO AS VAT_NUMBER AS LEGAL_DOC_ID,
    ppo.OBCHODNI_FIRMA AS COMPANY_NAME,
    pa.ULICE AS STREET
    FROM
    tarvos_dep.PARTNER p
    left join tarvos_dep.PAR_PRA_OSOBA ppo
    on p.ID=ppo.ID
    left join tarvos_dep.PAR_DOKLAD pd
    on p.ID=pd.PAR_ID
    left join tarvos_dep.PAR_DRUH_DOKLADU pdd
    on pd.DRUH_ID=pdd.ID
    left join tarvos_dep.STAT s
    on pd.STAT_VYDANI_ID=s.ID
    left join tarvos_dep.PAR_KONTAKT pk
    on pk.PAR_ID=p.ID
    left join tarvos_dep.PAR_ADRESA pa
    on pa.ID=pk.ID
    left join tarvos_dep.PAR_TYP_ADRESY pta
    on pa.TYP_ID=pta.ID
    WHERE 1 = 1
    and p.TYP_ID=1279;
    AND p.ID NOT IN (SELECT conf_value FROM config.unify WHERE conf_key = ''NonSyncCustomers'');'
;

exec sp_executesql @sqlDBsTARVOS;

select @row_count = @@ROWCOUNT;

update T24_DWH_LANDING_SIT.UNIFY.unify_job_step_log
set row_count = @row_count,
	end_date = getdate()
where id = @jobStepId
;

insert into T24_DWH_LANDING_SIT.UNIFY.unify_job_step_log (sync_job_id, event_name) 
values (@jobId, 'Create ##UNIFYTEMPTABLE - MATCH DATA')

select @jobStepId = SCOPE_IDENTITY();

if object_id('tempdb..##TEMPTABLEMATCH') is not null drop table ##TEMPTABLEMATCH;

SET @sqlDBsTEMP = '
	CREATE TABLE ##TEMPTABLEMATCHCORP(
		CUSTOMER_ID_T24 VARCHAR(200), 
		CUSTOMER_ID_TARVOS VARCHAR(200),
		LEGAL_DOC_NAME_T24 VARCHAR(200), 
        LEGAL_DOC_NAME_TARVOS VARCHAR(200), 
		DOCUMENT_COUNTRY_T24 VARCHAR(200), 
        DOCUMENT_COUNTRY_TARVOS VARCHAR(200), 
		LEGAL_DOC_ID_T24 VARCHAR(200), 
        LEGAL_DOC_ID_TARVOS VARCHAR(200), 
		COMPANY_NAME_T24 VARCHAR(200), 
        COMPANY_NAME_TARVOS VARCHAR(200), 
		STREET_T24 VARCHAR(200), 
        STREET_TARVOS VARCHAR(200), 
		POINT_HOLDER VARCHAR(200)) ;';

exec sp_executesql @sqlDBsTEMP;

select @row_count = @@ROWCOUNT;

update T24_DWH_LANDING_SIT.UNIFY.unify_job_step_log
set row_count = @row_count,
	end_date = getdate()
where id = @jobStepId
;


insert into T24_DWH_LANDING_SIT.UNIFY.unify_job_step_log (sync_job_id, event_name) 
values (@jobId, 'Create ##TEMPTABLEMANUALCHECK - MANUAL DATA')

select @jobStepId = SCOPE_IDENTITY();

if object_id('tempdb..##TEMPTABLEMANUALCHECK') is not null drop table ##TEMPTABLEMANUALCHECK;

SET @sqlDBsTEMP = '
	CREATE TABLE ##TEMPTABLEMANUALCHECKCORP(
		CUSTOMER_ID_T24 VARCHAR(200), 
		CUSTOMER_ID_TARVOS VARCHAR(200),
		LEGAL_DOC_NAME_T24 VARCHAR(200), 
        LEGAL_DOC_NAME_TARVOS VARCHAR(200), 
		DOCUMENT_COUNTRY_T24 VARCHAR(200), 
        DOCUMENT_COUNTRY_TARVOS VARCHAR(200), 
		LEGAL_DOC_ID_T24 VARCHAR(200), 
        LEGAL_DOC_ID_TARVOS VARCHAR(200), 
		COMPANY_NAME_T24 VARCHAR(200), 
        COMPANY_NAME_TARVOS VARCHAR(200), 
		STREET_T24 VARCHAR(200), 
        STREET_TARVOS VARCHAR(200), 
		POINT_HOLDER VARCHAR(200)) ;';

exec sp_executesql @sqlDBsTEMP;

select @row_count = @@ROWCOUNT;

update T24_DWH_LANDING_SIT.UNIFY.unify_job_step_log
set row_count = @row_count,
	end_date = getdate()
where id = @jobStepId
;

DECLARE
@RECID_TARVOS varchar(200),
@LEGAL_DOC_NAME_TARVOS varchar(200),
@L_TAX_RESIDENCE_TARVOS varchar(200),
@L_TAX_ID_NO_TARVOS varchar(200),
@DATE_OF_BIRTH_TARVOS varchar(200),
@FAMILY_NAME_TARVOS varchar(200),
@STREET_TARVOS varchar(200),
@RECID_T24 varchar(200),
@LEGAL_DOC_NAME_T24 varchar(200),
@L_TAX_RESIDENCE_T24 varchar(200),
@L_TAX_ID_NO_T24 varchar(200),
@DATE_OF_BIRTH_T24 varchar(200),
@FAMILY_NAME_T24 varchar(200),
@STREET_T24 varchar(200),
@POINT_HOLDER INT = 0
;

DECLARE unify_crsr CURSOR FOR
select RECID, LEGAL_DOC_NAME, L_TAX_RESIDENCE, L_TAX_ID_NO, DATE_OF_BIRTH, FAMILY_NAME, STREET
from ##AUXUNFIYTEMPTABLETARVOS
;
OPEN unify_crsr;
FETCH NEXT FROM unify_crsr INTO @RECID_TARVOS, @LEGAL_DOC_NAME_TARVOS, @L_TAX_RESIDENCE_TARVOS, @L_TAX_ID_NO_TARVOS, @DATE_OF_BIRTH_TARVOS, @FAMILY_NAME_TARVOS, @STREET_TARVOS;
WHILE @@FETCH_STATUS = 0
BEGIN
	
	
	insert into T24_DWH_LANDING_SIT.UNIFY.unify_job_step_log (sync_job_id, event_name)
	values (@jobId, 'Processing CUSTOMER ' + @RECID_TARVOS)
	;

	select @jobStepId = SCOPE_IDENTITY();

    DECLARE inner_unify_crsr CURSOR FOR
    SELECT RECID, LEGAL_DOC_NAME, L_TAX_RESIDENCE, L_TAX_ID_NO, DATE_OF_BIRTH, FAMILY_NAME, STREET
    FROM ##UNIFYTEMPTABLET24;
    OPEN inner_unify_crsr;
    FETCH NEXT FROM inner_unify_crsr INTO @RECID_T24, @LEGAL_DOC_NAME_T24, @L_TAX_RESIDENCE_T24, @L_TAX_ID_NO_T24, @DATE_OF_BIRTH_T24, @FAMILY_NAME_T24, @STREET_T24;
	WHILE @@FETCH_STATUS = 0
	BEGIN

		--IF @LEGAL_DOC_NAME_T24 = @LEGAL_DOC_NAME_TARVOS SET @POINT_HOLDER= @POINT_HOLDER + 40;
		IF @L_TAX_RESIDENCE_T24 = @L_TAX_RESIDENCE_TARVOS SET @POINT_HOLDER= @POINT_HOLDER + 30;
		IF (@L_TAX_ID_NO_T24 = @L_TAX_ID_NO_TARVOS AND @LEGAL_DOC_NAME_T24 = @LEGAL_DOC_NAME_TARVOS) SET @POINT_HOLDER= @POINT_HOLDER + 40;
		IF @DATE_OF_BIRTH_T24 = @DATE_OF_BIRTH_TARVOS SET @POINT_HOLDER= @POINT_HOLDER + 30;
		IF @FAMILY_NAME_T24 = @FAMILY_NAME_TARVOS SET @POINT_HOLDER= @POINT_HOLDER + 10;
		IF @STREET_T24 = @STREET_TARVOS SET @POINT_HOLDER= @POINT_HOLDER + 10;

		IF @POINT_HOLDER = 120 
			BEGIN
				INSERT INTO ##TEMPTABLEMATCHCORP (RECID, RECID_TARVOS, LEGAL_DOC_NAME, L_TAX_RESIDENCE, L_TAX_ID_NO, DATE_OF_BIRTH, FAMILY_NAME, STREET, POINT_HOLDER)
				VALUES (@RECID_T24,@RECID_TARVOS, @LEGAL_DOC_NAME_T24, @L_TAX_RESIDENCE_T24, @L_TAX_ID_NO_T24, @DATE_OF_BIRTH_T24, @FAMILY_NAME_T24, @STREET_T24, @POINT_HOLDER);
			END
		ELSE IF @POINT_HOLDER >= 90 AND @POINT_HOLDER < 120
			BEGIN
				INSERT INTO ##TEMPTABLEMANUALCHECKCORP (RECID, RECID_TARVOS,LEGAL_DOC_NAME, L_TAX_RESIDENCE, L_TAX_ID_NO, DATE_OF_BIRTH, FAMILY_NAME, STREET, POINT_HOLDER)
				VALUES (@RECID_T24,@RECID_TARVOS, @LEGAL_DOC_NAME_T24, @L_TAX_RESIDENCE_T24, @L_TAX_ID_NO_T24, @DATE_OF_BIRTH_T24, @FAMILY_NAME_T24, @STREET_T24, @POINT_HOLDER);
			END

		SET @POINT_HOLDER = 0;
		FETCH NEXT FROM inner_unify_crsr INTO @RECID_T24, @LEGAL_DOC_NAME_T24, @L_TAX_RESIDENCE_T24, @L_TAX_ID_NO_T24, @DATE_OF_BIRTH_T24, @FAMILY_NAME_T24, @STREET_T24;
    END;
    CLOSE inner_unify_crsr;
    DEALLOCATE inner_unify_crsr;
    
		
	update T24_DWH_LANDING_SIT.AUX.sync_job_step_log
	set end_date = getdate()
	where id = @jobStepId
	;
	FETCH NEXT FROM unify_crsr INTO  @RECID_TARVOS, @LEGAL_DOC_NAME_TARVOS, @L_TAX_RESIDENCE_TARVOS, @L_TAX_ID_NO_TARVOS, @DATE_OF_BIRTH_TARVOS, @FAMILY_NAME_TARVOS, @STREET_TARVOS;
END;
CLOSE unify_crsr;
DEALLOCATE unify_crsr;

UPDATE T24_DWH_LANDING_SIT.UNIFY.unify_job_log SET end_date = GETDATE() where id = @jobId;

--SELECT * FROM ##TEMPTABLEMATCH
--SELECT * FROM ##TEMPTABLEMANUALCHECK
--SELECT * FROM V_FBNK_CUSTOMER_TEST
