DECLARE @Object_Id INT;
DECLARE @Stats_Id INT;

DECLARE @StatsName SYSNAME;
SELECT @StatsName = Name, @Object_Id = Object_Id, @Stats_Id = Stats_Id FROM sys.stats
WHERE Name LIKE '%SL_PostedTime%';


DECLARE @MaximumAllowablePercentIncorrect INT = 20;
DECLARE @maxEQRowCount INT = 1000000;
DECLARE @Diagnostics BIT = 0;

IF @Diagnostics = 1 BEGIN
	SELECT * FROM sys.dm_db_Stats_histogram(@Object_Id, @Stats_Id)
	SELECT * FROM sys.dm_db_Stats_properties(@Object_Id, @Stats_Id)
END;


DECLARE @ObjectName SYSNAME = OBJECT_NAME(@Object_Id);
DECLARE @LeadColumnName SYSNAME;
SELECT @LeadColumnName = NAME
	FROM sys.stats_columns SC
	JOIN sys.columns C on 1=1
		AND C.Object_Id = SC.Object_Id 
		AND C.Column_Id = SC.Column_Id
	WHERE 1=1
		AND SC.Object_Id = @Object_Id
		AND Stats_Id = @Stats_Id
		AND Stats_Column_Id = 1;

IF @Diagnostics = 1 BEGIN
	PRINT 'LeadColumn : ' + @LeadColumnName;
END

DECLARE @filter_definition NVARCHAR(MAX);
SELECT @filter_definition = filter_definition FROM sys.stats WHERE Object_Id = @Object_Id AND Stats_Id = @Stats_Id;

DECLARE @Rows BIGINT;
DECLARE @Rows_Sampled BIGINT;
DECLARE @Steps SMALLINT;
SELECT @Steps = Steps, @Rows = Rows, @Rows_Sampled = Rows_Sampled FROM sys.dm_db_Stats_properties(@Object_Id, @Stats_Id)

DECLARE Histogram_Cursor CURSOR FOR
	SELECT Range_High_Key, Equal_Rows FROM sys.dm_db_stats_histogram(@Object_Id, @Stats_Id)
OPEN Histogram_Cursor

DECLARE @Range_High_Key SQL_VARIANT;
DECLARE @Equal_Rows BIGINT;

DECLARE @InRange INT = 0;
DECLARE @OutOfRange INT = 0;
DECLARE @NotChecked INT = 0;

FETCH NEXT FROM Histogram_Cursor
INTO @range_high_key, @equal_rows;

WHILE @@FETCH_STATUS = 0 BEGIN
	FETCH NEXT FROM Histogram_Cursor
	INTO @Range_High_Key, @equal_rows;

	DECLARE @rangeHighKeyNVarchar NVARCHAR(MAX) =
		CASE SQL_VARIANT_PROPERTY(@range_high_key, 'BaseType')
		WHEN 'datetime' then convert(NVARCHAR(MAX), cast(@range_high_key as datetime), 25)
		ELSE cast(@range_high_key AS NVARCHAR(MAX))
		END;

	IF @Equal_Rows <= @maxEQRowCount BEGIN
		DECLARE @Cmd NVARCHAR(MAX) = N'SELECT @RealCount = COUNT(*) FROM ' + @ObjectName + N' WHERE ' + @LeadColumnName + N' = ''' + @rangeHighKeyNVarchar + '''';
		IF @filter_definition IS NOT NULL BEGIN
			SET @cmd += ' AND ' + @filter_definition;
		END;

		IF @Diagnostics = 1 BEGIN
			PRINT 'running : ' + @cmd;
		END

		DECLARE @RealCount BIGINT;
		EXEC sp_ExecuteSql @cmd, N'@RealCount BIGINT Output', @RealCount OUTPUT;

		DECLARE @Difference BIGINT = @RealCount - @equal_Rows;
		DECLARE @OutByPercent DECIMAL = 100.0 * @Difference / @equal_Rows;

		IF ABS(@OutByPercent) > @MaximumAllowablePercentIncorrect BEGIN
			SET @OutOfRange +=1;
			PRINT N''
				+ N'Out of range statistics : Real count for ' + @LeadColumnName 
				+ N' = ' + @rangeHighKeyNVarchar
				+ N' is ' + cast(@RealCount AS NVARCHAR)
				+ N', estimate is ' + cast(@equal_rows AS NVARCHAR)
				+ N', out by ' + cast(@OutByPercent AS NVARCHAR)
				+ N'%';
		END ELSE BEGIN
			SET @InRange += 1;
			IF @Diagnostics = 1 BEGIN
				PRINT N''
					+ N'IN range statistics : Real count for ' + @LeadColumnName 
					+ N' = ' + @rangeHighKeyNVarchar
					+ N' is ' + cast(@RealCount AS NVARCHAR)
					+ N', estimate is ' + cast(@equal_rows AS NVARCHAR)
					+ N', out by ' + cast(@OutByPercent AS NVARCHAR)
					+ N'%';
			END;
		END
	END ELSE BEGIN
		SET @NotChecked += 1;
		IF @Diagnostics = 1 BEGIN
			PRINT N''
				+ N'Skipping ' + @LeadColumnName
				+ N' = ' + cast(@range_high_key AS NVARCHAR(MAX))
				+ N' as estimate is ' + cast(@equal_rows AS NVARCHAR)
				+ N' which exceeds @MaxEQRowCount = ' + cast(@MaxEQRowCount AS NVARCHAR)
		END;
	END
END;

PRINT 'Summary for ' + @StatsName + ' on table ' + @ObjectName
PRINT 'In Range = ' + cast(@InRange AS VARCHAR);
PRINT 'Out Of Range = ' + cast(@OutOfRange AS VARCHAR);
PRINT 'Not Checked = ' + cast(@NotChecked AS VARCHAR);

CLOSE Histogram_Cursor
DEALLOCATE Histogram_Cursor



select substring(text, statement_start_offset /2 +1, (statement_end_offset - statement_Start_offset) /2 + 1), creation_time, last_Execution_Time, execution_count, total_spills, last_spills, total_spills / execution_count AvgSpills, qp.* from sys.dm_exec_query_Stats
cross apply sys.dm_Exec_sql_TExt(sql_handle)
cross apply sys.dm_exec_query_Plan(plan_handle) qp
where total_spills > 0 --and text not like '%GenSpatialData%' and text not like '%TOP 1001%' and text not like '%RefUNLOCO%'
order by total_spills desc

create index faster on GlbReleaseNote (GF_ReleaseNoteDate)
update statistics refunloco with fullscan
update statistics GlbReleaseNote with fullscan
update statistics GlbReleaseNoteRead with fullscan
update statistics GenSpatialData with fullscan
