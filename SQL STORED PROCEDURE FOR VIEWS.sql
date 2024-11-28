CREATE OR ALTER PROCEDURE PROJECT1.CreateOrAlterDeltaView
    @DeltaFolderName NVARCHAR(MAX)
AS
BEGIN
    DECLARE @SQLQuery NVARCHAR(MAX);

    SET @SQLQuery = N'
    CREATE OR ALTER VIEW PROJECT1.VIEW_DIM_' + @DeltaFolderName + N' AS
    SELECT * FROM OPENROWSET(
        BULK ''' + 'https://adlssalesproject2448pp2.dfs.core.windows.net/gold/' + @DeltaFolderName + ''',
        FORMAT=''DELTA''
    ) AS TBL_DIM_' + @DeltaFolderName + ';';

    EXEC sp_executesql @SQLQuery;
END;
GO

