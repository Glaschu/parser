/*
================================================================================
Procedure:      dbo.usp_ProcessDailyCoreBankingSettlement_Monster
Author:         ChatGPT (synthetic example)
Created:        2025-08-18
Purpose:        Monster-sized, banking-style stored procedure to simulate a
                full daily settlement pipeline. Demonstrates complex T‑SQL
                constructs: temp tables, table variables, windowing, CTEs,
                MERGE/UPSERT patterns, batching, idempotency keys, audit,
                retry patterns, validation layers, FX conversion, fees,
                GL postings, and reconciliation summaries.

IMPORTANT:      This is a synthetic template intended for testing parsers and
                lineage tools. It references example schemas:
                  - Staging.* (incoming raw files)
                  - Ref.*     (reference/master data)
                  - Core.*    (gold/ledger stores)
                  - Audit.*   (audit, logging)
                  - Ops.*     (operational configs)
                Adjust object names/types to your environment.
================================================================================
*/

CREATE PROCEDURE dbo.usp_ProcessDailyCoreBankingSettlement_Monster
    @BatchDate              DATE,
    @BatchId                BIGINT,
    @RequestedBy            NVARCHAR(128),
    @ForceReprocess         BIT         = 0,       -- if 1, clear prior partials for the batch
    @DryRun                 BIT         = 0,       -- if 1, do all but final commits to Core
    @DefaultCurrency        CHAR(3)     = 'EUR',   -- for FX gaps
    @BatchSize              INT         = 10000,   -- iterative processing chunk size
    @MaxRetry               INT         = 2,       -- internal micro-retries on chunk failures
    @PostingDateOverride    DATE        = NULL     -- optional GL posting date override
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT OFF;  -- allow TRY/CATCH handling without killing session

    -----------------------------------------------------------------------------
    -- 0. Runtime variables & session context
    -----------------------------------------------------------------------------
    DECLARE @proc_name SYSNAME = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID);
    DECLARE @ts_start DATETIME2(3) = SYSDATETIME();
    DECLARE @ts_now   DATETIME2(3);
    DECLARE @msg      NVARCHAR(4000);

    DECLARE @PostingDate DATE = COALESCE(@PostingDateOverride, @BatchDate);

    -- Idempotency key for this run
    DECLARE @IdemKey VARBINARY(16) = CONVERT(VARBINARY(16), HASHBYTES('MD5',
        CONCAT(CONVERT(VARCHAR(10), @BatchDate, 120), '|', @BatchId, '|', @RequestedBy)));

    -----------------------------------------------------------------------------
    -- 1. Safety checks & batch registration
    -----------------------------------------------------------------------------
    IF NOT EXISTS (SELECT 1 FROM Ops.BatchRegistry WITH (UPDLOCK, HOLDLOCK)
                   WHERE BatchId = @BatchId AND BatchDate = @BatchDate)
    BEGIN
        INSERT INTO Ops.BatchRegistry (BatchId, BatchDate, RequestedBy, Status, CreatedAt)
        VALUES (@BatchId, @BatchDate, @RequestedBy, 'CREATED', SYSDATETIME());
    END

    IF @ForceReprocess = 1
    BEGIN
        UPDATE Ops.BatchRegistry
           SET Status = 'REPROCESSING', UpdatedAt = SYSDATETIME()
         WHERE BatchId = @BatchId AND BatchDate = @BatchDate;

        -- Clear any prior partial artifacts for this batch
        DELETE FROM Core.LedgerWork WHERE BatchId = @BatchId;
        DELETE FROM Core.GLWork     WHERE BatchId = @BatchId;
        DELETE FROM Audit.FailedTxn WHERE BatchId = @BatchId;
        DELETE FROM Audit.StepLog   WHERE BatchId = @BatchId;
    END

    -----------------------------------------------------------------------------
    -- 2. Temp objects
    -----------------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#Raw') IS NOT NULL DROP TABLE #Raw;
    IF OBJECT_ID('tempdb..#Fx')  IS NOT NULL DROP TABLE #Fx;
    IF OBJECT_ID('tempdb..#Acct') IS NOT NULL DROP TABLE #Acct;
    IF OBJECT_ID('tempdb..#Stage') IS NOT NULL DROP TABLE #Stage;
    IF OBJECT_ID('tempdb..#Valid') IS NOT NULL DROP TABLE #Valid;
    IF OBJECT_ID('tempdb..#Invalid') IS NOT NULL DROP TABLE #Invalid;
    IF OBJECT_ID('tempdb..#Fees') IS NOT NULL DROP TABLE #Fees;
    IF OBJECT_ID('tempdb..#Post') IS NOT NULL DROP TABLE #Post;
    IF OBJECT_ID('tempdb..#GL') IS NOT NULL DROP TABLE #GL;

    CREATE TABLE #Raw (
        SrcId           BIGINT         NOT NULL,
        TxnExternalId   NVARCHAR(64)   NOT NULL,
        AccountNo       NVARCHAR(32)   NOT NULL,
        Counterparty    NVARCHAR(128)  NULL,
        TxnDate         DATETIME2(3)   NOT NULL,
        ValueDate       DATE           NULL,
        Amount          DECIMAL(19,4)  NOT NULL,
        Currency        CHAR(3)        NOT NULL,
        Direction       CHAR(1)        NOT NULL,  -- 'C' credit, 'D' debit
        TxnType         NVARCHAR(50)   NOT NULL,
        Channel         NVARCHAR(50)   NULL,
        Narrative       NVARCHAR(400)  NULL,
        BatchId         BIGINT         NOT NULL,
        BatchDate       DATE           NOT NULL
    );

    CREATE TABLE #Fx (
        FromCcy CHAR(3) NOT NULL,
        ToCcy   CHAR(3) NOT NULL,
        Rate    DECIMAL(19,8) NOT NULL,
        AsOf    DATE NOT NULL,
        PRIMARY KEY (FromCcy, ToCcy, AsOf)
    );

    CREATE TABLE #Acct (
        AccountNo       NVARCHAR(32)  NOT NULL PRIMARY KEY,
        AccountId       BIGINT        NOT NULL,
        CustomerId      BIGINT        NOT NULL,
        BranchCode      NVARCHAR(10)  NOT NULL,
        Status          NVARCHAR(20)  NOT NULL, -- ACTIVE, FROZEN, CLOSED
        BaseCurrency    CHAR(3)       NOT NULL,
        OverdraftLimit  DECIMAL(19,4) NOT NULL,
        ProductCode     NVARCHAR(20)  NOT NULL
    );

    CREATE TABLE #Stage (
        -- raw + resolved IDs
        SrcId           BIGINT         NOT NULL,
        TxnExternalId   NVARCHAR(64)   NOT NULL,
        AccountNo       NVARCHAR(32)   NOT NULL,
        AccountId       BIGINT         NULL,
        CustomerId      BIGINT         NULL,
        BranchCode      NVARCHAR(10)   NULL,
        Status          NVARCHAR(20)   NULL,
        BaseCurrency    CHAR(3)        NULL,
        OverdraftLimit  DECIMAL(19,4)  NULL,
        ProductCode     NVARCHAR(20)   NULL,

        TxnDate         DATETIME2(3)   NOT NULL,
        ValueDate       DATE           NULL,
        AmountRaw       DECIMAL(19,4)  NOT NULL,
        CurrencyRaw     CHAR(3)        NOT NULL,
        AmountBase      DECIMAL(19,4)  NULL,
        CurrencyBase    CHAR(3)        NULL,
        Direction       CHAR(1)        NOT NULL,
        TxnType         NVARCHAR(50)   NOT NULL,
        Channel         NVARCHAR(50)   NULL,
        Narrative       NVARCHAR(400)  NULL,

        RiskScore       DECIMAL(9,4)   NULL,
        FeeAmount       DECIMAL(19,4)  NULL,
        FeeCode         NVARCHAR(20)   NULL,
        IsDuplicate     BIT            NOT NULL DEFAULT 0,
        IsValid         BIT            NULL,
        InvalidReason   NVARCHAR(400)  NULL,
        HashId          VARBINARY(16)  NULL
    );

    CREATE TABLE #Valid (
        RowNum BIGINT IDENTITY(1,1) PRIMARY KEY,
        HashId VARBINARY(16) NOT NULL
    );

    CREATE TABLE #Invalid (
        SrcId BIGINT NOT NULL,
        TxnExternalId NVARCHAR(64) NOT NULL,
        Reason NVARCHAR(400) NOT NULL
    );

    CREATE TABLE #Fees (
        HashId VARBINARY(16) NOT NULL PRIMARY KEY,
        FeeCode NVARCHAR(20) NOT NULL,
        FeeAmount DECIMAL(19,4) NOT NULL
    );

    CREATE TABLE #Post (
        -- the final posting grain to Core.LedgerWork
        HashId VARBINARY(16) NOT NULL PRIMARY KEY,
        AccountId BIGINT NOT NULL,
        PostingDate DATE NOT NULL,
        AmountBase DECIMAL(19,4) NOT NULL,
        Direction CHAR(1) NOT NULL,
        TxnType NVARCHAR(50) NOT NULL,
        FeeAmount DECIMAL(19,4) NOT NULL DEFAULT 0,
        RiskScore DECIMAL(9,4) NULL,
        Narrative NVARCHAR(400) NULL,
        BatchId BIGINT NOT NULL,
        CreatedAt DATETIME2(3) NOT NULL
    );

    CREATE TABLE #GL (
        -- staged GL lines (double-entry) ready for Core.GLWork
        LineId BIGINT IDENTITY(1,1) PRIMARY KEY,
        HashId VARBINARY(16) NOT NULL,
        GLAccount NVARCHAR(32) NOT NULL,
        Debit DECIMAL(19,4) NOT NULL DEFAULT 0,
        Credit DECIMAL(19,4) NOT NULL DEFAULT 0,
        PostingDate DATE NOT NULL,
        Narrative NVARCHAR(400) NULL
    );

    -----------------------------------------------------------------------------
    -- 3. Seed temp from source tables (raw, fx, accounts)
    -----------------------------------------------------------------------------
    INSERT INTO #Raw (SrcId, TxnExternalId, AccountNo, Counterparty, TxnDate, ValueDate,
                      Amount, Currency, Direction, TxnType, Channel, Narrative, BatchId, BatchDate)
    SELECT r.SrcId, r.TxnExternalId, r.AccountNo, r.Counterparty, r.TxnDate, r.ValueDate,
           r.Amount, r.Currency, r.Direction, r.TxnType, r.Channel, r.Narrative, r.BatchId, r.BatchDate
      FROM Staging.Transactions r WITH (NOLOCK)
     WHERE r.BatchId = @BatchId AND r.BatchDate = @BatchDate;

    INSERT INTO #Fx (FromCcy, ToCcy, Rate, AsOf)
    SELECT f.FromCurrency, f.ToCurrency, f.Rate, f.AsOf
      FROM Ref.CurrencyRate f WITH (NOLOCK)
     WHERE f.AsOf IN (@BatchDate, DATEADD(DAY,-1,@BatchDate))
       AND f.ToCurrency = @DefaultCurrency;

    INSERT INTO #Acct (AccountNo, AccountId, CustomerId, BranchCode, Status, BaseCurrency, OverdraftLimit, ProductCode)
    SELECT a.AccountNo, a.AccountId, a.CustomerId, a.BranchCode, a.Status, a.BaseCurrency, a.OverdraftLimit, a.ProductCode
      FROM Ref.Account a WITH (NOLOCK);

    -----------------------------------------------------------------------------
    -- 4. Stage + resolve account/FX + compute hash id (idempotency)
    -----------------------------------------------------------------------------
    ;WITH R AS (
        SELECT * FROM #Raw
    ), A AS (
        SELECT * FROM #Acct
    ), J AS (
        SELECT R.*, A.AccountId, A.CustomerId, A.BranchCode, A.Status AS AcctStatus,
               A.BaseCurrency, A.OverdraftLimit, A.ProductCode
          FROM R
          LEFT JOIN A ON A.AccountNo = R.AccountNo
    ), X AS (
        SELECT J.*,
               COALESCE(fr.Rate, 1.00000000) AS FxRate,
               COALESCE(fr.ToCcy, J.Currency) AS ToCcy
          FROM J
          LEFT JOIN #Fx fr
            ON fr.FromCcy = J.Currency AND fr.AsOf IN (J.BatchDate, DATEADD(DAY,-1,J.BatchDate))
           AND fr.ToCcy = @DefaultCurrency
    )
    INSERT INTO #Stage (
        SrcId, TxnExternalId, AccountNo, AccountId, CustomerId, BranchCode, Status,
        BaseCurrency, OverdraftLimit, ProductCode,
        TxnDate, ValueDate, AmountRaw, CurrencyRaw, AmountBase, CurrencyBase,
        Direction, TxnType, Channel, Narrative,
        RiskScore, FeeAmount, FeeCode, IsDuplicate, IsValid, InvalidReason, HashId)
    SELECT x.SrcId, x.TxnExternalId, x.AccountNo, x.AccountId, x.CustomerId, x.BranchCode, x.AcctStatus,
           x.BaseCurrency, x.OverdraftLimit, x.ProductCode,
           x.TxnDate, x.ValueDate, x.Amount, x.Currency,
           ROUND(x.Amount * x.FxRate, 2) AS AmountBase,
           @DefaultCurrency AS CurrencyBase,
           x.Direction, x.TxnType, x.Channel, x.Narrative,
           NULL AS RiskScore, NULL AS FeeAmount, NULL AS FeeCode,
           0 AS IsDuplicate, NULL AS IsValid, NULL AS InvalidReason,
           CONVERT(VARBINARY(16), HASHBYTES('MD5',
               CONCAT(@BatchId,'|',@BatchDate,'|',x.TxnExternalId,'|',x.AccountNo,'|',
                      CONVERT(VARCHAR(50),x.Amount),'|',x.Currency,'|',x.Direction,'|',x.TxnType))) AS HashId
      FROM X x;

    -----------------------------------------------------------------------------
    -- 5. De-duplication within batch & vs Core (idempotency)
    -----------------------------------------------------------------------------
    UPDATE s
       SET IsDuplicate = 1
      FROM #Stage s
     WHERE EXISTS (
            SELECT 1 FROM Core.Ledger l WITH (NOLOCK)
             WHERE l.IdempotencyKey = s.HashId
           )
        OR EXISTS (
            SELECT 1 FROM Core.LedgerWork lw WITH (NOLOCK)
             WHERE lw.IdempotencyKey = s.HashId AND lw.BatchId = @BatchId
           );

    -----------------------------------------------------------------------------
    -- 6. Validation layers (account status, direction, amounts, currency, dates)
    -----------------------------------------------------------------------------
    UPDATE s
       SET IsValid = CASE
                        WHEN s.IsDuplicate = 1 THEN 0
                        WHEN s.AccountId IS NULL THEN 0
                        WHEN s.Status NOT IN ('ACTIVE') THEN 0
                        WHEN s.Direction NOT IN ('C','D') THEN 0
                        WHEN s.AmountBase IS NULL OR s.AmountBase <= 0 THEN 0
                        WHEN s.CurrencyBase <> @DefaultCurrency THEN 0
                        WHEN s.TxnDate > DATEADD(DAY,1,CAST(@BatchDate AS DATETIME2(0))) THEN 0
                        ELSE 1
                     END,
           InvalidReason = CASE
                        WHEN s.IsDuplicate = 1 THEN 'DUPLICATE'
                        WHEN s.AccountId IS NULL THEN 'UNKNOWN_ACCOUNT'
                        WHEN s.Status NOT IN ('ACTIVE') THEN 'ACCOUNT_NOT_ACTIVE'
                        WHEN s.Direction NOT IN ('C','D') THEN 'BAD_DIRECTION'
                        WHEN s.AmountBase IS NULL OR s.AmountBase <= 0 THEN 'NON_POSITIVE_AMOUNT'
                        WHEN s.CurrencyBase <> @DefaultCurrency THEN 'BAD_BASE_CURRENCY'
                        WHEN s.TxnDate > DATEADD(DAY,1,CAST(@BatchDate AS DATETIME2(0))) THEN 'FUTURE_DATED'
                        ELSE NULL
                     END
      FROM #Stage s;

    INSERT INTO #Invalid (SrcId, TxnExternalId, Reason)
    SELECT s.SrcId, s.TxnExternalId, s.InvalidReason
      FROM #Stage s
     WHERE s.IsValid = 0;

    -----------------------------------------------------------------------------
    -- 7. AML / Risk scoring (simple synthetic rule set)
    -----------------------------------------------------------------------------
    ;WITH Scores AS (
        SELECT s.HashId,
               CAST(
                    (CASE WHEN s.AmountBase >= 10000 THEN 0.40 ELSE 0 END) +
                    (CASE WHEN s.Channel IN ('WIRE','INTL') THEN 0.30 ELSE 0 END) +
                    (CASE WHEN s.TxnType IN ('CASH','CRYPTO') THEN 0.20 ELSE 0 END) +
                    (CASE WHEN s.Narrative LIKE '%gift%' THEN 0.10 ELSE 0 END)
               AS DECIMAL(9,4)) AS Score
          FROM #Stage s
         WHERE s.IsValid = 1
    )
    UPDATE s SET RiskScore = sc.Score
      FROM #Stage s
      JOIN Scores sc ON sc.HashId = s.HashId;

    -----------------------------------------------------------------------------
    -- 8. Fee engine (tiered fees by product + channel)
    -----------------------------------------------------------------------------
    ;WITH FeeRule AS (
        SELECT p.ProductCode, c.Channel, c.MinAmt, c.MaxAmt, c.FeeCode, c.FeeFlat, c.FeePct
          FROM Ref.FeeConfig c
          JOIN Ref.Product p ON p.ProductCode = c.ProductCode
    ), FeeCalc AS (
        SELECT s.HashId, 
               fr.FeeCode,
               CAST(ROUND(COALESCE(fr.FeeFlat,0) + (COALESCE(fr.FeePct,0) * s.AmountBase), 2) AS DECIMAL(19,4)) AS FeeAmount
          FROM #Stage s
          JOIN FeeRule fr
            ON fr.ProductCode = s.ProductCode
           AND (fr.Channel = s.Channel OR fr.Channel IS NULL)
           AND s.AmountBase BETWEEN fr.MinAmt AND fr.MaxAmt
         WHERE s.IsValid = 1
    )
    INSERT INTO #Fees (HashId, FeeCode, FeeAmount)
    SELECT HashId, FeeCode, FeeAmount FROM FeeCalc;

    UPDATE s
       SET s.FeeAmount = f.FeeAmount,
           s.FeeCode   = f.FeeCode
      FROM #Stage s
      JOIN #Fees f ON f.HashId = s.HashId;

    -----------------------------------------------------------------------------
    -- 9. Overdraft / balance pre-check (synthetic; assumes Core.Balances snapshot)
    -----------------------------------------------------------------------------
    ;WITH Bal AS (
        SELECT b.AccountId, b.AvailableBalance
          FROM Core.Balances b WITH (NOLOCK)
    ), NeedCheck AS (
        SELECT s.HashId, s.AccountId, s.AmountBase, s.Direction, s.OverdraftLimit
          FROM #Stage s
         WHERE s.IsValid = 1
    )
    UPDATE s
       SET IsValid = CASE
                        WHEN s.Direction = 'D' AND (b.AvailableBalance - s.AmountBase) < -s.OverdraftLimit THEN 0
                        ELSE s.IsValid
                     END,
           InvalidReason = CASE
                        WHEN s.Direction = 'D' AND (b.AvailableBalance - s.AmountBase) < -s.OverdraftLimit THEN 'OVERDRAFT_LIMIT_EXCEEDED'
                        ELSE s.InvalidReason
                     END
      FROM #Stage s
      JOIN Bal b ON b.AccountId = s.AccountId;

    -- capture new invalids from overdraft check
    INSERT INTO #Invalid (SrcId, TxnExternalId, Reason)
    SELECT s.SrcId, s.TxnExternalId, s.InvalidReason
      FROM #Stage s
     WHERE s.IsValid = 0
       AND NOT EXISTS (SELECT 1 FROM #Invalid i WHERE i.SrcId = s.SrcId);

    -----------------------------------------------------------------------------
    -- 10. Prepare posting set
    -----------------------------------------------------------------------------
    INSERT INTO #Post (HashId, AccountId, PostingDate, AmountBase, Direction, TxnType, FeeAmount, RiskScore, Narrative, BatchId, CreatedAt)
    SELECT s.HashId, s.AccountId, @PostingDate, s.AmountBase, s.Direction, s.TxnType,
           COALESCE(s.FeeAmount,0), s.RiskScore, s.Narrative, @BatchId, SYSDATETIME()
      FROM #Stage s
     WHERE s.IsValid = 1;

    -- registered valid HashIds for batching
    INSERT INTO #Valid (HashId)
    SELECT p.HashId FROM #Post p;

    -----------------------------------------------------------------------------
    -- 11. Batch processing loop with micro-retries
    -----------------------------------------------------------------------------
    DECLARE @rows BIGINT, @lo BIGINT = 1, @hi BIGINT, @attempt INT, @batch_end BIGINT, @affected INT;

    SELECT @rows = COUNT(*) FROM #Valid;

    WHILE @lo <= @rows
    BEGIN
        SET @batch_end = @lo + @BatchSize - 1;
        SELECT @hi = CASE WHEN @batch_end > @rows THEN @rows ELSE @batch_end END;

        SET @attempt = 0;
        WHILE @attempt <= @MaxRetry
        BEGIN
            BEGIN TRY
                BEGIN TRANSACTION;

                -- 11.1 Upsert to Core.LedgerWork (idempotent by HashId)
                MERGE Core.LedgerWork AS tgt
                USING (
                    SELECT v.HashId, p.AccountId, p.PostingDate, p.AmountBase, p.Direction, p.TxnType,
                           p.FeeAmount, p.RiskScore, p.Narrative, p.BatchId, @IdemKey AS IdempotencyKey
                      FROM #Valid v
                      JOIN #Post  p ON p.HashId = v.HashId
                     WHERE v.RowNum BETWEEN @lo AND @hi
                ) AS src
                   ON tgt.IdempotencyKey = src.HashId  -- work table uses HashId as idem marker
                WHEN MATCHED THEN
                    UPDATE SET tgt.AccountId   = src.AccountId,
                               tgt.PostingDate = src.PostingDate,
                               tgt.AmountBase  = src.AmountBase,
                               tgt.Direction   = src.Direction,
                               tgt.TxnType     = src.TxnType,
                               tgt.FeeAmount   = src.FeeAmount,
                               tgt.RiskScore   = src.RiskScore,
                               tgt.Narrative   = src.Narrative,
                               tgt.BatchId     = src.BatchId,
                               tgt.UpdatedAt   = SYSDATETIME()
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (IdempotencyKey, AccountId, PostingDate, AmountBase, Direction, TxnType,
                            FeeAmount, RiskScore, Narrative, BatchId, CreatedAt)
                    VALUES (src.HashId, src.AccountId, src.PostingDate, src.AmountBase, src.Direction, src.TxnType,
                            src.FeeAmount, src.RiskScore, src.Narrative, src.BatchId, SYSDATETIME());

                -- 11.2 Derive GL lines for the batch slice (double entry)
                ;WITH Slice AS (
                    SELECT v.HashId, p.AccountId, p.AmountBase, p.Direction
                      FROM #Valid v
                      JOIN #Post  p ON p.HashId = v.HashId
                     WHERE v.RowNum BETWEEN @lo AND @hi
                ), Map AS (
                    SELECT m.ProductCode, m.TxnType, m.GL_Debit, m.GL_Credit
                      FROM Ref.GLMap m
                ), Src AS (
                    SELECT s.HashId, s.AccountId, p.TxnType, p.AmountBase, p.Direction,
                           a.ProductCode
                      FROM Slice s
                      JOIN #Post p ON p.HashId = s.HashId
                      JOIN #Stage st ON st.HashId = s.HashId
                      JOIN #Acct a ON a.AccountId = st.AccountId
                ), JoinMap AS (
                    SELECT Src.HashId, COALESCE(Map.GL_Debit,'9999') AS GL_Debit,
                           COALESCE(Map.GL_Credit,'9999') AS GL_Credit, Src.AmountBase, Src.Direction
                      FROM Src
                 LEFT JOIN Map
                        ON Map.ProductCode = Src.ProductCode AND Map.TxnType = Src.TxnType
                )
                INSERT INTO #GL (HashId, GLAccount, Debit, Credit, PostingDate, Narrative)
                SELECT j.HashId,
                       CASE WHEN j.Direction = 'D' THEN j.GL_Debit ELSE j.GL_Credit END AS GLAccount,
                       CASE WHEN j.Direction = 'D' THEN j.AmountBase ELSE 0 END AS Debit,
                       CASE WHEN j.Direction = 'C' THEN j.AmountBase ELSE 0 END AS Credit,
                       @PostingDate,
                       CONCAT('Batch ', @BatchId, ' idem ', CONVERT(VARBINARY(16), j.HashId))
                  FROM JoinMap j;

                -- 11.3 Optional final commit to Core (if not @DryRun)
                IF @DryRun = 0
                BEGIN
                    -- Move LedgerWork -> Ledger
                    INSERT INTO Core.Ledger (IdempotencyKey, AccountId, PostingDate, AmountBase, Direction,
                                             TxnType, FeeAmount, RiskScore, Narrative, BatchId, CreatedAt)
                    SELECT w.IdempotencyKey, w.AccountId, w.PostingDate, w.AmountBase, w.Direction,
                           w.TxnType, w.FeeAmount, w.RiskScore, w.Narrative, w.BatchId, SYSDATETIME()
                      FROM Core.LedgerWork w
                     WHERE w.BatchId = @BatchId
                       AND w.CreatedAt >= @ts_start;  -- only current-run rows

                    -- Post GL work -> GL (synthetic)
                    INSERT INTO Core.GLWork (HashId, GLAccount, Debit, Credit, PostingDate, Narrative, BatchId)
                    SELECT g.HashId, g.GLAccount, g.Debit, g.Credit, g.PostingDate, g.Narrative, @BatchId
                      FROM #GL g
                     WHERE g.HashId IN (SELECT v.HashId FROM #Valid v WHERE v.RowNum BETWEEN @lo AND @hi);
                END

                COMMIT TRANSACTION;
                SET @affected = @hi - @lo + 1;

                SET @ts_now = SYSDATETIME();
                INSERT INTO Audit.StepLog (BatchId, BatchDate, StepName, Detail, [RowCount], Ts)
                VALUES (@BatchId, @BatchDate, 'UPSERT+BATCH', CONCAT('rows ', @lo, '-', @hi), @affected, @ts_now);

                BREAK; -- success: exit retry loop
            END TRY
            BEGIN CATCH
                ROLLBACK TRANSACTION;
                SET @attempt += 1;

                SET @msg = CONCAT('Batch slice ', @lo, '-', @hi, ' failed attempt ', @attempt,
                                  ': ', ERROR_MESSAGE());
                INSERT INTO Audit.StepLog (BatchId, BatchDate, StepName, Detail, [RowCount], Ts)
                VALUES (@BatchId, @BatchDate, 'RETRY', @msg, 0, SYSDATETIME());

                IF @attempt > @MaxRetry
                BEGIN
                    -- capture slice failures into Audit.FailedTxn by HashId mapping
                    INSERT INTO Audit.FailedTxn (BatchId, TxnExternalId, Reason, CreatedAt)
                    SELECT @BatchId, s.TxnExternalId, CONCAT('SLICE_FAIL: ', ERROR_MESSAGE()), SYSDATETIME()
                      FROM #Stage s
                      JOIN #Valid v ON v.HashId = s.HashId
                     WHERE v.RowNum BETWEEN @lo AND @hi;
                    -- do not BREAK; proceed to next slice
                    BREAK;
                END
            END CATCH
        END -- retry loop

        SET @lo = @hi + 1;
    END -- while slices

    -----------------------------------------------------------------------------
    -- 12. Persist invalids and duplicates to audit (non-blocking)
    -----------------------------------------------------------------------------
    INSERT INTO Audit.FailedTxn (BatchId, TxnExternalId, Reason, CreatedAt)
    SELECT DISTINCT @BatchId, i.TxnExternalId, i.Reason, SYSDATETIME()
      FROM #Invalid i;

    -----------------------------------------------------------------------------
    -- 13. Reconciliation snapshots & summaries
    -----------------------------------------------------------------------------
    DECLARE @total_raw DECIMAL(19,4) = (SELECT COALESCE(SUM(Amount),0) FROM #Raw);
    DECLARE @total_stage_valid DECIMAL(19,4) = (SELECT COALESCE(SUM(AmountBase),0) FROM #Stage WHERE IsValid = 1);
    DECLARE @total_stage_invalid DECIMAL(19,4) = (SELECT COALESCE(SUM(AmountBase),0) FROM #Stage WHERE IsValid = 0);
    DECLARE @count_valid BIGINT = (SELECT COUNT(*) FROM #Stage WHERE IsValid = 1);
    DECLARE @count_invalid BIGINT = (SELECT COUNT(*) FROM #Stage WHERE IsValid = 0);
    DECLARE @count_dupe BIGINT = (SELECT COUNT(*) FROM #Stage WHERE IsDuplicate = 1);

    INSERT INTO Audit.ReconSummary (BatchId, BatchDate, Metric, ValueNum, ValueStr, CreatedAt)
    VALUES
        (@BatchId, @BatchDate, 'RAW_TOTAL', @total_raw, NULL, SYSDATETIME()),
        (@BatchId, @BatchDate, 'VALID_TOTAL', @total_stage_valid, NULL, SYSDATETIME()),
        (@BatchId, @BatchDate, 'INVALID_TOTAL', @total_stage_invalid, NULL, SYSDATETIME()),
        (@BatchId, @BatchDate, 'COUNT_VALID', @count_valid, NULL, SYSDATETIME()),
        (@BatchId, @BatchDate, 'COUNT_INVALID', @count_invalid, NULL, SYSDATETIME()),
        (@BatchId, @BatchDate, 'COUNT_DUPLICATE', @count_dupe, NULL, SYSDATETIME());

    -----------------------------------------------------------------------------
    -- 14. Finalization: promote work tables to final tables (if not DryRun)
    -----------------------------------------------------------------------------
    IF @DryRun = 0
    BEGIN
        BEGIN TRY
            BEGIN TRANSACTION;

            -- Move GLWork -> GL
            INSERT INTO Core.GL (HashId, GLAccount, Debit, Credit, PostingDate, Narrative, BatchId, CreatedAt)
            SELECT w.HashId, w.GLAccount, w.Debit, w.Credit, w.PostingDate, w.Narrative, w.BatchId, SYSDATETIME()
              FROM Core.GLWork w
             WHERE w.BatchId = @BatchId;

            -- Update balances (synthetic: apply net per account)
            ;WITH Net AS (
                SELECT AccountId,
                       SUM(CASE WHEN Direction = 'C' THEN AmountBase ELSE -AmountBase END) AS NetAmt
                  FROM Core.LedgerWork
                 WHERE BatchId = @BatchId
                 GROUP BY AccountId
            )
            UPDATE b
               SET b.AvailableBalance = b.AvailableBalance + n.NetAmt,
                   b.UpdatedAt = SYSDATETIME()
              FROM Core.Balances b
              JOIN Net n ON n.AccountId = b.AccountId;

            -- Move LedgerWork -> LedgerFinal + clear work (idempotent by IdempotencyKey)
            INSERT INTO Core.LedgerFinal (IdempotencyKey, AccountId, PostingDate, AmountBase, Direction,
                                          TxnType, FeeAmount, RiskScore, Narrative, BatchId, CreatedAt)
            SELECT w.IdempotencyKey, w.AccountId, w.PostingDate, w.AmountBase, w.Direction,
                   w.TxnType, w.FeeAmount, w.RiskScore, w.Narrative, w.BatchId, SYSDATETIME()
              FROM Core.LedgerWork w
             WHERE w.BatchId = @BatchId
               AND NOT EXISTS (SELECT 1 FROM Core.LedgerFinal f WHERE f.IdempotencyKey = w.IdempotencyKey);

            DELETE FROM Core.LedgerWork WHERE BatchId = @BatchId;
            DELETE FROM Core.GLWork     WHERE BatchId = @BatchId;

            COMMIT TRANSACTION;
        END TRY
        BEGIN CATCH
            ROLLBACK TRANSACTION;
            SET @msg = CONCAT('Finalization failed: ', ERROR_MESSAGE());
            INSERT INTO Audit.StepLog (BatchId, BatchDate, StepName, Detail, [RowCount], Ts)
            VALUES (@BatchId, @BatchDate, 'FINALIZE_FAIL', @msg, 0, SYSDATETIME());
            -- Do not rethrow; allow batch to mark as PARTIAL
        END CATCH
    END

    -----------------------------------------------------------------------------
    -- 15. Mark batch status & emit end log
    -----------------------------------------------------------------------------
    DECLARE @has_failures BIT = CASE WHEN EXISTS (SELECT 1 FROM Audit.FailedTxn WHERE BatchId = @BatchId) THEN 1 ELSE 0 END;

    UPDATE Ops.BatchRegistry
       SET Status = CASE WHEN @DryRun = 1 THEN 'DRYRUN_COMPLETE'
                         WHEN @has_failures = 1 THEN 'COMPLETE_WITH_ERRORS'
                         ELSE 'COMPLETE' END,
           UpdatedAt = SYSDATETIME(),
           MetricsJson = CONCAT('{',
                '"rawTotal":', COALESCE(CONVERT(VARCHAR(40),@total_raw),'0'), ',',
                '"validTotal":', COALESCE(CONVERT(VARCHAR(40),@total_stage_valid),'0'), ',',
                '"invalidTotal":', COALESCE(CONVERT(VARCHAR(40),@total_stage_invalid),'0'), ',',
                '"countValid":', @count_valid, ',',
                '"countInvalid":', @count_invalid, ',',
                '"countDuplicate":', @count_dupe, ',',
                '"postingDate":"', CONVERT(CHAR(10), @PostingDate, 120), '"',
            '}')
     WHERE BatchId = @BatchId AND BatchDate = @BatchDate;

    SET @ts_now = SYSDATETIME();
    INSERT INTO Audit.StepLog (BatchId, BatchDate, StepName, Detail, [RowCount], Ts)
    VALUES (@BatchId, @BatchDate, 'END', CONCAT('proc ', @proc_name, ' duration ms ', DATEDIFF(ms, @ts_start, @ts_now)), 0, @ts_now);

    -----------------------------------------------------------------------------
    -- 16. Return result sets for quick inspection
    -----------------------------------------------------------------------------
    SELECT TOP (50) * FROM #Stage ORDER BY IsValid DESC, AmountBase DESC;
    SELECT TOP (50) * FROM #Invalid ORDER BY Reason, TxnExternalId;
    SELECT TOP (50) * FROM #GL ORDER BY LineId DESC;

END
GO

/*
USAGE EXAMPLE (adjust for your environment):

EXEC dbo.usp_ProcessDailyCoreBankingSettlement_Monster
     @BatchDate = '2025-08-18',
     @BatchId = 2025081801,
     @RequestedBy = N'jglasgow',
     @ForceReprocess = 1,
     @DryRun = 1,
     @DefaultCurrency = 'EUR',
     @BatchSize = 5000,
     @MaxRetry = 2,
     @PostingDateOverride = NULL;
*/
