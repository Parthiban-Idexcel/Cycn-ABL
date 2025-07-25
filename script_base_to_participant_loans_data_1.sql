DROP PROCEDURE IF EXISTS script_base_to_participant_loans_data;
DELIMITER ;;

CREATE PROCEDURE script_base_to_participant_loans_data()
BEGIN
    DECLARE v_base_db VARCHAR(255);
    DECLARE v_fms_db VARCHAR(255);
    DECLARE v_admin_db VARCHAR(255);
    DECLARE v_count INT DEFAULT 0;
    DECLARE v_row_count_view1 INT DEFAULT 0;
    DECLARE v_row_count_view2 INT DEFAULT 0;
    DECLARE sql_stmt TEXT;
    DECLARE v_start_time DATETIME(6);
    DECLARE v_end_time DATETIME(6);
    DECLARE v_step_duration INT;
    DECLARE v_step_name VARCHAR(255);
    DECLARE v_procedure_name VARCHAR(255) DEFAULT 'script_base_to_participant_loans_data';

    -- Example table for procedure execution logs
    CREATE TABLE IF NOT EXISTS procedure_execution_log (
       id INT AUTO_INCREMENT PRIMARY KEY,
       procedure_name VARCHAR(255),
       step_name VARCHAR(255),
       start_time DATETIME(6),
       end_time DATETIME(6),
       duration_in_mseconds INT,
       UNIQUE KEY unique_step_start_time (step_name, start_time)
    );

    -- Step 1: Start Logging
    SET v_step_name = 'Step 1: script_base_to_participant_loans_data';
    SET v_start_time = NOW(6);
    INSERT INTO procedure_execution_log (procedure_name, step_name, start_time) 
    VALUES (v_procedure_name, v_step_name, v_start_time);

    -- Get the current database name
    SET v_base_db = (SELECT DATABASE());
    SET v_fms_db = CONCAT(v_base_db, '_multiloan');

    -- Get admin database name
    SELECT SCHEMA_NAME INTO v_admin_db 
    FROM INFORMATION_SCHEMA.SCHEMATA 
    WHERE SCHEMA_NAME LIKE '%admin' 
    LIMIT 1;

    -- Check if ParticipationModule feature is enabled
    SET @v_count = 0;
    SET @query = CONCAT(
        'SELECT COUNT(1) INTO @v_count 
         FROM ', v_admin_db, '.cync_features cf 
         LEFT JOIN ', v_base_db, '.abl_features_subscriptions als 
         ON cf.name = als.feature_name 
         WHERE cf.name = ''ParticipationModule'' 
         AND cf.lender_wise_enabled = 1  
         AND als.status = ''ON'' 
         LIMIT 1'
    );

    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Assign the result to v_count
    SET v_count = @v_count;

    -- If feature is not enabled, log error and exit
    IF v_count = 0 THEN
        SET v_end_time = NOW(6);
        SET v_step_duration = TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time);
        UPDATE procedure_execution_log
        SET end_time = v_end_time, duration_in_mseconds = v_step_duration, step_name = 'ERROR: ParticipationModule is OFF, cannot create views.'
        WHERE start_time = v_start_time;

        SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT = 'ERROR: ParticipationModule is OFF, cannot create views.';
    END IF;

    -- Drop and create views
    SET @sql_stmt = 'DROP VIEW IF EXISTS view_participant_loans;';
    PREPARE stmt FROM @sql_stmt;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Generate and execute SQL for view_participant_loans
    SET @sql_stmt = CONCAT(
        'CREATE VIEW view_participant_loans AS
        SELECT bl.loan_no, bl.loan_name, bl.id AS original_pk, ''ABL'' AS loan_type,
            0 AS loan_amount, bl.borrower_id, 0 AS principal_bal, bl.credit_line_amt AS credit_line,
            0 AS outstanding_loan_bal, 0 AS rate_adjustment, bp.interest_rate_code_id,
            bp.max_interest_rate AS max_interest_rt, bp.min_interest_rate AS min_interest_rt,
            bl.loan_origination_dt AS effective_date, ''system'' AS created_by, NOW() AS created_at,
            ''system'' AS updated_by, NOW() AS updated_at, ''LoanNumber'' AS cync_model_type
        FROM ', v_base_db, '.base_loan_numbers bl
        JOIN ', v_base_db, '.base_parameters bp ON bp.borrower_id = bl.borrower_id
        UNION ALL
        SELECT ml.loan_no, ml.loan_name, ml.id AS original_pk, ''MCL'' AS loan_type,
            0 AS loan_amount, ml.borrower_id, ml.current_balance_amt AS principal_bal, ml.credit_line_amt AS credit_line,
            0 AS outstanding_loan_bal, 0 AS rate_adjustment, ml.interest_rate_code_id,
            ml.max_interest_rt, ml.min_interest_rt, ml.loan_origination_dt AS effective_date,
            ''system'' AS created_by, NOW() AS created_at, ''system'' AS updated_by, NOW() AS updated_at, ''LoanNumber'' AS cync_model_type
        FROM ', v_fms_db, '.multi_loan_loan_numbers ml;'
    );

    PREPARE stmt FROM @sql_stmt;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Drop and create view_participant_loans_not_exist_data
    SET @sql_stmt = 'DROP VIEW IF EXISTS view_participant_loans_not_exist_data;';
    PREPARE stmt FROM @sql_stmt;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @sql_stmt = CONCAT(
        'CREATE VIEW view_participant_loans_not_exist_data AS
        SELECT vpl.* 
        FROM ', v_base_db, '.participant_loans vpl
        WHERE vpl.loan_type IN (''ABL'', ''MCL'')  
        AND NOT EXISTS (
            SELECT 1 
            FROM ', v_base_db, '.view_participant_loans pl
            WHERE vpl.original_pk = pl.original_pk
            AND vpl.loan_type = pl.loan_type
            AND pl.loan_type <> ''TERMLOAN''
        );'
    );

    PREPARE stmt FROM @sql_stmt;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Get row counts
    SELECT COUNT(*) INTO v_row_count_view1 FROM view_participant_loans;
    SELECT COUNT(*) INTO v_row_count_view2 FROM view_participant_loans_not_exist_data;

    -- Log completion
    SET v_end_time = NOW(6);
    SET v_step_duration = TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time);
    UPDATE procedure_execution_log
    SET end_time = v_end_time, duration_in_mseconds = v_step_duration, step_name = CONCAT('Rows: ', v_row_count_view1, ', ', v_row_count_view2)
    WHERE start_time = v_start_time;

    -- Return row counts
    SELECT v_row_count_view1 AS view_participant_loans_count, v_row_count_view2 AS view_participant_loans_not_exist_data_count;
END ;;
DELIMITER ;
