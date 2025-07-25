DROP PROCEDURE IF EXISTS script_base_to_participant_loans_data;
DELIMITER $$

CREATE PROCEDURE script_base_to_participant_loans_data()
BEGIN
    DECLARE v_base_db VARCHAR(255);
    DECLARE v_mcl_db VARCHAR(255);
    DECLARE v_admin_db VARCHAR(255);
    DECLARE v_count INT DEFAULT 0;
    DECLARE v_row_count INT DEFAULT 0;
    DECLARE v_start_time DATETIME(6);
    DECLARE v_end_time DATETIME(6);
    DECLARE v_step_duration INT;
    DECLARE v_step_name VARCHAR(255);
    DECLARE v_procedure_name VARCHAR(255) DEFAULT 'script_base_to_participant_loans_data';

    -- Initialize Start Time
    SET v_start_time = NOW(6);
    SET v_step_name = 'Step 1: script_base_to_participant_loans_data';
    INSERT INTO procedure_execution_log (procedure_name, step_name, start_time) 
    VALUES (v_procedure_name, v_step_name, v_start_time);

    -- Get the current database name
    SET v_base_db = (SELECT DATABASE());
    SET v_mcl_db = CONCAT(v_base_db, '_multiloan');

    -- Get admin database dynamically
    SET @sql_admin = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE '%admin' LIMIT 1";
    PREPARE stmt_admin FROM @sql_admin;
    EXECUTE stmt_admin;
    DEALLOCATE PREPARE stmt_admin;

    -- Assign admin DB name correctly
    SET v_admin_db = (SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE '%admin' LIMIT 1);

    -- Check if ParticipationModule feature is enabled
    SET @query = CONCAT(
        'SELECT COUNT(1) 
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

    -- Retrieve value from session variable
    SET v_count = (SELECT @v_count);

    -- Exit if feature is not enabled
    IF v_count = 0 THEN
        SET v_end_time = NOW(6);
        SET v_step_duration = TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time);
        
        UPDATE procedure_execution_log
        SET end_time = v_end_time, duration_in_mseconds = v_step_duration, step_name = 'ERROR: ParticipationModule feature not enabled'
        WHERE start_time = v_start_time;

        SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT = 'ERROR: ParticipationModule feature not enabled, cannot insert data into participant_loans TABLE.';
    END IF;

    -- Dynamic SQL to insert into participant_loans
    SET @sql_stmt = CONCAT(
        'INSERT INTO participant_loans (loan_no, loan_name, original_pk, loan_type, 
            loan_amount, borrower_id, principal_bal, credit_line, outstanding_loan_bal, 
            rate_adjustment, interest_rate_code_id, max_interest_rt, min_interest_rt, 
            effective_date, created_by, created_at, updated_by, updated_at, cync_model_type)
        
        SELECT * FROM (
            SELECT bl.loan_no, bl.loan_name, bl.id AS original_pk, ''ABL'' AS loan_type,
                0 AS loan_amount, bl.borrower_id, 0 AS principal_bal, bl.credit_line_amt AS credit_line,
                0 AS outstanding_loan_bal, 0 AS rate_adjustment, bp.interest_rate_code_id,
                bp.max_interest_rate AS max_interest_rt, bp.min_interest_rate AS min_interest_rt,
                bl.loan_origination_dt AS effective_date, ''system'' AS created_by, NOW() AS created_at,
                ''system'' AS updated_by, NOW() AS updated_at, ''LoanNumber'' AS cync_model_type
            FROM ', v_base_db, '.base_loan_numbers bl
            JOIN ', v_base_db, '.base_parameters bp ON bp.borrower_id = bl.borrower_id
            WHERE NOT EXISTS (
                SELECT 1 FROM participant_loans pl 
                WHERE pl.loan_no = bl.loan_no AND pl.loan_type = ''ABL''
            )
            
            UNION ALL
            
            SELECT ml.loan_no, ml.loan_name, ml.id AS original_pk, ''MCL'' AS loan_type,
                0 AS loan_amount, ml.borrower_id, ml.current_balance_amt AS principal_bal, ml.credit_line_amt AS credit_line,
                0 AS outstanding_loan_bal, 0 AS rate_adjustment, ml.interest_rate_code_id,
                ml.max_interest_rt, ml.min_interest_rt, ml.loan_origination_dt AS effective_date,
                ''system'' AS created_by, NOW() AS created_at, ''system'' AS updated_by, NOW() AS updated_at, ''LoanNumber'' AS cync_model_type
            FROM ', v_mcl_db, '.multi_loan_loan_numbers ml
            WHERE NOT EXISTS (
                SELECT 1 FROM participant_loans pl 
                WHERE pl.loan_no = ml.loan_no AND pl.loan_type = ''MCL''
            )
        ) AS new_data;'
    );

    PREPARE stmt FROM @sql_stmt;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Get inserted row count dynamically
    SET @sql_count = 'SELECT COUNT(*) FROM participant_loans';
    PREPARE stmt_count FROM @sql_count;
    EXECUTE stmt_count;
    DEALLOCATE PREPARE stmt_count;

    -- Assign the result to v_row_count
    SET v_row_count = (SELECT @row_count);

    -- Log completion
    SET v_end_time = NOW(6);
    SET v_step_duration = TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time);

    UPDATE procedure_execution_log
    SET end_time = v_end_time, duration_in_mseconds = v_step_duration, step_name = CONCAT('Rows Inserted: ', v_row_count)
    WHERE start_time = v_start_time;

END $$

DELIMITER ;
