DROP PROCEDURE IF EXISTS participant_loan_allocation_closure_proc;
DELIMITER ;;
CREATE PROCEDURE participant_loan_allocation_closure_proc(p_allocation_id int(11))
BEGIN

DECLARE v_start_time DATETIME(6);					     -- Variables for step logging
DECLARE v_end_time DATETIME(6);
DECLARE v_step_duration INT;
DECLARE v_step_name VARCHAR(255);
DECLARE v_procedure_name VARCHAR(255) DEFAULT 'participant_loan_allocation_closure_proc';

-- Example table for procedure execution logs
	CREATE TABLE IF NOT EXISTS procedure_execution_log (
       id INT AUTO_INCREMENT PRIMARY KEY,           -- Unique identifier for each log entry.
       procedure_name VARCHAR(255),                 -- Name of the procedure being logged.
       step_name VARCHAR(255),                      -- Description of the current step in the procedure.
       start_time DATETIME(6),                         -- Start time of the step.
       end_time DATETIME(6),                           -- End time of the step.
       duration_in_mseconds INT,                     -- Duration of the step in seconds.
       UNIQUE KEY unique_step_start_time (STEP_NAME, START_TIME)
      );
	  
 -- Step 1: participant_loan_allocation_closure_proc 
    SET v_step_name = 'Step 1: participant_loan_allocation_closure_proc';
    SET v_start_time = NOW(6);
    INSERT INTO procedure_execution_log (procedure_name,step_name, start_time) VALUES (v_procedure_name,v_step_name, v_start_time);




SELECT
    pla.id ,
    pla.allocation_number,
    pp.id AS participantid,
    pp.participant_name,
    pp.participant_code,
    COALESCE(pls.outstanding_balance, 0) AS outstanding_balance,
    COALESCE(pls.closing_balance, 0) AS Principalbalance,
    COALESCE(pls.interest_balance, 0) AS interestbalance,
    COALESCE(pls.fee_balance, 0) AS feebalance,
    (
        SELECT ptd.activity_date
        FROM participant_transaction_details ptd
        JOIN participant_charge_templates pct ON ptd.participant_charge_template_id = pct.id
        WHERE pct.source_type = 'advance'
          AND ptd.participant_loan_allocation_id = pla.id
          AND ptd.participant_party_id = pp.id
        ORDER BY ptd.activity_date DESC
        LIMIT 1
    ) AS Last_Disbursement_date,
    DATEDIFF(CURRENT_DATE, COALESCE((
        SELECT ptd.activity_date
        FROM participant_transaction_details ptd
        JOIN participant_charge_templates pct ON ptd.participant_charge_template_id = pct.id
        WHERE pct.source_type = 'advance'
          AND ptd.participant_loan_allocation_id = pla.id
          AND ptd.participant_party_id = pp.id
        ORDER BY ptd.activity_date DESC
        LIMIT 1
    ), CURRENT_DATE)) AS disbursement_pending_days,
    (
        SELECT ptd.charge_amount
        FROM participant_transaction_details ptd
        JOIN participant_charge_templates pct ON ptd.participant_charge_template_id = pct.id
        WHERE pct.source_type = 'advance'
          AND ptd.participant_loan_allocation_id = pla.id
          AND ptd.participant_party_id = pp.id
        ORDER BY ptd.activity_date DESC
        LIMIT 1
    ) AS disbursement_amount,
    COALESCE(pls.disbursement - pls.principal_paid, 0) AS disbursement_principal_balance,
    COALESCE(dtdh.net_exp_amount, 0) AS net_exp_amount,
    dtdh.net_settlement_amount_date AS Settlement_date,
    dtdh.net_settlemt_amount_today AS net_Settlement_amount_today,
    CASE
        WHEN COALESCE(dtdh.net_exp_amount, 0) > 0 THEN dtdh.net_exp_amount
        ELSE 0
    END AS expected_contribution,
    ROUND(
        CASE
            WHEN COALESCE(dtdh.net_exp_amount, 0) > 0
            THEN (pls.disbursement - pls.principal_paid) / dtdh.net_exp_amount
            ELSE 0
        END,
        5
    ) AS disbursement_principal_balance_percentage,
    ROUND(
        CASE
            WHEN COALESCE(dtdh.net_exp_amount, 0) > 0
            THEN ((dtdh.net_exp_amount - (pls.disbursement - pls.principal_paid)) * 100) / dtdh.net_exp_amount
            ELSE 0
        END,
        5
    ) AS disbursement_shortage
FROM participant_loan_allocations pla
JOIN participant_parties pp ON pla.participation_party_id = pp.id
LEFT JOIN net_settlement_amount_daily_transaction_details_history dtdh
    ON dtdh.participant_party_id = pp.id
    AND dtdh.participant_loan_allocation_id = pla.id
    AND dtdh.Transaction_date = CURRENT_DATE
LEFT JOIN participant_loan_summaries pls
    ON pls.participant_loan_allocation_id = pla.id
    AND pls.participant_party_id = pp.id
WHERE pla.id = p_allocation_id;

/*
WITH RankedTransactions AS (
    SELECT 
        ptd_inner.participant_loan_allocation_id,
        ptd_inner.participant_party_id,
        ptd_inner.participant_charge_template_id,
        ptd_inner.activity_date,
	ptd_inner.charge_amount,
        ROW_NUMBER() OVER (
            PARTITION BY ptd_inner.participant_loan_allocation_id, 
                         ptd_inner.participant_charge_template_id, 
                         ptd_inner.participant_party_id 
            ORDER BY ptd_inner.activity_date DESC
        ) AS rnk
    FROM participant_transaction_details ptd_inner
    JOIN participant_charge_templates pct_inner 
        ON ptd_inner.participant_charge_template_id = pct_inner.id
    WHERE pct_inner.source_type = 'advance'
)
SELECT 
    pla.id,
    pla.allocation_number,
    pp.id AS participantid,
    pp.participant_name,
    pp.participant_code,
    COALESCE(pl_sum.outstanding_balance, 0) AS outstanding_balance,
    COALESCE(pl_sum.closing_balance, 0) AS Principalbalance,
    COALESCE(pl_sum.interest_balance, 0) AS interestbalance,
    COALESCE(pl_sum.fee_balance, 0) AS feebalance,
    COALESCE(ptd.activity_date, ' ') AS Last_Disbursement_date,
    COALESCE(DATEDIFF(CURRENT_DATE, ptd.activity_date), 0) AS disbursement_pending_days,
    COALESCE(ptd.charge_amount, 0) AS disbursement_amount,
    COALESCE(pl_sum.disbursement - pl_sum.principal_paid, 0) AS disbursement_principal_balance,
    COALESCE(dtdh.net_exp_amount, 0) AS net_exp_amount,
    dtdh.net_settlement_amount_date AS Settlement_date,
    dtdh.net_settlemt_amount_today AS net_Settlement_amount_today,
    IF(COALESCE(dtdh.net_exp_amount, 0) <= 0, 0, dtdh.net_exp_amount) AS expected_contribution,
    ROUND(COALESCE(pl_sum.disbursement - pl_sum.principal_paid, 0), 2) AS disbursement_principal_balance,
    ROUND(IF(COALESCE(dtdh.net_exp_amount, 0) <= 0, 0, 
        ((pl_sum.disbursement - pl_sum.principal_paid) / dtdh.net_exp_amount)), 5) AS disbursement_principal_balance_percentage,
    ROUND(IF(COALESCE(dtdh.net_exp_amount, 0) <= 0, 0, 
        ((dtdh.net_exp_amount - (pl_sum.disbursement - pl_sum.principal_paid)) * 100) / dtdh.net_exp_amount), 5) AS disbursement_shortage
FROM participant_loan_allocations pla
JOIN participant_parties pp ON pla.participation_party_id = pp.id
LEFT JOIN RankedTransactions ptd ON ptd.participant_loan_allocation_id = pla.id
    AND ptd.participant_party_id = pp.id
    AND ptd.rnk = 1 -- Ensuring only the latest transaction is selected
LEFT JOIN net_settlement_amount_daily_transaction_details_history dtdh 
    ON dtdh.participant_party_id = pp.id
    AND dtdh.participant_loan_allocation_id = pla.id
    AND dtdh.Transaction_date = CURRENT_DATE
LEFT JOIN participant_loan_summaries pl_sum 
    ON pla.id = pl_sum.participant_loan_allocation_id
    AND pp.id = pl_sum.participant_party_id
WHERE pla.id = p_allocation_id;
*/

--  Step 1 participant_loan_allocation_closure_proc
    SET v_end_time = NOW(6);
    SET v_step_duration = (SELECT TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time));
    UPDATE procedure_execution_log
    SET end_time = v_end_time, duration_in_mseconds = v_step_duration
    WHERE step_name = v_step_name AND start_time = v_start_time;
				 
END ;;
DELIMITER ;


