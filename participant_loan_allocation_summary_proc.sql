DROP PROCEDURE IF EXISTS participant_loan_allocation_summary_proc;
DELIMITER ;;
CREATE PROCEDURE participant_loan_allocation_summary_proc(
p_loanDeal varchar(100),  				-- Input parameter: Loan deal identifier(s)
p_defaultLender varchar(50), 			-- Input parameter: Default lender(s)
p_loanCollaborationType varchar(50),  	-- Input parameter: Collaboration type (syndication/participation)
p_is_closure varchar(10)  				-- Input parameter: Indicates closure status (ALL/True/False)
)
proc_label:BEGIN

-- Declare variables used for processing input strings
DECLARE p_value varchar(255); 			-- Temporary variable for processing p_loanDeal
DECLARE done INT DEFAULT 0; 			-- Loop control for p_loanDeal
DECLARE separator_position INT; 		-- Position of separator in p_loanDeal
DECLARE V_defaultLender VARCHAR(255); 	-- Temporary variable for processing p_defaultLender
DECLARE done_1 INT DEFAULT 0; 			-- Loop control for p_defaultLender
DECLARE separator_position_1 INT; 		-- Position of separator in p_defaultLender
DECLARE v_start_time DATETIME(6);					     -- Variables for step logging
DECLARE v_end_time DATETIME(6);
DECLARE v_step_duration INT;
DECLARE v_step_name VARCHAR(255);
DECLARE v_procedure_name VARCHAR(255) DEFAULT 'participant_loan_allocation_summary_proc';

-- Drop temporary tables if they exist to avoid conflicts
DROP TEMPORARY TABLE IF EXISTS temp_allocation_summary_details;
--  DROP TEMPORARY TABLE IF EXISTS temp_loandeal_info;
--  DROP TEMPORARY TABLE IF EXISTS temp_is_closure;
DROP TEMPORARY TABLE IF EXISTS interest_rate;

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
/*	  
 -- Step 1: temp_is_closure 
    SET v_step_name = 'Step 1: temp_is_closure';
    SET v_start_time = NOW(6);
    INSERT INTO procedure_execution_log (procedure_name,step_name, start_time) VALUES (v_procedure_name,v_step_name, v_start_time);
	
-- Create temp_is_closure table based on p_is_closure parameter
IF p_is_closure ='ALL' THEN
CREATE TEMPORARY TABLE temp_is_closure AS SELECT 0 is_closure UNION SELECT 1 is_closure;
ELSEIF p_is_closure='True' THEN
CREATE TEMPORARY TABLE temp_is_closure AS SELECT 1 is_closure;
ELSE
CREATE TEMPORARY TABLE temp_is_closure AS SELECT 0 is_closure;
END IF;

-- Step 1 temp_is_closure
    SET v_end_time = NOW(6);
    SET v_step_duration = (SELECT TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time));
    UPDATE procedure_execution_log
    SET end_time = v_end_time, duration_in_mseconds = v_step_duration
    WHERE step_name = v_step_name AND start_time = v_start_time;

  -- Step 1: temp_loandeal_info
    SET v_step_name = 'Step 1: temp_loandeal_info';
    SET v_start_time = NOW(6);
    INSERT INTO procedure_execution_log (procedure_name,step_name, start_time) VALUES (v_procedure_name,v_step_name, v_start_time);
	
-- Create temp_loandeal_info table and populate it by parsing p_loanDeal
   CREATE TEMPORARY TABLE temp_loandeal_info (loanDeal varchar(255));
  -- Loop through the  p_loanDeal and insert values into the temporary table
    WHILE NOT done DO
        SET separator_position = LOCATE(',', p_loanDeal);

        IF separator_position > 0 THEN
            SET p_value = SUBSTRING(p_loanDeal, 1, separator_position - 1);
            SET p_loanDeal= SUBSTRING(p_loanDeal, separator_position + 1);
        ELSE
            SET p_value = p_loanDeal;
            SET done = 1;
        END IF;

        INSERT INTO temp_loandeal_info (loanDeal) VALUES (p_value);
    END WHILE;

-- Step 2 temp_loandeal_info
    SET v_end_time = NOW(6);
    SET v_step_duration = (SELECT TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time));
    UPDATE procedure_execution_log
    SET end_time = v_end_time, duration_in_mseconds = v_step_duration
    WHERE step_name = v_step_name AND start_time = v_start_time;
*/

 -- Step 1: interest_rate
    SET v_step_name = 'Step 1: interest_rate';
    SET v_start_time = NOW(6);
    INSERT INTO procedure_execution_log (procedure_name,step_name, start_time) VALUES (v_procedure_name,v_step_name, v_start_time);
	
-- Create interest_rate table to calculate the interest rates based on certain conditions
CREATE TEMPORARY TABLE interest_rate as
SELECT participant_party_id,
       participant_loan_allocation_id,
       CASE
           WHEN (interest_pct BETWEEN min_interest_rt AND max_interest_rt)
                OR (min_interest_rt = 0
                    AND max_interest_rt = 0) THEN interest_pct
           WHEN interest_pct < min_interest_rt THEN min_interest_rt
           WHEN interest_pct > max_interest_rt THEN max_interest_rt
       END AS totalinterestrate
FROM
  (SELECT pla.participation_party_id AS participant_party_id,
          pla.id AS participant_loan_allocation_id,
          (COALESCE(ir.rate_value, 0) + COALESCE(ra.value, 0)) AS interest_pct,
          pla.min_interest_rt,
          pla.max_interest_rt
   FROM participant_loan_allocations pla
   LEFT JOIN
     (SELECT ir1.interest_rate_code_id,
             ir1.rate_value,
             RANK() OVER (PARTITION BY ir1.interest_rate_code_id
                          ORDER BY ir1.rate_date DESC) AS `rank`
      FROM base_interest_rates ir1
      WHERE ir1.rate_date <= CURRENT_DATE) ir ON ir.interest_rate_code_id = pla.interest_rate_code_id
   AND ir.`rank` = 1
   LEFT JOIN
     (SELECT ra1.participant_loan_allocation_id,
             ra1.value,
             RANK() OVER (PARTITION BY ra1.participant_loan_allocation_id
                          ORDER BY ra1.effective_date DESC) AS `rank`
      FROM participant_loan_rate_adjustments ra1
      WHERE ra1.effective_date <= CURRENT_DATE) ra ON ra.participant_loan_allocation_id = pla.id
   AND ra.`rank` = 1) grp
GROUP BY participant_party_id,
         participant_loan_allocation_id,
         totalinterestrate;
	
	-- Add an index to interest_rate table for optimized querying
	CREATE INDEX  ir_indx on interest_rate (participant_party_id,participant_loan_allocation_id);

-- Step 4 interest_rate
    SET v_end_time = NOW(6);
    SET v_step_duration = (SELECT TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time));
    UPDATE procedure_execution_log
    SET end_time = v_end_time, duration_in_mseconds = v_step_duration
    WHERE step_name = v_step_name AND start_time = v_start_time;

 -- Step 5: temp_allocation_summary_details
    SET v_step_name = 'Step 2: temp_allocation_summary_details';
    SET v_start_time = NOW(6);
    INSERT INTO procedure_execution_log (procedure_name,step_name, start_time) VALUES (v_procedure_name,v_step_name, v_start_time);
	
-- Create temp_allocation_summary_details table to summarize loan allocations
-- Summarized query combining details from multiple related tables
-- UNION used to combine participant-level and allocation-level data
-- Relevant calculations and conditional fields applied
CREATE TEMPORARY TABLE temp_allocation_summary_details
SELECT * 
 FROM(SELECT pl.id,
          pl.loan_name,
          ' ' AS allocation_number,
          pb.client_name,
	  pla.participation_party_id as participant_party_id,
          IF (al.name IS NULL,
                         p_defaultLender,
                         al.name) AS participation_party_name,
             NULL AS participant_code,
             NULL AS loan_allocation_method,
             NULL AS participant_percent,
             NULL AS participation_priority,
             COALESCE(mls.disbursement, 0) AS disbursement_amount,
             (COALESCE(mls.principal_paid, 0) + COALESCE(mls.fee_paid, 0) + COALESCE(mls.interest_paid, 0)) AS repayment,
             COALESCE(disbursement, 0) - COALESCE(principal_paid, 0) AS disbursement_balance,
             COALESCE(mls.outstanding_loan_balance, 0) AS outstanding_loan_bal,
             COALESCE(mls.principal_paid, 0) AS principal_paid,
             COALESCE(mls.principal_balance, 0) AS principal_bal,
             COALESCE(mls.fee_paid, 0) AS fee_paid,
             COALESCE(mls.fee_balance, 0) AS fee_balance,
             COALESCE(mls.interest_paid, 0) AS interest_paid,
             COALESCE(mls.interest_balance, 0) AS interest_balance,
             COALESCE(mls.interest_accrued, 0) AS interest_accrued,
             COALESCE(mls.fee_accrued, 0) AS fee_accrued,
             COALESCE(mls.other_fees, 0) AS other_fees,
             COALESCE(mls.interest_amount, 0) AS interest_amount,
             irc.rate_code,
             COALESCE(pl.rate_adjustment, 0) AS rate_adjustment,
             pl.credit_line loan_limit,
             pl.min_interest_rt,
             pl.max_interest_rt,
             pl.id AS correlation_column,
             '' over_advance_on,
                ''over_advance_percentage,
                  "p" AS relation_type,
                  ' ' loan_closure_date,
                      ' ' charges_accrue_till,
                          ' ' loan_closure_reason,
                              ' ' is_allow_new_loan_participant,
                                  ' ' closure_comments,
                                      IF (pla.is_closure = 0,
                                          'False',
                                          'True')is_closure,
                                         'False' AS is_delay_fee_setup_available,
                                         'False' AS is_agency_fee_setup_available,
                                         0 AS copyChargeTemplateFromGlobalLoan,
                                         0 AS isFloatNotApply,
                                         0 AS copyInterestSettingsFromGlobalLoan,
                                         0 AS autoApplyDisbursement,
                                         0 AS autoApplyDisbursementOption,
                                         0 AS isAutoApplyRepayments,
                                         0 AS isAutoApplySettlementDate,
                                         0 AS ByParticipationpercentage,
                                         0 AS ByParticipationPriority,
                                         0 AS totalinterestrate
   FROM participant_loans pl
   JOIN participant_loan_allocations pla ON pla.loan_id = pl.id
   JOIN base_multi_loan_summary mls ON pl.borrower_id = mls.borrower_id
   AND pl.original_pk = mls.loan_number_id
   AND mls.loan_type = (CASE
                            WHEN pl.loan_type = 'MCL' THEN 'NABL'
                            ELSE pl.loan_type
                        END)
   JOIN participant_borrowers pb ON pl.borrower_id = pb.id
   LEFT JOIN base_interest_rate_codes irc ON pl.interest_rate_code_id = irc.id
   INNER JOIN participant_loan_settings pls ON pl.id = pls.loan_id
   AND (-- Include both 'syndication' and 'participation' for syndication type
 (p_loanCollaborationType = 'syndication'
  AND pls.loan_collaboration NOT IN ('syndication',
                                     'participation'))-- Include only participation type

        OR (p_loanCollaborationType = 'participation'
            AND pls.loan_collaboration = 'participation'))
   LEFT JOIN base_affiliate_lenders al ON al.id = pls.affiliate_lender
   WHERE pls.loan_deal = p_loanDeal
   UNION  
	SELECT pla.id,
                pl.loan_name,
                pla.allocation_number,
                pb.client_name,
		pp.id as participant_party_id,
                pp.participant_name,
                pp.participant_code,
                pls.loan_allocation_method,
                pla.participation_percentage,
                pla.participation_priority,
                COALESCE(pl_sum.disbursement,0) as disbursement_amount,
                (coalesce(pl_sum.principal_paid,0) + coalesce(pl_sum.fee_paid,0) + coalesce(pl_sum.interest_paid,0) ) as repayment,
                (coalesce(pl_sum.disbursement,0) - coalesce(pl_sum.principal_paid,0)) as  disbursement_balance,
                coalesce(pl_sum.outstanding_balance,0) outstanding_loan_bal,
                coalesce(pl_sum.principal_paid,0) principal_paid,
                coalesce(pl_sum.closing_balance,0) as principal_bal,
                coalesce(pl_sum.fee_paid,0)as fee_paid,
                coalesce(pl_sum.fee_balance,0) as fee_balance,
                coalesce(pl_sum.interest_paid,0) as interest_paid,
                coalesce(pl_sum.interest_balance,0) as interest_balance,
                coalesce(pl_sum.interest_accrued,0) as interest_accrued,
                coalesce(pl_sum.fee_accrued,0) as fee_accrued,
                coalesce(pl_sum.other_fees,0) as other_fees,
                coalesce(pl_sum.interest_amount,0) as interest_amount,
                irc1.rate_code,
                pla.rate_adjustment,
                pla.participation_max_amount loan_limit,
                pla.min_interest_rt,
                pla.max_interest_rt,
                pla.loan_id,
                CASE
                    WHEN pla.over_advance_on='gl_credit_line_amount' THEN 'Global Loan Credit Line Amount'
                    WHEN pla.over_advance_on='net_bb_availability' THEN 'Approved Net BB Availability'
                    WHEN pla.over_advance_on='gross_bb_availability' THEN 'Approved Gross BB Availability'
                    WHEN pla.over_advance_on='gl_disbursement_amount' THEN 'Global Loan Disbursement Amount'
                END over_advance_on,
                pla.over_advance_percentage,
                "C" AS relation_type,
                pla.loan_closure_date,
                pla.charges_accrue_till,
                pla.loan_closure_reason,
                pla.is_allow_new_loan_participant,
                pla.closure_comments,
                IF (pla.is_closure = 0,
                    'False',
                    'True')is_closure,
                   IF (pla.is_delay_fee_setup_available=0,
                       'False',
                       'True')AS is_delay_fee_setup_available,
                      IF (pla.is_agency_fee_setup_available=0,
                          'False',
                          'True')AS is_agency_fee_setup_available,
                         COALESCE(pla.charge_on_pro_rata_basis, 0) AS copyChargeTemplateFromGlobalLoan,
                         COALESCE(pla.is_float_not_apply, 0) AS isFloatNotApply,
                         COALESCE(pla.interest_on_pro_rata_basis, 0) AS copyInterestSettingsFromGlobalLoan,
                         COALESCE(pla.is_auto_apply_disbursement, 0) AS autoApplyDisbursement,
                         CASE
                             WHEN pla.auto_apply_disbursement_based_on_global_loan =1 THEN 'By Participant % On Global Loan Transaction Date'
                             WHEN pla.auto_apply_disbursement_based_on_disbursement_schedule =1 THEN 'By Disbursement Schedule '
                         END autoApplyDisbursementOption,
                         COALESCE(pla.is_auto_apply_repayments, 0) AS isAutoApplyRepayments,
                         IF (COALESCE(pla.is_auto_apply_settlement_date, 0)=1,
                             'On Settlement Date',
                             'On Global Loan Transaction Date ') AS isAutoApplySettlementDate,
                            COALESCE(is_auto_apply_percentage, 0)AS ByParticipationpercentage,
                            COALESCE(is_auto_apply_priority, 0)AS ByParticipationPriority,
                            ir.totalinterestrate
   FROM participant_loan_allocations pla
   JOIN participant_loans pl ON pla.loan_id = pl.id
   JOIN participant_parties pp ON pla.participation_party_id = pp.id
   JOIN participant_borrowers pb ON pl.borrower_id = pb.id
   LEFT JOIN interest_rate ir ON ir.participant_party_id=pp.id
   AND ir.participant_loan_allocation_id=pla.id
   LEFT JOIN base_interest_rate_codes irc1 ON pla.interest_rate_code_id = irc1.id
   LEFT JOIN participant_loan_summaries pl_sum on pla.id = pl_sum.participant_loan_allocation_id 
   JOIN participant_loan_settings pls ON pla.loan_id = pls.loan_id
   AND loan_collaboration = p_loanCollaborationType
   WHERE pls.loan_deal = p_loanDeal)grp
WHERE (p_is_closure = 'All'
       OR (p_is_closure = 'True'
           AND is_closure = 'True')
       OR (p_is_closure = 'False'
           AND is_closure = 'False'))
ORDER BY correlation_column,
         relation_type DESC;


     select * from temp_allocation_summary_details;
		
-- Step 3 temp_allocation_summary_details
    SET v_end_time = NOW(6);
    SET v_step_duration = (SELECT TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time));
    UPDATE procedure_execution_log
    SET end_time = v_end_time, duration_in_mseconds = v_step_duration
    WHERE step_name = v_step_name AND start_time = v_start_time;
	
END ;;

DELIMITER ;



