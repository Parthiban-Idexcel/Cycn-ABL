DROP PROCEDURE IF EXISTS participant_loan_allocation_grid_proc;
DELIMITER ;;
CREATE PROCEDURE participant_loan_allocation_grid_proc(
p_loan_id int(11), -- Input parameter: Loan ID
p_loanDeal varchar(100), -- Input parameter: Loan Deal identifier
p_participant_name varchar(50)  -- Input parameter: Participant name
)
proc_label:BEGIN

DECLARE v_start_time DATETIME(6);                                            -- Variables for step logging
DECLARE v_end_time DATETIME(6);
DECLARE v_step_duration INT;
DECLARE v_step_name VARCHAR(255);
DECLARE v_procedure_name VARCHAR(255) DEFAULT 'participant_loan_allocation_grid_proc';

-- Drop the temporary table if it exists
DROP TEMPORARY TABLE IF EXISTS temp_loan_allocation_grid_details;
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

-- Step 1 interest_rate
    SET v_end_time = NOW(6);
    SET v_step_duration = (SELECT TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time));
    UPDATE procedure_execution_log
    SET end_time = v_end_time, duration_in_mseconds = v_step_duration
    WHERE step_name = v_step_name AND start_time = v_start_time;

 -- Step 1: temp_loan_allocation_grid_details
    SET v_step_name = 'Step 1: temp_loan_allocation_grid_details';
    SET v_start_time = NOW(6);
    INSERT INTO procedure_execution_log (procedure_name,step_name, start_time) VALUES (v_procedure_name,v_step_name, v_start_time);

-- Create a temporary table to store the result set
CREATE TEMPORARY TABLE temp_loan_allocation_grid_details
SELECT *
FROM
  (-- First part of UNION: Fetch details for primary participant loans
          SELECT "" id,                                                             -- Placeholder for ID
          "" AS allocationNumber,                                               -- Placeholder for allocation number
          IF (al.name IS NULL,p_participant_name,al.name) AS participantName,   -- Use affiliate lender name or participant name
             null as participant_code,                                          -- Placeholder for participant code
             null as loanallocationmethod,                                      -- Placeholder for loan allocation method
             NULL AS participationPercentage,                                   -- Placeholder for participation percentage
             NULL AS participation_priority,                                    -- Placeholder for participation priority
             pls.role_loan_deal AS participantRole,                             -- Role in loan deal
             COALESCE(mls.disbursement, 0) AS disbursementAmount,                                                                       -- Total disbursement amount
             COALESCE(mls.disbursement, 0) - COALESCE(mls.principal_paid, 0) AS disbursementBalance, -- Remaining disbursement balance
             COALESCE(mls.principal_balance, 0) AS participationPrincipalBalance,                      -- Principal balance
             irc.rate_code AS rateCode,                                                                -- Interest rate code
             COALESCE(pl.rate_adjustment, 0) AS rateAdjustment,                 -- Rate adjustment
             pl.min_interest_rt AS minInterest,                                 -- Minimum interest rate
             pl.max_interest_rt AS maxInterest,                                 -- Maximum interest rate
             pl.credit_line AS loan_limit,                                      -- Loan limit
             mls.interest_pct AS interestRateCode,                               -- Interest rate percentage
             "" effective_date_loan_participation,                               -- Placeholder for effective date
             pl.id loan_id,                                                      -- Loan ID
             ""is_closure,                                                       -- Placeholder for closure indicator
            ""loan_closure_date,                                                 -- Placeholder for closure date
            "" charges_accrue_till,
            "" AS commitment_amount,                                             -- Placeholder for commitment amount
             "p" AS relation_type,                                               -- Relation type for primary participant
             "" AS participant_party_id,                                         -- Placeholder for participant party ID
             0 AS copyChargeTemplateFromGlobalLoan,                                      -- Flags and configurations
             0 AS isFloatNotApply,
             0 AS copyInterestSettingsFromGlobalLoan,
             0 AS autoApplyDisbursement ,
             0 AS autoApplyDisbursementOption,
             0 AS isAutoApplyRepayments,
             0 AS isAutoApplySettlementDate,
             0 AS isAutoApplyPercentage ,
             0 AS isAutoApplyPriority,
            ' ' as isdelayfeesetupavailable,                                            -- Placeholder for delay fee setup availability
            ' ' as isagencyfeesetupavailable,
            0 as totalinterestrate
   FROM participant_loans pl
   JOIN base_multi_loan_summary mls ON pl.borrower_id = mls.borrower_id
   AND pl.original_pk = mls.loan_number_id
   AND mls.loan_type = (CASE
                            WHEN pl.loan_type = 'MCL' THEN 'NABL'
                            ELSE pl.loan_type
                        END)
   JOIN participant_borrowers pb ON pl.borrower_id = pb.id
   LEFT JOIN base_interest_rate_codes irc ON pl.interest_rate_code_id = irc.id
   INNER JOIN participant_loan_settings pls ON pl.id = pls.loan_id
   LEFT JOIN base_affiliate_lenders al ON al.id = pls.affiliate_lender
   WHERE pls.loan_deal = p_loanDeal
     AND pl.id = p_loan_id
     AND pls.loan_collaboration <> 'Syndication'
     AND EXISTS
       (SELECT 1
        FROM participant_loan_allocations pla
        WHERE pla.loan_id = pl.id )
   UNION SELECT pla.id,
                pla.allocation_number,
                pp.participant_name,
                pp.participant_code,
                case
                    when pls.loan_allocation_method ='percentage_allocation' then 'Percentage'
                    when pls.loan_allocation_method ='priority_allocation' then 'Priority'
                end loanallocationmethod,
                pla.participation_percentage,
                pla.participation_priority,
                pp.participant_role,
                disbursement_amount,
                disbursement_balance,
                participation_principal_bal,
                irc1.rate_code,
                pla.rate_adjustment,
                pla.min_interest_rt,
                pla.max_interest_rt,
                pla.participation_max_amount loan_limit,
                interestRateCode,
                pla.effective_date_loan_participation,
                pla.loan_id,
                pla.is_closure,
                pla.loan_closure_date,
                                pla.charges_accrue_till,
                pp.commitment_amount commitmentAmount,
                "c" AS relation_type,                                                                                                                   -- Relation type for allocations
                pp.id AS participant_party_id,
                COALESCE(pla.charge_on_pro_rata_basis,0) AS copyChargeTemplateFromGlobalLoan,
                COALESCE(pla.is_float_not_apply,0) AS isFloatNotApply,
                COALESCE(pla.interest_on_pro_rata_basis,0) AS copyInterestSettingsFromGlobalLoan,
                COALESCE(pla.is_auto_apply_disbursement,0) AS autoApplyDisbursement ,
                case
                    when pla.auto_apply_disbursement_based_on_global_loan =1 then 'By Participant % On Global Loan Transaction Date'
                    when pla.auto_apply_disbursement_based_on_disbursement_schedule  =1 then 'By Disbursement Schedule '
                end autoApplyDisbursementOption,
                COALESCE(pla.is_auto_apply_repayments,0) AS isAutoApplyRepayments,
                if (COALESCE(pla.is_auto_apply_settlement_date,0)=1 ,'On Settlement Date','On Global Loan Transaction Date ') AS isAutoApplySettlementDate,
                COALESCE(is_auto_apply_percentage ,0)AS ByParticipationpercentage ,
                COALESCE(is_auto_apply_priority ,0)AS ByParticipationPriority,
                coalesce(pla.is_delay_fee_setup_available,0) as isdelayfeesetupavailable,
                coalesce(pla.is_agency_fee_setup_available,0) as isagencyfeesetupavailable,
                ir.totalinterestrate
   FROM participant_loan_allocations pla
   JOIN participant_loans ON pla.loan_id = participant_loans.id
   JOIN participant_parties pp ON pla.participation_party_id = pp.id
   LEFT JOIN interest_rate ir ON ir.participant_party_id=pp.id
             AND ir.participant_loan_allocation_id=pla.id
   LEFT JOIN base_interest_rate_codes irc1 ON pla.interest_rate_code_id = irc1.id
   LEFT JOIN
     (SELECT plsum.participant_loan_allocation_id,
             plsum.disbursement AS disbursement_amount,
             plsum.disbursement - plsum.principal_paid AS disbursement_balance,
             plsum.closing_balance AS participation_principal_bal,
             plsum.interest_pct AS interestRateCode
      FROM participant_loan_summaries plsum) splsum ON splsum.participant_loan_allocation_id = pla.id
   JOIN participant_loan_settings pls ON pla.loan_id = pls.loan_id
   WHERE pls.loan_deal = p_loanDeal
     AND pla.loan_id = p_loan_id) grp
ORDER BY relation_type DESC;

                -- Retrieve data from the temporary table
        select * from temp_loan_allocation_grid_details;

-- Step 1 temp_loan_allocation_grid_details
    SET v_end_time = NOW(6);
    SET v_step_duration = (SELECT TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time));

    UPDATE procedure_execution_log
    SET end_time = v_end_time, duration_in_mseconds = v_step_duration
    WHERE step_name = v_step_name AND start_time = v_start_time;

END ;;

DELIMITER ;