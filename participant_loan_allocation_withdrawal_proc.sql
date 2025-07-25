-- Drop the procedure if it already exists to avoid duplication errors.
DROP PROCEDURE IF EXISTS participant_loan_allocation_withdrawal_proc;
DELIMITER ;;
CREATE PROCEDURE participant_loan_allocation_withdrawal_proc(
p_loan_id int(11),  -- Input parameter: Loan ID
p_loanDeal varchar(100), -- Input parameter: Loan deal identifier
p_participant_party_id int(11) -- Input parameter: Participant party ID
)
proc_label:BEGIN

DECLARE v_start_time DATETIME(6);					     -- Variables for step logging
DECLARE v_end_time DATETIME(6);
DECLARE v_step_duration INT;
DECLARE v_step_name VARCHAR(255);
DECLARE v_procedure_name VARCHAR(255) DEFAULT 'participant_loan_allocation_withdrawal_proc';

-- Drop the temporary table if it already exists to avoid conflicts.
DROP TEMPORARY TABLE IF EXISTS participant_allocation_transaction;

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
	  
 -- Step 1: participant_allocation_transaction 
    SET v_step_name = 'Step 1: participant_allocation_transaction';
    SET v_start_time = NOW(6);
    INSERT INTO procedure_execution_log (procedure_name,step_name, start_time) VALUES (v_procedure_name,v_step_name, v_start_time);
	

-- Create a temporary table to store participant loan allocation transaction details
CREATE TEMPORARY TABLE participant_allocation_transaction
SELECT pla.id,															-- Participant loan allocation ID
                pla.allocation_number as allocationNumber,									-- Allocation number
                pp.participant_name as participantName,										-- Participant name
		pp.participant_code,												-- Participant code
                IF(pls.loan_allocation_method = 'percentage_allocation', 'Percentage', 'Priority') AS loanallocationmethod,		 -- Loan allocation method (Percentage or Priority)
                pla.participation_percentage as participationPercentage,					 -- Participation percentage
                pla.participation_priority,													 -- Participation priority
                pp.participant_role as participantRole,										 -- Participant role
                COALESCE(pl_sum.disbursement,0) as disbursementamount,							 -- Disbursement amount
                (coalesce(pl_sum.disbursement,0) - coalesce(pl_sum.principal_paid,0)) as disbursementbalance,							 -- Disbursement balance
                coalesce(pl_sum.closing_balance,0) as participationPrincipalbalance,		 -- Principal balance
                irc1.rate_code as rateCode,													 -- Interest rate code
                pla.rate_adjustment as rateAdjustment,										 -- Rate adjustment
                pla.min_interest_rt as minInterest,											 -- Minimum interest rate
                pla.max_interest_rt as maxInterest,											 -- Maximum interest rate
                pla.participation_max_amount loan_limit,									 -- Loan limit
                pl_sum.interest_pct as interestRateCode,									 -- Interest rate code from summaries
                pla.effective_date_loan_participation,										 -- Effective date of loan participation
                pla.loan_id,													 -- Loan ID
                pla.is_closure,													 -- Indicates if the allocation is closed
                pla.loan_closure_date,												 -- Loan closure date 
                pp.commitment_amount commitment_amount,										 -- Commitment amount
                "c" AS relation_type,														 -- Relation type (constant)
                pp.id AS participant_party_id												 -- Participant party ID	
   FROM participant_loan_allocations pla													 -- Main table: Participant loan allocations
   JOIN participant_loans ON pla.loan_id = participant_loans.id and pla.is_closure=0		 -- Filter: Only non-closed allocations
   JOIN participant_parties pp ON pla.participation_party_id = pp.id 						 -- Join on participant parties
   LEFT JOIN base_interest_rate_codes irc1 ON pla.interest_rate_code_id = irc1.id			 -- Join on interest rate codes
   LEFT JOIN participant_loan_summaries pl_sum ON pla.id = pl_sum.participant_loan_allocation_id		-- Subquery to fetch loan summaries
   JOIN participant_loan_settings pls ON pla.loan_id = pls.loan_id							 -- Join on loan settings
   WHERE pls.loan_deal = p_loanDeal															 -- Filter: Loan deal matches input
     AND pla.loan_id = p_loan_id															 -- Filter: Loan ID matches input
         AND pla.participation_party_id=p_participant_party_id								 -- Filter: Participant party matches input
         AND EXISTS  (-- Subquery to check if participant has related transactions
		 select 1 from participant_transaction_details ptd where ptd.participant_party_id=pla.participation_party_id and ptd.participant_loan_allocation_id=pla.id LIMIT 1 );	
		-- Output the contents of the temporary table for review or further processing.
        select * from participant_allocation_transaction;
		
		-- Step 1 participant_allocation_transaction
    SET v_end_time = NOW(6);
    SET v_step_duration = (SELECT TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time));
    UPDATE procedure_execution_log
    SET end_time = v_end_time, duration_in_mseconds = v_step_duration
    WHERE step_name = v_step_name AND start_time = v_start_time;

END ;;

DELIMITER ;

