DROP PROCEDURE IF EXISTS participant_summary_proc;
DELIMITER ;;
-- Create the stored procedure with input parameters for participant ID and loan collaboration type.
CREATE PROCEDURE participant_summary_proc(
p_participant_id text,
p_loanCollaborationType varchar(255)
)
proc_label:BEGIN

-- Declare variables for use within the procedure.
DECLARE p_value int(11);							-- Holds individual participant IDs during parsing.
DECLARE done INT DEFAULT 0;							-- Loop control variable for participant ID parsing.
DECLARE separator_position INT;						-- Tracks the position of the separator in the participant ID string.
DECLARE V_loanCollaborationType VARCHAR(255);		-- Holds individual loan collaboration types during parsing.
DECLARE done_1 INT DEFAULT 0;						-- Loop control variable for loan collaboration type parsing.
DECLARE separator_position_1 INT;					-- Tracks the position of the separator in the loan collaboration type string.
DECLARE v_count int(11);							-- Tracks the count of specific loan collaboration types.
DECLARE v_start_time DATETIME(6);					     -- Variables for step logging
DECLARE v_end_time DATETIME(6);
DECLARE v_step_duration INT;
DECLARE v_step_name VARCHAR(255);
DECLARE v_procedure_name VARCHAR(255) DEFAULT 'participant_summary_proc';
	
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
	
-- Drop temporary tables if they exist to prevent conflicts.
DROP TEMPORARY TABLE IF EXISTS temp_participation_details;
DROP TEMPORARY TABLE IF EXISTS temp_participation_loan_summary_info;
DROP TEMPORARY TABLE IF EXISTS temp_participation_count_summary_info;
DROP TEMPORARY TABLE IF EXISTS temp_participation_user_details;
DROP TEMPORARY TABLE IF EXISTS temp_participant_ids;
DROP TEMPORARY TABLE IF EXISTS temp_loanCollaboration_Type;
DROP TEMPORARY TABLE IF EXISTS temp_participant_role;
DROP TEMPORARY TABLE IF EXISTS temp_net_settlement;

 -- Step 1: Initialize Step Logging
    SET v_step_name = 'Step 1: Initialize';
    SET v_start_time = NOW(6);
    INSERT INTO procedure_execution_log (procedure_name,step_name, start_time) VALUES (v_procedure_name,v_step_name, v_start_time);
	
	-- Create a temporary table to store parsed participant IDs.
    CREATE TEMPORARY TABLE temp_participant_ids (participant_id int(11));

    -- Loop through the  p_participant_idand insert values into the temporary table
	-- Parse and insert participant IDs into the temporary table.
    WHILE NOT done DO
        SET separator_position = LOCATE(',', p_participant_id);				-- Find the position of the next separator.

        IF separator_position > 0 THEN
            SET p_value = SUBSTRING(p_participant_id, 1, separator_position - 1);		-- Extract the current ID.
            SET p_participant_id= SUBSTRING(p_participant_id, separator_position + 1);	-- Trim the processed part.
        ELSE
            SET p_value = p_participant_id;		-- Assign the last ID in the string.
            SET done = 1;						-- End the loop.
        END IF;

        INSERT INTO temp_participant_ids (participant_id) VALUES (p_value); -- Insert the ID into the temporary table.
    END WHILE;

	
	-- Create a temporary table to store parsed loan collaboration types.
	CREATE TEMPORARY TABLE temp_loanCollaboration_Type (loanCollaborationType varchar(255));
	
	-- Parse and insert loan collaboration types into the temporary table.
    -- Loop through the  p_loanCollaborationType and insert values into the temporary table
    WHILE NOT done_1 DO
        SET separator_position_1 = LOCATE(',', p_loanCollaborationType); -- Find the position of the next separator.
        IF separator_position_1 > 0 THEN
            SET V_loanCollaborationType = SUBSTRING(p_loanCollaborationType, 1, separator_position_1 - 1); -- Extract the current type.
            Set p_loanCollaborationType= SUBSTRING(p_loanCollaborationType, separator_position_1 + 1); -- Trim the processed part.
        ELSE
            SET V_loanCollaborationType = p_loanCollaborationType; -- Assign the last type in the string.
            SET done_1 = 1; -- End the loop.
        END IF;

        INSERT INTO temp_loanCollaboration_Type (loanCollaborationType) VALUES (V_loanCollaborationType); -- Insert the type into the temporary table.
    END WHILE;

	-- Check the count of 'Syndication' types and create a temporary table based on the result.
	set v_count =(select count(1) from temp_loanCollaboration_Type where loanCollaborationType='Syndication' );

	if v_count >0 then
	CREATE TEMPORARY TABLE temp_participant_role
	select distinct participant_role from participant_parties; -- Include all participant roles if 'Syndication' exists.
	elseif v_count = 0 then
	CREATE TEMPORARY TABLE temp_participant_role  
	select 'participant' participant_role ;  -- Include only 'participant' role otherwise.
	end if;

-- Step 1 Implementation
    SET v_end_time = NOW(6);
    SET v_step_duration = (SELECT TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time));
    UPDATE procedure_execution_log
    SET end_time = v_end_time, duration_in_mseconds = v_step_duration
    WHERE step_name = v_step_name AND start_time = v_start_time;

  -- Step 2: temp_participation_details
    SET v_step_name = 'Step 2: temp_participation_details';
    SET v_start_time = NOW(6);
    INSERT INTO procedure_execution_log (procedure_name,step_name, start_time) VALUES (v_procedure_name,v_step_name, v_start_time);
	
	CREATE TEMPORARY TABLE temp_net_settlement AS
	SELECT participant_party_id,
		   MAX(net_settlemt_amount_today) AS net_settlemt_amount_today,
		   MAX(net_settlement_amount) AS net_settlement_amount,
		   MAX(net_settlement_amount_date) AS net_settlement_amount_date
	FROM net_settlement_amount_daily_transaction_details_history
	WHERE Transaction_date = CURRENT_DATE
	GROUP BY participant_party_id;
	
	
-- Create a temporary table to store participation details with joined and aggregated data.
CREATE TEMPORARY TABLE temp_participation_details
SELECT pp.id,
       pp.participant_name AS participantName,
       pp.settlement_frequency AS settlementFrequency,
       pp.participant_code AS participantCode,
       pp.commitment_amount AS commitmentAmount,
       pp.deleted_at,
       pp.is_active,
       pp.is_system_defined,
       pp.settlement_frequency_day AS settlementFrequencyDay,
       pp.email_settlement_report AS emailSettlementReport,
       nsa.net_settlemt_amount_today AS net_exp_amount_today,
       nsa.net_settlement_amount AS net_exp_amount,
       nsa.net_settlement_amount_date AS Settlement_date
FROM participant_parties pp
JOIN temp_participant_ids tt ON pp.id = tt.participant_id
JOIN temp_participant_role tr ON CONVERT(pp.participant_role USING utf8mb4) = CONVERT(tr.participant_role USING utf8mb4)
LEFT JOIN temp_net_settlement nsa ON nsa.participant_party_id = pp.id
WHERE pp.deleted_at IS NULL;

-- Create an index on the temporary table for faster access.
create index tpd_ind on temp_participation_details (id);

-- Step 2 temp_participation_details
    SET v_end_time = NOW(6);
    SET v_step_duration = (SELECT TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time));
    UPDATE procedure_execution_log
    SET end_time = v_end_time, duration_in_mseconds = v_step_duration
    WHERE step_name = v_step_name AND start_time = v_start_time;
	

  -- Step 3: temp_participation_loan_summary_info
    SET v_step_name = 'Step 3: temp_participation_loan_summary_info';
    SET v_start_time = NOW(6);
    INSERT INTO procedure_execution_log (procedure_name,step_name, start_time) VALUES (v_procedure_name,v_step_name, v_start_time);

-- Creates a temporary table to store aggregated loan summary information for each participant.
CREATE TEMPORARY TABLE temp_participation_loan_summary_info AS
SELECT pls.participant_party_id,
       SUM(COALESCE(pls.disbursement, 0)) disbursement,						-- Calculates the total disbursement for each participant, treating NULLs as 0.
       SUM(COALESCE(pls.principal_paid, 0)) principal_paid,					 -- Calculates the total principal paid for each participant, treating NULLs as 0.
	   -- Computes the balance by subtracting total principal paid from total disbursement.
       (SUM(COALESCE(pls.disbursement, 0)) - SUM(COALESCE(pls.principal_paid, 0))) AS disbursementBalance,
       sum(COALESCE(pls.closing_balance,0)) as principal_inbalance,			-- Calculates the total principal in balance for each participant.
       SUM(COALESCE(pls.outstanding_balance, 0))outstanding_balance			-- Computes the total outstanding balance for each participant.
FROM participant_loan_summaries pls											-- Sources data from the participant loan summaries table.
     JOIN temp_participant_ids tt ON pls.participant_party_id = tt.participant_id  -- Filters records based on matching participant party IDs with a temporary table.
GROUP BY pls.participant_party_id;											-- Groups the aggregated results by participant party ID.

-- Creates an index on the participant_party_id column for faster lookups in the temporary loan summary table.
create index tplsi_ind on temp_participation_loan_summary_info  (participant_party_id);

-- Step 3 temp_participation_loan_summary_info
    SET v_end_time = NOW(6);
    SET v_step_duration = (SELECT TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time));
    UPDATE procedure_execution_log
    SET end_time = v_end_time, duration_in_mseconds = v_step_duration
    WHERE step_name = v_step_name AND start_time = v_start_time;

-- Step 4: temp_participation_count_summary_info
    SET v_step_name = 'Step 4: temp_participation_count_summary_info';
    SET v_start_time = NOW(6);
    INSERT INTO procedure_execution_log (procedure_name,step_name, start_time) VALUES (v_procedure_name,v_step_name, v_start_time);

-- Creates a temporary table to store count-based participant information.
CREATE TEMPORARY TABLE temp_participation_count_summary_info
SELECT
    pp.id participant_party_id,						 --  participant ID as the primary key.
    COALESCE(mail_count,0)mail_count,					 -- Retrieves the mail count or defaults to 0 if NULL.
    COALESCE(contact_count,0)contact_count,				 -- Retrieves the contact count or defaults to 0 if NULL.
    COALESCE(bank_account_count,0) bank_account_count,	 -- Retrieves the bank account count or defaults to 0 if NULL.
    COALESCE(loan_count,0)loan_count					 -- Retrieves the loan count or defaults to 0 if NULL.
FROM temp_participation_details pp						 -- Sources base data from the participation details temporary table.
LEFT JOIN												 -- Counts mail addresses per participant ID.
    (SELECT participant_party_id, COUNT(1) AS mail_count
     FROM participant_mail_addresses
     WHERE deleted_at IS NULL							 -- Excludes deleted mail addresses
     GROUP BY participant_party_id) pma ON pma.participant_party_id= pp.id
LEFT JOIN
    (SELECT participant_party_id, COUNT(1) AS contact_count -- Counts contacts per participant ID.
     FROM participant_contacts							
     WHERE deleted_at IS NULL							-- Excludes deleted contacts.
     GROUP BY participant_party_id) pc ON pc.participant_party_id = pp.id
LEFT JOIN
    (SELECT participant_party_id, COUNT(1) AS bank_account_count -- Counts bank accounts per participant ID.
     FROM participant_client_bank_accounts			  
     WHERE deleted_at IS NULL						  -- Excludes deleted bank accounts.
     GROUP BY participant_party_id) pcb ON  pcb.participant_party_id = pp.id
LEFT JOIN
    (SELECT pla.participation_party_id, COUNT(1) AS loan_count		-- Counts loans per participant ID.
     FROM participant_loan_allocations pla
	 -- Filters active loans by checking for a NULL closure date.
     JOIN participant_loan_settings pls ON pla.loan_id = pls.loan_id and loan_closure_date is NULL
     join temp_loanCollaboration_Type tt on  tt.loanCollaborationType = pls.loan_collaboration
	  -- Joins to filter by loan collaboration type.
     GROUP BY pla.participation_party_id) pla ON  pla.participation_party_id = pp.id
order by pp.id;

-- Creates an index on the participant_party_id column for faster lookups in the temporary count summary table.
create index tpcsi_ind on temp_participation_count_summary_info (participant_party_id);

-- Step 4 temp_participation_count_summary_info
    SET v_end_time = NOW(6);
    SET v_step_duration = (SELECT TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time));
    UPDATE procedure_execution_log
    SET end_time = v_end_time, duration_in_mseconds = v_step_duration
    WHERE step_name = v_step_name AND start_time = v_start_time;

-- Step 5: temp_participation_user_details
    SET v_step_name = 'Step 5: temp_participation_user_details';
    SET v_start_time = NOW(6);
    INSERT INTO procedure_execution_log (procedure_name,step_name, start_time) VALUES (v_procedure_name,v_step_name, v_start_time);

	
	-- Creates a temporary table to store user update details for participants.
	CREATE TEMPORARY TABLE temp_participation_user_details AS
	SELECT participant_party_id,
		   bs.user_name updated_by,		-- Retrieves the name of the user who last updated the record.
		   bp.updated_at				-- Retrieves the timestamp of the last update.
	FROM
	  (SELECT participant_party_id,
			  substring_index(updated_by, ';', -1) updated_by,		-- Extracts the user ID from the updated_by field.
			  substring_index(updated_by, ';', 1) updated_at		-- Extracts the update timestamp from the updated_by field.
	   FROM
		 (SELECT pp.id participant_party_id,-- Selects the most recent update from multiple sources.
				 GREATEST(COALESCE((SELECT MAX(CONCAT(COALESCE(ppt.updated_at, '1970-00-00 00:00:00'), ';', COALESCE(ppt.updated_by, 0)))
									 FROM participant_parties ppt
									WHERE ppt.id = pp.id
									GROUP BY ppt.id), '1970-00-00 00:00:00;0'),
						  COALESCE((SELECT MAX(CONCAT(COALESCE(pcb.updated_at, '1970-00-00 00:00:00'), ';', COALESCE(pcb.updated_by, 0)))
									  FROM participant_client_bank_accounts pcb
									 WHERE pcb.participant_party_id = pp.id
									 GROUP BY pcb.participant_party_id), '1970-00-00 00:00:00;0'),
						  COALESCE((SELECT MAX(CONCAT(COALESCE(pma.updated_at, '1970-00-00 00:00:00'), ';', COALESCE(pma.updated_by, 0)))
							 		  FROM participant_mail_addresses pma
									 WHERE pma.participant_party_id = pp.id
									GROUP BY pma.participant_party_id), '1970-00-00 00:00:00;0'),
					      COALESCE((SELECT MAX(CONCAT(COALESCE(pc.updated_at, '1970-00-00 00:00:00'), ';', COALESCE(pc.updated_by, 0)))
									 FROM participant_contacts pc
								    WHERE pc.participant_party_id = pp.id
								    GROUP BY pc.participant_party_id), '1970-00-00 00:00:00;0')) AS updated_by
		  FROM temp_participation_details pp)vp)bp
	LEFT JOIN base_users bs ON bp.updated_by = bs.id;

	-- Creates an index on the participant_party_id column for faster lookups in the temporary user details table.
	create index tpud_ind on temp_participation_user_details(participant_party_id);

-- Step 5 temp_participation_user_details
    SET v_end_time = NOW(6);
    SET v_step_duration = (SELECT TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time));
    UPDATE procedure_execution_log
    SET end_time = v_end_time, duration_in_mseconds = v_step_duration
    WHERE step_name = v_step_name AND start_time = v_start_time;

-- Step 6: Final Result
    SET v_step_name = 'Step 6: Final Result';
    SET v_start_time = NOW(6);
    INSERT INTO procedure_execution_log (procedure_name,step_name, start_time) VALUES (v_procedure_name,v_step_name, v_start_time);

	
SELECT pp.id,															-- Retrieves the participant ID.
       pp.participantName,												-- Retrieves the participant name.
       pp.settlementFrequency,
       pp.participantCode,
       round(COALESCE(pp.commitmentAmount,0),2)commitmentAmount,
       round(COALESCE(pp.commitmentAmount,0)-COALESCE(pls.disbursementBalance,0),2)commitmentBalance,
       round(COALESCE(pls.outstanding_balance,0),2) participationOutstandingBalance,
       round(COALESCE(pls.disbursement,0),2) disbursement,
       round(COALESCE(pls.principal_inbalance,0),2) totalAmountExposure,
       round(COALESCE(pls.disbursementBalance,0),2) disbursementBalance,
       COALESCE(pcs.loan_count, 0) noOfLoansCount,
       COALESCE(pcs.bank_account_count, 0) bankDetailsCount,
       COALESCE(pcs.mail_count, 0) mailingAddressCount,
       COALESCE(pcs.contact_count, 0) contactsCount,
       pud.updated_by,
       pud.updated_at,
       pp.is_active,
       pp.is_system_defined,
       pp.settlementFrequencyDay,
       --  if (COALESCE(pp.emailSettlementReport,0)= 0,'false','true') as emailSettlementReport,
       pp.emailSettlementReport,
       pp.net_exp_amount_today as netsettlementasoftoday,
       net_exp_amount as  netsettlementamount,
       Settlement_date
FROM temp_participation_details pp
LEFT JOIN temp_participation_loan_summary_info pls ON pp.id =pls.participant_party_id  -- Joins the loan summary data to the participant details.
LEFT JOIN temp_participation_count_summary_info pcs ON pcs.participant_party_id=pp.id  -- Joins the count summary data to the participant details.
LEFT JOIN temp_participation_user_details pud ON pud.participant_party_id=pp.id		   -- Joins the user details to the participant details.
order by pud.updated_at desc ,pp.participantName asc;								   -- Orders results by update timestamp and participant name.

-- Step 6: Final Result
    SET v_end_time = NOW(6);
    SET v_step_duration = (SELECT TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time));
    UPDATE procedure_execution_log
    SET end_time = v_end_time, duration_in_mseconds = v_step_duration
    WHERE step_name = v_step_name AND start_time = v_start_time;

END ;;

DELIMITER ;