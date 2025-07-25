DROP PROCEDURE IF EXISTS participant_loan_allocation_setup_proc;
DELIMITER ;;
CREATE PROCEDURE participant_loan_allocation_setup_proc(p_loan_id int(11))

proc_label:BEGIN

-- Declare variables
DECLARE v_loan_collaboration varchar(50);
DECLARE V_role_loan_deal varchar(50);
DECLARE V_participation_party_id INT(11);
DECLARE v_count INTEGER;
DECLARE v_start_time DATETIME(6);					     -- Variables for step logging
DECLARE v_end_time DATETIME(6);
DECLARE v_step_duration INT;
DECLARE v_step_name VARCHAR(255);
DECLARE v_procedure_name VARCHAR(255) DEFAULT 'participant_loan_allocation_setup_proc';

 -- Drop temporary tables if they exist
DROP TEMPORARY TABLE IF EXISTS temp_loancollaboration_type;
DROP TEMPORARY TABLE IF EXISTS temp_participant_party_id;
DROP TEMPORARY TABLE IF EXISTS temp_loan_allocation_setup;

-- Create temporary tables
CREATE TEMPORARY TABLE temp_loancollaboration_type (participantRole Varchar(50));
CREATE TEMPORARY TABLE temp_participant_party_id (participantRole Varchar(50),participant_party_id Int(11));

-- Fetch loan collaboration details
SELECT pls.loan_collaboration,role_loan_deal,participation_party_id 
  INTO v_loan_collaboration, V_role_loan_deal,V_participation_party_id 
FROM participant_loan_settings pls
WHERE pls.loan_id = p_loan_id LIMIT 1;

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
	  
 -- Step 1: temp_loancollaboration_type 
    SET v_step_name = 'Step 1: temp_loancollaboration_type';
    SET v_start_time = NOW(6);
    INSERT INTO procedure_execution_log (procedure_name,step_name, start_time) VALUES (v_procedure_name,v_step_name, v_start_time);

-- Conditional logic based on loan collaboration type
IF v_loan_collaboration ='Participation' THEN
	INSERT INTO temp_loancollaboration_type values('Participant');
        INSERT INTO temp_participant_party_id Values ( null,V_participation_party_id);

ELSEIF v_loan_collaboration ='Syndication'  AND V_role_loan_deal = 'Lead Lender'THEN

	--  INSERT INTO temp_loancollaboration_type SELECT 'Participant' UNION SELECT 'Lead Lender';
        INSERT INTO temp_loancollaboration_type values('Participant');

	SELECT COUNT(1) into v_count from participant_loan_allocations where loan_id=p_loan_id and participation_party_id=V_participation_party_id;
	
	If V_count > 0 Then
	
		INSERT INTO temp_participant_party_id Values (null,null);
	Else

		INSERT INTO temp_participant_party_id Values ('Lead Lender',V_participation_party_id);

	End IF;

ELSEIF v_loan_collaboration ='Syndication' AND V_role_loan_deal = 'Agent'THEN

	INSERT INTO temp_loancollaboration_type values('Participant');
        INSERT INTO temp_participant_party_id Values (null, V_participation_party_id);
ELSE
	INSERT INTO temp_loancollaboration_type VALUES(NULL);
        INSERT INTO temp_participant_party_id Values ( null,null);

END IF;

-- Step 1 temp_loancollaboration_type
    SET v_end_time = NOW(6);
    SET v_step_duration = (SELECT TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time));
    UPDATE procedure_execution_log
    SET end_time = v_end_time, duration_in_mseconds = v_step_duration
    WHERE step_name = v_step_name AND start_time = v_start_time;

  -- Step 2: temp_loan_allocation_setup
    SET v_step_name = 'Step 2: temp_loan_allocation_setup';
    SET v_start_time = NOW(6);
    INSERT INTO procedure_execution_log (procedure_name,step_name, start_time) VALUES (v_procedure_name,v_step_name, v_start_time);
	
-- Create the final temporary table for loan allocation setup
CREATE TEMPORARY TABLE temp_loan_allocation_setup
SELECT id ,
       participant_name AS participantName,
       participant_code AS participantCode,
       participant_number AS participantNumber,
       participantRole,
       IF(COALESCE(acceptOverAdvance,0)=0,'false','true')acceptOverAdvance,
       IF(isActive =0,'false','true') isActive,
       IF(isSystemDefined =0,'false','true') isSystemDefined,
       settlementFrequencyDay,
       IF(isPrimaryManagerAll =0,'false','true')isPrimaryManagerAll,
       IF(emailSettlementReport =0,'false','true')emailSettlementReport,
       IF(isSendNotification =0,'false','true')isSendNotification,
        COALESCE(commitment_amount,0) AS commitmentAmount,
        COALESCE((commitment_amount- coalesce(allocation_loan_limit,0)),0) AS commitmentBalance
From (SELECT pp.id ,
       pp.participant_name ,
       pp.participant_code,
	   pp.participant_number,
       pp.participant_role AS participantRole,
	   pp.settlement_frequency AS settlementFrequency,
	   pp.accept_over_advance AS acceptOverAdvance,
	   pp.is_active AS isActive,
	   pp.is_system_defined AS isSystemDefined,
	   pp.settlement_frequency_day AS settlementFrequencyDay,
	   pp.is_primary_manager_all AS isPrimaryManagerAll,
	   pp.email_settlement_report AS emailSettlementReport,
	   pp.is_send_notification AS isSendNotification,
       pp.commitment_amount,
       SUM(COALESCE(pla.participation_max_amount,0)) AS allocation_loan_limit
FROM participant_parties pp
        JOIN temp_loancollaboration_type tt ON tt.participantRole = pp.participant_role
        LEFT JOIN participant_loan_allocations pla ON pp.id = pla.participation_party_id AND pla.is_closure = 0
    WHERE 
         pp.id NOT IN (
            SELECT 
                participation_party_id
            FROM 
                participant_loan_allocations
            WHERE 
                loan_id = p_loan_id
        )
        AND pp.is_active = 1
        AND pp.deleted_at IS NULL
    GROUP BY pp.id,
            pp.participant_name,
            pp.participant_code,
            pp.participant_number,
            pp.participant_role,
            pp.settlement_frequency,
            pp.accept_over_advance,
            pp.is_active,
            pp.is_system_defined,
            pp.settlement_frequency_day,
            pp.is_primary_manager_all,
            pp.email_settlement_report,
            pp.is_send_notification,
            pp.commitment_amount
UNION
SELECT pp.id,
       pp.participant_name,
       pp.participant_code,
       pp.participant_number,
       pp.participant_role AS participantRole,
       pp.settlement_frequency AS settlementFrequency,
       pp.accept_over_advance AS acceptOverAdvance,
       pp.is_active AS isActive,
       pp.is_system_defined AS isSystemDefined,
       pp.settlement_frequency_day AS settlementFrequencyDay,
       pp.is_primary_manager_all AS isPrimaryManagerAll,
       pp.email_settlement_report AS emailSettlementReport,
       pp.is_send_notification AS isSendNotification,
       pp.commitment_amount,
       SUM(COALESCE(pla.participation_max_amount, 0)) AS allocation_loan_limit
FROM participant_parties pp
JOIN temp_participant_party_id pt ON pt.participantRole = pp.participant_role and pt.participant_party_id = pp.id 
LEFT JOIN participant_loan_allocations pla ON pp.id = pla.participation_party_id
AND pla.is_closure = 0 GROUP BY pp.id,
            pp.participant_name,
            pp.participant_code,
            pp.participant_number,
            pp.participant_role,
            pp.settlement_frequency,
            pp.accept_over_advance,
            pp.is_active,
            pp.is_system_defined,
            pp.settlement_frequency_day,
            pp.is_primary_manager_all,
            pp.email_settlement_report,
            pp.is_send_notification,
            pp.commitment_amount)pp where pp.id is not null;

		-- Return the final result
        SELECT * FROM temp_loan_allocation_setup;
		
-- Step 2 temp_loan_allocation_setup
    SET v_end_time = NOW(6);
    SET v_step_duration = (SELECT TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) * 1000000 + MICROSECOND(v_end_time) - MICROSECOND(v_start_time));
    UPDATE procedure_execution_log
    SET end_time = v_end_time, duration_in_mseconds = v_step_duration
    WHERE step_name = v_step_name AND start_time = v_start_time;

END ;;

DELIMITER ;
