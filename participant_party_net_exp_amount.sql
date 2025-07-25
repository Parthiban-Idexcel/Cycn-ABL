DROP PROCEDURE IF EXISTS participant_party_net_exp_amount_history;
DELIMITER ;;

CREATE PROCEDURE participant_party_net_exp_amount_history()
BEGIN

DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE duration TIME;

DROP TEMPORARY TABLE IF EXISTS temp_pp_net_exp_amount_details;
DROP TEMPORARY TABLE IF EXISTS temp_pp_settle_amount_details;
DROP TEMPORARY TABLE IF EXISTS temp_participant_party_details;

-- Insert start time into log table
INSERT INTO event_log_participant_party_net_exp_amount (event_name, start_time) VALUES ('participant_party_net_exp_amount_history', NOW());

SET start_time = NOW(3);

CREATE TEMPORARY TABLE IF NOT EXISTS temp_participant_party_details
SELECT distinct pp.id participant_party_id,
       pla.id participant_loan_allocation_id,
	   pla.allocation_number,
       pl.id loan_id,
       pl.original_pk original_loan_id,
       ptd.activity_date
FROM participant_loan_allocations pla
   JOIN participant_parties pp ON pp.id = pla.participation_party_id
   JOIN participant_loans pl ON pl.id = pla.loan_id
   JOIN participant_charge_templates pct ON pct.participant_loan_allocation_id=pla.id
   LEFT JOIN participant_transaction_details ptd ON ptd.participant_loan_allocation_id = pla.id
   AND ptd.participant_party_id = pla.participation_party_id
   AND ptd.participant_charge_template_id = pct.id
   order by pp.id,pla.id,ptd.activity_date;

SET end_time = NOW(3);
SET duration = TIMEDIFF(end_time, start_time);
SELECT 'STEP-1', duration;
SET start_time = NOW(3);

CREATE TEMPORARY TABLE IF NOT EXISTS temp_pp_net_exp_amount_details
SELECT ptd.participant_party_id,
       ptd.participant_loan_allocation_id,
       ptd.activity_date,
       loan_net_exp_amount_history(ptd.participant_loan_allocation_id, ptd.activity_date) Net_exp_amount
FROM temp_participant_party_details ptd
where ptd.activity_date is not null
GROUP BY ptd.participant_party_id,
         ptd.participant_loan_allocation_id,
         ptd.activity_date;

SET end_time = NOW(3);
SET duration = TIMEDIFF(end_time, start_time);
SELECT 'STEP-2',duration;
SET start_time = NOW(3);

CREATE TEMPORARY TABLE IF NOT EXISTS temp_pp_settle_amount_details
SELECT ptd.participant_party_id,
       ptd.activity_date,
       participant_net_exp_amount_history(ptd.participant_party_id, ptd.activity_date) net_settlement_amount
FROM temp_participant_party_details ptd
where ptd.activity_date is not null
GROUP BY ptd.participant_party_id,
         ptd.activity_date;


SET end_time = NOW(3);
SET duration = TIMEDIFF(end_time, start_time);
SELECT 'STEP-3', duration;
SET start_time = NOW(3);

--  CREATE TEMPORARY TABLE IF NOT EXISTS PARTICIPANT_ALLOCATION_WISE_NET_EXP_AMOUNT
REPLACE INTO participant_allocation_wise_net_exp_amount
SELECT pd.loan_id,
       pd.original_loan_id,
       pd.allocation_number,
       pd.activity_date,
       pd.participant_party_id,
       pd.participant_loan_allocation_id,
       nad.Net_exp_amount,
       sad.net_settlement_amount
FROM temp_participant_party_details pd
     LEFT JOIN temp_pp_net_exp_amount_details nad ON pd.participant_party_id = nad.participant_party_id
          AND nad.participant_loan_allocation_id = pd.participant_loan_allocation_id
          AND pd.activity_date = nad.activity_date
     LEFT JOIN temp_pp_settle_amount_details sad ON pd.participant_party_id = sad.participant_party_id
          AND pd.activity_date = sad.activity_date;

SET end_time = NOW(3);
SET duration = TIMEDIFF(end_time, start_time);
SELECT 'STEP-4',duration;

  -- Insert end time into log table
    UPDATE event_log_participant_party_net_exp_amount SET end_time = NOW() WHERE event_name = 'participant_party_net_exp_amount_history' AND end_time IS NULL;

END ;;

DELIMITER ;