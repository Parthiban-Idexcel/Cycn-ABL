DROP function IF EXISTS loan_net_exp_amount_history;
DELIMITER ;;
CREATE function loan_net_exp_amount_history(p_loan_allocations_id INT(11),p_activity_date date)
RETURNS varchar(255)
BEGIN

DECLARE v_net_exp_settlement_amount varchar(255);
DECLARE v_sett_date date;
DECLARE v_from_date date;
DECLARE v_pp_date date;
DECLARE v_to_date date;
Declare v_participant_name varchar(50);
declare v_pid int(25);
declare v_settlement_frequency varchar(50);
declare v_settlement_frequency_day varchar(50);
declare v_settlement_Date date;
declare v_settlement_Dates date;
declare v_current_date date;
declare v_participant_commitment_Amount DECIMAL(24,11);
declare v_participant_commitment_balance DECIMAL(24,11);
declare v_loan_id int (11);
declare v_original_pk int (11);
declare v_loan_type varchar(255);

DROP TEMPORARY TABLE IF EXISTS settlement_summary_original_principal_balance;
DROP TEMPORARY TABLE IF EXISTS settlement_summary_details;
DROP TEMPORARY TABLE IF EXISTS temp_part_net_exp_settlement_amount;
DROP TEMPORARY TABLE IF EXISTS temp_net_exp_settlement_amount;
DROP TEMPORARY TABLE IF EXISTS temp_bb_availability;

select pp.id,pp.participant_name,pp.settlement_frequency,pp.settlement_frequency_day into v_pid,v_participant_name,v_settlement_frequency,v_settlement_frequency_day from participant_parties pp join participant_loan_allocations pla on pp.id=pla.participation_party_id where pla.id=p_loan_allocations_id limit 1;

set v_from_date =(SELECT
                        CASE
                            WHEN v_settlement_frequency = 'Weekly' THEN
                                CASE v_settlement_frequency_day
                                    WHEN 'Monday' THEN
                                        CASE
                                            WHEN DAYOFWEEK(p_activity_date) = 2 THEN p_activity_date  -- Monday
                                            ELSE DATE_SUB(p_activity_date, INTERVAL (DAYOFWEEK(p_activity_date) + 5) % 7 DAY)
                                        END

                                   WHEN 'Tuesday' THEN
                                                                                CASE
                                           WHEN DAYOFWEEK(p_activity_date) = 3 THEN p_activity_date  -- Tuesday
                                           ELSE DATE_SUB(p_activity_date, INTERVAL (DAYOFWEEK(p_activity_date) + 4) % 7 DAY)
                                       END

                                                                        WHEN 'Wednesday' THEN
                                                                                CASE
                                                                                        WHEN DAYOFWEEK(p_activity_date) = 4 THEN p_activity_date  -- Wednesday
                                                                                        ELSE DATE_SUB(p_activity_date, INTERVAL (DAYOFWEEK(p_activity_date) + 3) % 7 DAY)
                                                                                END
                                                                        WHEN 'Thursday' THEN
                                                                                CASE
                                                                                        WHEN DAYOFWEEK(p_activity_date) = 5 THEN p_activity_date  -- Thursday
                                                                                        ELSE DATE_SUB(p_activity_date, INTERVAL (DAYOFWEEK(p_activity_date) + 2) % 7 DAY)
                                                                                END
                                                                        WHEN 'Friday' THEN
                                                                                CASE
                                                                                        WHEN DAYOFWEEK(p_activity_date) = 6 THEN p_activity_date  -- Friday
                                                                                        ELSE DATE_SUB(p_activity_date, INTERVAL (DAYOFWEEK(p_activity_date) + 1) % 7 DAY)
                                                                                END
                                                                        WHEN 'Saturday' THEN
                                                                                CASE
                                                                                        WHEN DAYOFWEEK(p_activity_date) = 7 THEN p_activity_date  -- Saturday
                                                                                        ELSE DATE_SUB(v_to_date, INTERVAL (DAYOFWEEK(p_activity_date) + 0) % 7 DAY)
                                                                                END
                                                                        WHEN 'Sunday' THEN
                                                                                CASE
                                                                                        WHEN DAYOFWEEK(p_activity_date) = 1 THEN p_activity_date  -- Sunday
                                                                                        ELSE DATE_SUB(p_activity_date, INTERVAL (DAYOFWEEK(p_activity_date) + 6) % 7 DAY)
                                                                                END
                                                                END
                                                        WHEN v_settlement_frequency = 'Monthly' THEN
                                                                CASE v_settlement_frequency_day
                                                                        WHEN 'First Day of Month' THEN DATE_FORMAT(p_activity_date, '%Y-%m-01')
                                                                        WHEN 'Last Day of Month' THEN LAST_DAY(p_activity_date)
                                                                END
                                                END);

    SET v_pp_date = (SELECT DATE_SUB(v_from_date, INTERVAL 1 DAY) );
    SET v_settlement_Date =(SELECT DATE_ADD(v_from_date, INTERVAL 7 DAY));
    set v_to_date = (select p_activity_date);

set v_participant_commitment_Amount =(select commitment_amount from participant_parties where id = v_pid);

set v_participant_commitment_balance =(select (SUM(COALESCE(pls.disbursement, 0)) - SUM(COALESCE(pls.principal_paid, 0))) AS disbursementBalance from participant_loan_summaries pls  where participant_party_id = v_pid GROUP BY pls.participant_party_id);

CREATE TEMPORARY TABLE temp_bb_availability
SELECT
    pla.participation_party_id,
    pla.allocation_number,
    pla.loan_id,
    pl.loan_type,
   MAX(IF(description='Total Available For Advance' AND section='total_collateral', COALESCE(bh.total, 0), 0)) AS gross_bb_availability,
   MAX(IF(description='NET BORROWING BASE AVAILABILITY' AND section='net_availability', COALESCE(bh.total, 0), 0)) AS net_bb_availability
FROM
    abl_bbc_histories bh
    JOIN abl_bbcs bbc ON bbc.id = bh.bbc_id AND bbc.borrower_id = bh.borrower_id
    JOIN participant_loans pl ON bh.borrower_id = pl.borrower_id
    JOIN participant_loan_settings pls ON pl.id = pls.loan_id AND pl.borrower_id = pls.client_id AND pls.loan_deal = 'Sold'
    JOIN participant_loan_allocations pla ON pla.loan_id = pl.id
WHERE loan_type='ABL'
    AND bbc.status NOT IN ('U', 'S', 'D')
    AND pla.id = p_loan_allocations_id
    AND description IN ('NET BORROWING BASE AVAILABILITY', 'Total Available For Advance')
    AND section IN ('net_availability', 'total_collateral')
GROUP BY
    pla.participation_party_id,
    pla.allocation_number,
    pla.loan_id,
    pl.loan_type
ORDER BY
    MAX(bbc.bbc_dt) DESC;


CREATE TEMPORARY TABLE settlement_summary_original_principal_balance
SELECT v_participant_name AS participant_name,
    v_settlement_frequency AS settlement_frequency,
    v_settlement_Date AS settlement_Date,
    over_advance,
    allocation_number,
    allocation_id,
    over_advance_on,
    over_advance_percentage,
    loanid,
    loan_name,
    original_princ_balance,
    credit_line,
    global_loan_credit_line,
    Over_Advance_Amount,
    Amount_Available_Participate_Settlement_Date ,
    loan_allocation_method,
    participation_percentage,
    participation_max_amount,
    if (loan_allocation_method ='percentage_allocation',((Amount_Available_Participate_Settlement_Date * participation_percentage) / 100),(Amount_Available_Participate_Settlement_Date)) AS party_alloc_amt,
    gross_bb_availability,
    net_bb_availability
FROM (
    SELECT
        over_advance,
        allocation_number,
        over_advance_on,
        over_advance_percentage,
        allocation_id,
        loanid,
        loan_name,
        original_princ_balance,
        credit_line,
        global_loan_credit_line,
        if((original_princ_balance - global_loan_credit_line) > 0,(original_princ_balance - global_loan_credit_line),0) Over_Advance_Amount,
        least (original_princ_balance,global_loan_credit_line) Amount_Available_Participate_Settlement_Date,
        loan_allocation_method,
        participation_percentage,
        participation_max_amount,
        gross_bb_availability,
        net_bb_availability
    FROM (
        SELECT over_advance,
            allocation_id,
            allocation_number,
            over_advance_on,
            over_advance_percentage,
            loanid,
            loan_name,
            original_princ_balance,
            credit_line,
            case
            when over_advance_on ='gl_credit_line_amount' THEN ((credit_line*(COALESCE(over_advance_percentage,0)/100))+credit_line)
            when over_advance_on ='gl_disbursement_amount' THEN ((original_princ_balance*(COALESCE(over_advance_percentage,0)/100))+original_princ_balance)
            when over_advance_on ='net_bb_availability' then ((net_bb_availability*(COALESCE(over_advance_percentage,0)/100))+net_bb_availability)
            when over_advance_on ='gross_bb_availability' then ((gross_bb_availability*(COALESCE(over_advance_percentage,0)/100))+gross_bb_availability)
            End AS global_loan_credit_line ,
            loan_allocation_method,
            participation_percentage,
            participation_max_amount,
            gross_bb_availability,
            net_bb_availability
        FROM (
            SELECT
                pp.over_advance,
                pp.allocation_id,
                pp.allocation_number,
                pp.over_advance_on,
                pp.over_advance_percentage,
                pp.loanid,
                pp.loan_name,
                (disburment_amount - ABS(repayment_amount)) AS original_princ_balance,
                pp.credit_line,
                pp.loan_allocation_method,
                pp.participation_percentage,
                pp.participation_max_amount,
                pp.gross_bb_availability,
                pp.net_bb_availability
            FROM (
                SELECT
                    pla.is_accept_over_advance AS over_advance,
                    pla.id allocation_id,
                    pla.allocation_number,
                    pla.over_advance_on,
                    pla.over_advance_percentage,
                    pl.id loanid,
                    pl.loan_name,
                    pl.credit_line,
                    SUM(
                        IF(charge_code_name = 'DISBURSEMENT',
                           IF(natural_sign = '+', pch.charge_amount, -1 * pch.charge_amount),
                           0
                        )
                    ) AS disburment_amount,
                    SUM(
                        IF(charge_code_name = 'REPAYMENT',
                           IF(natural_sign = '+', charge_amount, -1 * charge_amount),
                           0
                        )
                    ) AS repayment_amount,
                   pls.loan_allocation_method,
                  if (pls.loan_allocation_method ='percentage_allocation' , pla.participation_percentage, pla.participation_priority ) participation_percentage,
                    participation_max_amount,
                   COALESCE(tba.gross_bb_availability,0) gross_bb_availability ,
                   COALESCE(tba.net_bb_availability,0)net_bb_availability
                FROM participant_charges_histories pch
                JOIN participant_loans pl ON pl.original_pk = pch.original_loan_id and pl.loan_type = if(pch.loan_type = 'NABL','MCL', pch.loan_type)
                JOIN participant_loan_allocations pla ON pla.loan_id = pl.id
                left join temp_bb_availability tba on pla.loan_id=tba.loan_id and pla.participation_party_id=tba.participation_party_id and pla.allocation_number=tba.allocation_number
                WHERE pla.id = p_loan_allocations_id
                    AND activity_date <= v_to_date
                    AND charge_code_name IN ('DISBURSEMENT', 'REPAYMENT')
                GROUP BY pch.original_loan_id
            ) pp
        ) vp
    ) st
) pj
 order by allocation_id,loanid ;

CREATE TEMPORARY TABLE settlement_summary_details
SELECT
    VP.allocation_number,
    VP.allocation_id,
    VP.loanid,
    VP.party_principal_balance,
    VP.advance_received,
    VP.collection_received,
    VP.party_Disbursement_balance,
    VP.party_Disbursement_balance AS Cumulative_Party_Disbursement_Balance,
    VP.Charges_created,
    VP.Charges_paid
FROM (
    SELECT
        pp.allocation_number,
        pp.allocation_id,
        pp.loanid,
        (pp.advance_paid - ABS(collection_paid)) AS party_principal_balance,
        advance_received,
        collection_received,
        (advance_received - ABS(collection_received)) AS party_Disbursement_balance,
        Charges_created,
        Charges_paid
    FROM (
        SELECT
            pla.allocation_number,
            pla.id allocation_id,
            pl.id loanid,
            SUM(
                IF(
                    source_type = 'Advance' AND activity_date <= v_to_date,
                    IF(pct.natural_sign = '+', ptd.charge_amount, -1 * ptd.charge_amount),
                    0
                )
            ) AS advance_received,
            SUM(
                IF(
                    source_type = 'collection' AND activity_date <= v_to_date,
                    IF(pct.natural_sign = '+', ptd.charge_amount, -1 * ptd.charge_amount),
                    0
                )
            ) AS collection_received,
            SUM(CASE
        WHEN pct.source_type in  ('fees/charges' ,'interest') AND activity_date <= v_to_date
             AND (pct.add_to_balance=1 OR (pct.add_to_balance=0 and pct.accrued_to_loan=0)) THEN
                 IF(pct.natural_sign = '+', ptd.charge_amount, -1 * ptd.charge_amount)
        ELSE 0
    END) as Charges_created,
            SUM(CASE
                WHEN pct.source_type in('collection') AND activity_date <= v_to_date THEN
                        CASE
                             WHEN natural_sign = '+' THEN
                                  ptd.pending_accrued_fees + ptd.pending_accrued_interest
                             ELSE
                                -1 * (ptd.pending_accrued_fees + ptd.pending_accrued_interest)
                         END
                ELSE 0
            END)  AS Charges_paid,
            COALESCE(
                SUM(
                    IF(
                        source_type = 'Advance' AND activity_date <= v_to_date,
                        IF(pct.natural_sign = '+', ptd.charge_amount, -1 * ptd.charge_amount),
                        0
                    )
                ),
                0
            ) AS advance_paid,
            COALESCE(
                SUM(
                    IF(
                        source_type = 'collection' AND activity_date <= v_to_date,
                        IF(pct.natural_sign = '+', ptd.charge_amount, -1 * ptd.charge_amount),
                        0
                    )
                ),
                0
            ) AS collection_paid
        FROM participant_loan_allocations pla
        JOIN participant_loans pl ON pl.id = pla.loan_id
        JOIN participant_charge_templates pct ON pct.participant_loan_allocation_id=pla.id
              AND pct.source_type IN ("Advance", "collection", "fees/charges", "interest")
        left JOIN participant_transaction_details ptd ON pla.participation_party_id = ptd.participant_party_id
              AND ptd.participant_loan_allocation_id = pla.id
              AND ptd.participant_charge_template_id=pct.id
              AND ptd.activity_date <= v_to_date
        WHERE pla.id = p_loan_allocations_id
        GROUP BY allocation_number
        ) pp
) VP order by VP.allocation_id,VP.loanid ;

CREATE TEMPORARY TABLE temp_part_net_exp_settlement_amount
SELECT
    vp.participant_name,
    vp.settlement_frequency,
    vp.settlement_Date,
    vp.over_advance,
    vp.allocation_number,
    vp.allocation_id,
    vp.over_advance_on,
    vp.over_advance_percentage,
    vp.gross_bb_availability,
    vp.net_bb_availability,
    vp.loanid,
    vp.loan_name,
    vp.credit_line,
    vp.global_loan_credit_line,
    vp.Over_Advance_Amount,
    vp.Amount_Available_Participate_Settlement_Date,
    vp.original_princ_balance,
    vp.participation_percentage,
    vp.party_alloc_amt,
    vp.commitment_Amount,
    vp.party_principal_balance,
    vp.commitment_balance ,
    vp.participation_max_amount,
    if (vp.loan_limit_balance <=0,0,vp.loan_limit_balance) as loan_limit_balance,
    LEAST(COALESCE(vp.party_alloc_amt, 0), COALESCE(vp.participation_max_amount,0)) expected_contribution,
    LEAST(COALESCE(vp.party_alloc_amt, 0), COALESCE(vp.participation_max_amount,0)) - COALESCE(vp.party_principal_balance, 0) AS net_exp_amount,
    vp.advance_received,
    vp.collection_received,
   coalesce(vp.party_Disbursement_balance,0) party_Disbursement_balance ,
   coalesce(vp.Cumulative_Party_Disbursement_Balance,0) Cumulative_Party_Disbursement_Balance,
    vp.Charges_created,
    vp.Charges_paid
FROM (
 SELECT
    pp.participant_name,
    pp.settlement_frequency,
    pp.settlement_Date,
    over_advance,
    pp.allocation_number,
    pp.allocation_id,
    pp.over_advance_on,
    pp.over_advance_percentage,
    coalesce(pp.gross_bb_availability,0) gross_bb_availability,
    coalesce(pp.net_bb_availability,0)net_bb_availability,
    pp.loanid,
    pp.loan_name,
    pp.credit_line,
    global_loan_credit_line,
    COALESCE(Over_Advance_Amount, 0) AS Over_Advance_Amount,
    COALESCE(Amount_Available_Participate_Settlement_Date, 0) AS Amount_Available_Participate_Settlement_Date,
    COALESCE(pp.original_princ_balance, 0) AS original_princ_balance,
    COALESCE(pp.participation_percentage, 0) AS participation_percentage,
    if (pp.party_alloc_amt <= 0, 0 ,pp.party_alloc_amt) as party_alloc_amt,
    pp.commitment_Amount,
    if (COALESCE(pp.party_principal_balance, 0)<=0,0,coalesce(pp.party_principal_balance,0)) AS party_principal_balance,
    if (COALESCE(pp.commitment_balance, 0)<=0,0,coalesce(pp.commitment_balance,0)) AS commitment_balance,
    if (COALESCE(pp.participation_max_amount, 0)<=0,0,coalesce(pp.participation_max_amount,0)) AS participation_max_amount,
    (COALESCE(pp.participation_max_amount, 0) - COALESCE(pp.party_principal_balance, 0)) AS loan_limit_balance,
    COALESCE(pp.advance_received, 0) AS advance_received,
    COALESCE(pp.collection_received, 0) AS collection_received,
    COALESCE(pp.party_Disbursement_balance) AS party_Disbursement_balance,
    COALESCE(pp.Cumulative_Party_Disbursement_Balance) AS Cumulative_Party_Disbursement_Balance,
    COALESCE(pp.Charges_created, 0) AS Charges_created,
    COALESCE(pp.Charges_paid, 0) AS Charges_paid
FROM (
    SELECT
        pb.participant_name,
        pb.settlement_frequency,
        pb.settlement_Date,
        over_advance,
        pb.allocation_number,
        pb.allocation_id,
        pb.over_advance_on,
        pb.over_advance_percentage,
        pb.gross_bb_availability,
        pb.net_bb_availability,
        pb.loanid,
        pb.loan_name,
        pb.original_princ_balance,
        pb.credit_line,
        global_loan_credit_line,
        pb.Amount_Available_Participate_Settlement_Date,
        Over_Advance_Amount,
        pb.participation_percentage,
        coalesce(pb.party_alloc_amt,0)party_alloc_amt,
        COALESCE(v_participant_commitment_Amount, 0) commitment_Amount,
        sd.party_principal_balance,
        (COALESCE(v_participant_commitment_Amount,0) - COALESCE(sd.party_principal_balance,0)) AS commitment_balance,
        participation_max_amount,
        advance_received,
        collection_received,
        party_Disbursement_balance,
        Cumulative_Party_Disbursement_Balance,
        Charges_created,
        Charges_paid
    FROM settlement_summary_original_principal_balance pb
    LEFT JOIN settlement_summary_details sd ON pb.allocation_number = sd.allocation_number
    GROUP BY allocation_number
) pp)vp order by vp.allocation_id,vp.loanid ;


CREATE TEMPORARY TABLE temp_net_exp_settlement_amount
SELECT participant_name,
       final_exp_contribution_amount,
       commitment_Amount,
       Charges_created,
       Charges_paid,
      (final_exp_contribution_amount + (charges_created - abs(Charges_paid))) net_exp_settlement_amount,
      expected_contribution,
      net_exp_amount
FROM
        (SELECT sp.participant_name,
                least (sum(sp.net_exp_amount_this_week),sp.commitment_Amount) as final_exp_contribution_amount,
                commitment_Amount,
                sum(sp.Charges_created)Charges_created ,
                sum(sp.Charges_paid)Charges_paid,
                sp.expected_contribution,
                sp.net_exp_amount,
                sp.settlement_Date
  FROM   (
          SELECT pp.participant_name,
             pp.settlement_frequency,
             pp.settlement_Date,
             pp.pp_date,
             pp.from_date,
             pp.to_date,
             pp.over_advance,
             pp.allocation_number,
             pp.loan_name,
             pp.original_princ_balance AS Cumulative_Global_Loan_Disbursement_Balance,
             pp.credit_line AS Global_Loan_Commitment_Amount,
             pp.party_principal_balance,
             pp.Over_Advance_Amount,
             pp.Amount_Available_Participate_Settlement_Date,
             pp.participation_percentage,
             pp.party_alloc_amt,
             pp.commitment_Amount,
             pp.commitment_balance,
             pp.loan_limit,
             pp.loan_limit_balance,
             pp.Cumulative_Party_Disbursement_Balance,
             pp.expected_contribution,
             pp.net_exp_amount,
             pp.net_exp_amount AS net_exp_amount_this_week,
             pp.advance_received,
             pp.collection_received,
             pp.party_Disbursement_balance,
             pp.Charges_created,
             pp.Charges_paid
      FROM (SELECT participant_name,
                settlement_frequency,
                settlement_Date,
                v_pp_date AS pp_date,
                v_from_date AS from_date,
                v_to_date AS TO_DATE,
                over_advance,
                allocation_number,
                loan_name,
                credit_line,
                Over_Advance_Amount,
                Amount_Available_Participate_Settlement_Date,
                original_princ_balance,
                participation_percentage,
                party_alloc_amt,
                commitment_Amount,
                party_principal_balance,
                commitment_balance,
                participation_max_amount loan_limit,
                loan_limit_balance,
                expected_contribution,
                net_exp_amount,
                advance_received,
                collection_received,
                party_Disbursement_balance,
                Cumulative_Party_Disbursement_Balance,
                Charges_created,
                Charges_paid
         FROM temp_part_net_exp_settlement_amount) pp) sp GROUP BY participant_name )sm;




         select concat(round(coalesce(net_exp_amount,0),2),' : ',round(coalesce(expected_contribution,0),2),' : ',round(coalesce(advance_received,0),2)) into  v_net_exp_settlement_amount from temp_part_net_exp_settlement_amount;

return v_net_exp_settlement_amount;

END ;;

DELIMITER ;