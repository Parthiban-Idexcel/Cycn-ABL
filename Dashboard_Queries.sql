Overview
--------

Participating loan count
------------------------
 
WITH ParticipantAllocationCounts AS (
    SELECT
        t.participant_code,
        t.participant_name,
        COUNT(DISTINCT t.allocation_number) AS ParticipantLoanCount
    FROM
        participant_allocation_wise_transaction_history AS t
    GROUP BY
        t.participant_code,
        t.participant_name
),
TotalAllocationCount AS (
    SELECT
        COUNT(DISTINCT allocation_number) AS OverallTotalLoanCount
    FROM
        participant_allocation_wise_transaction_history
)
SELECT
    CONCAT_WS(' - ', pac.participant_name, pac.participant_code) AS `Participant Name - Code`,
    pac.ParticipantLoanCount AS `Participating Loan Count`,
    ROUND(
        CASE
            WHEN tac.OverallTotalLoanCount = 0 THEN 0.00
            ELSE (pac.ParticipantLoanCount / tac.OverallTotalLoanCount) * 100
        END,
    2) AS `Participating Loan Percentage`
FROM
    ParticipantAllocationCounts AS pac
CROSS JOIN -- A CROSS JOIN is used here because 'TotalAllocationCount' is a single-row result,
           -- and we need to join it with every row from 'ParticipantAllocationCounts'.
    TotalAllocationCount AS tac
ORDER BY
    `Participant Name - Code`; -- Order by the new concatenated column

=========================================================================================================================

Net Charges on Participation Loans
----------------------------------------
 
WITH LoanLevelSummary AS (
    SELECT
        COALESCE(mls.other_fees, 0) AS other_fees,
        COALESCE(mls.interest_amount, 0) AS interest_amount,
        (COALESCE(mls.other_fees, 0) + COALESCE(mls.interest_amount, 0)) AS total_charges_amt
    FROM participant_loans pl
    JOIN base_multi_loan_summary mls ON pl.borrower_id = mls.borrower_id
                                   AND pl.original_pk = mls.loan_number_id
                                   AND mls.loan_type = (CASE WHEN pl.loan_type = 'MCL' THEN 'NABL' ELSE pl.loan_type END)
    INNER JOIN participant_loan_settings pls ON pl.id = pls.loan_id
                                      AND pl.borrower_id = pls.client_id
    LEFT JOIN base_affiliate_lenders al ON al.id = pls.affiliate_lender
    INNER JOIN (SELECT DISTINCT loan_id FROM participant_loan_allocations) pla_exists ON pla_exists.loan_id = pl.id
    WHERE pls.loan_deal = 'Sold'
     AND pls.loan_collaboration != 'Syndication' ),

AllocationLevelSummary AS (
    SELECT
        COALESCE(pl_sum.other_fees, 0) AS other_fees,
        COALESCE(pl_sum.interest_amount, 0) AS interest_amount,
        (COALESCE(pl_sum.other_fees, 0) + COALESCE(pl_sum.interest_amount, 0)) AS total_charges_amt
    FROM participant_loan_allocations pla
    JOIN participant_loans pl ON pla.loan_id = pl.id
    JOIN participant_parties pp ON pla.participation_party_id = pp.id
	JOIN participant_loan_settings pls ON pla.loan_id = pls.loan_id
    LEFT JOIN participant_loan_summaries pl_sum ON pla.id = pl_sum.participant_loan_allocation_id
    WHERE pls.loan_deal = 'Sold'
     AND pls.loan_collaboration != 'Syndication' ),

TotalLoanCharges AS (
    SELECT SUM(total_charges_amt) AS sum_loan_charges
    FROM LoanLevelSummary
),
TotalAllocationCharges AS (
    SELECT SUM(total_charges_amt) AS sum_allocation_charges
    FROM AllocationLevelSummary
)

SELECT
    ROUND((COALESCE(tlc.sum_loan_charges, 0) - COALESCE(tac.sum_allocation_charges, 0)) / 1000000000, 2) AS 'Net Charges on Participation Loans'
FROM TotalLoanCharges AS tlc
CROSS JOIN -- Use CROSS JOIN as both CTEs return a single row.
    TotalAllocationCharges AS tac;

Participation / Syndication Analytics
--------------------------------------------------------------------------------------------------------------------------------
Participant Outstanding balance , Participant Unutitlized Loan Limit , Participant Effective Utilization% ---Cards and Participant Loan Count --Table

select * from participant_allocation_wise_transaction_history;
--------------------------------------------------------------------------------------------------------------------------------
Net charges on participation loans -- card 

SELECT * 
    FROM 
      (
        SELECT 
          pl.id, 
          pl.loan_name,
          pls.loan_collaboration,
          '' as allocation_number, 
          IF (al.name is null, ' ', al.name) as participant_name,
          pls.role_loan_deal AS participant_role, 
          mls.client as client_name, 
          mls.client_id as client_no, 
          COALESCE(mls.outstanding_loan_balance, 0) as outstanding_balance, 
          COALESCE(mls.disbursement, 0) as disbursement_amount, 
          (
            COALESCE(mls.principal_paid, 0) + COALESCE(mls.fee_paid, 0) + COALESCE(mls.interest_paid, 0)
          ) AS repayment,
          COALESCE(mls.disbursement, 0) - COALESCE(mls.principal_paid, 0) as disbursement_balance,
          COALESCE(mls.principal_paid, 0) as principal_paid, 
          COALESCE(mls.principal_balance, 0) as principal_balance, 
          COALESCE(mls.fee_paid, 0) as fee_paid, 
          COALESCE(mls.fee_balance, 0) as fee_balance, 
          COALESCE(mls.interest_paid, 0) as interest_paid, 
          COALESCE(mls.interest_balance, 0) as interest_balance, 
          COALESCE(mls.interest_accrued, 0) as interest_accrued, 
          COALESCE(mls.fee_accrued, 0) as fee_accrued, 
          COALESCE(mls.other_fees, 0) as other_fees, 
          COALESCE(mls.interest_amount, 0) as interest_amount, 
          COALESCE(mls.interest_pct, 0) as interest_pct, 
          pl.id as correlation_column, 
          'p' as relation_type
        FROM 
          participant_loans pl 
          join base_multi_loan_summary mls on pl.borrower_id = mls.borrower_id 
          AND pl.original_pk = mls.loan_number_id 
          AND mls.loan_type = (
            case when pl.loan_type = 'MCL' then 'NABL' else pl.loan_type end
          ) 
          inner join participant_loan_settings pls on pl.id = pls.loan_id 
          AND pl.borrower_id = pls.client_id
          LEFT JOIN base_affiliate_lenders al on al.id = pls.affiliate_lender
        where 
          pls.loan_deal = 'Sold'
          AND pls.loan_collaboration != "Syndication"
          AND EXISTS(
            SELECT 
              1 
            FROM 
              participant_loan_allocations pla 
            WHERE 
              pla.loan_id = pl.id
          ) 
        UNION 
        SELECT 
          pla.id, 
          pl.loan_name,
          pls.loan_collaboration,
          pla.allocation_number, 
          pp.participant_name, 
          pp.participant_role, 
          pb.client_name, 
          pb.client_number, 
          pl_sum.outstanding_balance, 
          pl_sum.disbursement, 
          (
            pl_sum.principal_paid + pl_sum.fee_paid + pl_sum.interest_paid
          ),
          pl_sum.disbursement - pl_sum.principal_paid,
          pl_sum.principal_paid, 
          pl_sum.closing_balance, 
          pl_sum.fee_paid, 
          pl_sum.fee_balance, 
          pl_sum.interest_paid, 
          pl_sum.interest_balance, 
          pl_sum.interest_accrued, 
          pl_sum.fee_accrued, 
          pl_sum.other_fees, 
          pl_sum.interest_amount, 
          pl_sum.interest_pct, 
          pla.loan_id, 
          'c' as relation_type
        FROM 
          participant_loan_allocations pla 
          join participant_loans pl on pla.loan_id = pl.id 
          join participant_parties pp on pla.participation_party_id = pp.id 
          left join participant_loan_summaries pl_sum on pla.id = pl_sum.participant_loan_allocation_id 
          join participant_loan_settings pls on pla.loan_id = pls.loan_id 
          join participant_borrowers pb on pb.id = pls.client_id 
        where 
          pls.loan_deal = 'Sold'
          AND pls.loan_collaboration != "Syndication"
      ) grp 
    ORDER BY 
      correlation_column, 
      relation_type desc;
	  

------------------------------------------------------------------------------------------------------------------------------
Total Number of Active Loans-- Card

SELECT pls.loan_collaboration,
       SUM(CASE
               WHEN pls.loan_collaboration = 'Participation'
                    AND pp.participant_role = 'Participant' THEN 1
               WHEN pls.loan_collaboration = 'Syndication'
                    AND pp.participant_role IN ('Lead Lender', 'Participant') THEN 1
               ELSE 0
           END) AS loan_count
FROM participant_loan_allocations pla
JOIN participant_loan_settings pls ON pla.loan_id = pls.loan_id
JOIN participant_parties pp ON pla.participation_party_id = pp.id
GROUP BY pls.loan_collaboration;
--------------------------------------------------------------------------------------------------------------------------------
Total Number if participant--card

SELECT 
  SUM(CASE WHEN is_active = 1  and participant_role in ('Lead Lender','Participant') THEN 1 ELSE 0 END) AS active_count,
  SUM(CASE WHEN is_active = 0  and participant_role in ('Participant') THEN 1 ELSE 0 END) AS inactive_count
FROM participant_parties 
WHERE deleted_at IS NULL;
--------------------------------------------------------------------------------------------------------------------------------

Participant Loan Statistics
---------------------------------------------------------------------------------------------------------------------------------
select * from participant_allocation_wise_transaction_history;

---------------------------------------------------------------------------------------------------------------------------------
Trend Analysis
---------------------------------------------------------------------------------------------------------------------------------
SELECT vp.id AS participant_id,
       vp.allocation_id,
       vp.participant_name,
       vp.Loan_Allocation_No,
       vp.outstanding_balance,
       vp.disburment_amount AS Disbursements,
       vp.repayment_amount AS Total_Repayments,
       (vp.disburment_amount - ABS(vp.repayment_amount)+vp.Fee_Repayments+vp.Interest_Repayments) AS Disbursement_balance,
       vp.Interest_Repayments,
       vp.Interest_Charged,
       vp.interest_balance,
       vp.Fee_Repayments,
       vp.Fee_Charged,
       vp.fee_balance,
       vp.principal_paid AS Principal_Repayments,
       vp.commitment_amount,
       CONCAT(vp.activity_month,"/",vp.activity_year) activity_date,
       vp.loan_collaboration,
       vp.participant_role
FROM
  (SELECT pp.id,
          pp.participant_name,
          pla.allocation_number AS Loan_Allocation_No,
          pla.id AS allocation_id,
          SUM(CASE
                  WHEN pct.natural_sign = '-'
                       AND COALESCE(ptd.accrued_to_loan, 0) = 0 THEN ptd.charge_amount * -1
                  WHEN pct.natural_sign = '+'
                       AND COALESCE(ptd.accrued_to_loan, 0) = 0 THEN ptd.charge_amount
                  ELSE 0
              END) AS outstanding_balance,
          SUM(CASE
                  WHEN pct.source_type = 'advance' THEN ptd.charge_amount
                  ELSE 0
              END) AS disburment_amount,
          SUM(CASE
                  WHEN pct.source_type = 'collection' THEN ptd.charge_amount
                  ELSE 0
              END) AS repayment_amount,
          SUM(CASE
                  WHEN pct.natural_sign = '-'
                       AND pct.add_to_balance = 0
                       AND (pct.source_type = 'fees/charges'
                            OR pct.source_type = 'adjustment') THEN (ptd.charge_amount + ptd.float_amount) * -1
                  WHEN pct.natural_sign = '+'
                       AND pct.add_to_balance = 0
                       AND (pct.source_type = 'fees/charges'
                            OR pct.source_type = 'adjustment') THEN ptd.charge_amount + ptd.float_amount
                  ELSE 0
              END) AS fee_charged,
          (SUM(CASE
                   WHEN (pct.source_type = 'fees/charges'
                         OR pct.source_type = 'adjustment')
                        AND pct.add_to_balance = 0
                        AND COALESCE(pct.accrued_to_loan, 0) = 0 THEN ptd.charge_amount
                   ELSE 0
               END) - SUM(ptd.pending_accrued_fees)) AS fee_balance,
          SUM(CASE
                  WHEN pct.natural_sign = '-'
                       AND pct.add_to_balance = 0
                       AND pct.source_type = 'interest' THEN (ptd.charge_amount + ptd.float_amount) * -1
                  WHEN pct.natural_sign = '+'
                       AND pct.add_to_balance = 0
                       AND pct.source_type = 'interest' THEN ptd.charge_amount + ptd.float_amount
                  ELSE 0
              END) AS interest_charged,
          (SUM(CASE
                   WHEN pct.source_type = 'interest'
                        AND pct.add_to_balance = 0
                        AND COALESCE(pct.accrued_to_loan, 0) = 0 THEN ptd.charge_amount
                   ELSE 0
               END) - SUM(ptd.pending_accrued_interest)) AS interest_balance,
          SUM(CASE
                  WHEN pct.source_type = 'collection' THEN (ptd.charge_amount - ptd.pending_accrued_interest - ptd.pending_accrued_fees)
                  ELSE 0
              END) AS principal_paid,
          SUM(ptd.pending_accrued_interest) AS interest_repayments,
          SUM(ptd.pending_accrued_fees) AS fee_repayments,
          pp.commitment_amount,
          MONTH(ptd.activity_date) as activity_month,
            YEAR(ptd.activity_date) as activity_year,
          pls.loan_collaboration,
          pp.participant_role
   FROM participant_transaction_details ptd
   JOIN participant_loan_allocations pla ON ptd.participant_loan_allocation_id = pla.id
   JOIN participant_charge_templates pct ON ptd.participant_charge_template_id = pct.id
   JOIN participant_loans pl ON pla.loan_id = pl.id
   JOIN participant_parties pp ON pp.id = pla.participation_party_id
   JOIN participant_loan_settings pls ON pla.loan_id = pls.loan_id
   --  WHERE ptd.activity_date <= CURRENT_DATE
   WHERE ptd.activity_date between DATE_SUB(CURRENT_DATE, INTERVAL 365 DAY) and CURRENT_DATE
   GROUP BY pp.id,
            pp.participant_name,
            pla.allocation_number,
            pla.id,
            pp.commitment_amount,
            pls.loan_collaboration,
            pp.participant_role,
            MONTH(ptd.activity_date),
            YEAR(ptd.activity_date)) vp
ORDER BY participant_name,
         Loan_Allocation_No ASC,
         vp.activity_month,
         vp.activity_year;
---------------------------------------------------------------------------------------------------------------------------------
charges Analysis
---------------------------------------------------------------------------------------------------------------------------------

SELECT pp.participant_name,
       pla.allocation_number Loan_Allocation_No,
       ptd.activity_date Transaction_Date,
       pct.participant_charge_name Description,
       if((pct.add_to_balance=0
           AND pct.accrued_to_loan=0),'Accrued to statement', 'In-balance') Posting_Type,
       round(sum(IF(natural_sign='+', ptd.charge_amount, -1*ptd.charge_amount)), 5) amount
FROM participant_loan_allocations pla
JOIN participant_parties pp ON pla.participation_party_id=pp.id
JOIN participant_transaction_details ptd ON ptd.participant_loan_allocation_id=pla.id
JOIN participant_charge_templates pct ON ptd.participant_charge_template_id=pct.id
AND (pct.add_to_balance=1
     OR (pct.add_to_balance=0
         AND pct.accrued_to_loan=0))
AND pct.source_type IN ('interest',
                        'fees/charges')
GROUP BY pp.participant_name,
         pla.allocation_number,
         ptd.activity_date,
         pct.participant_charge_name,
         pct.add_to_balance,
         pct.accrued_to_loan
ORDER BY pla.allocation_number,
         pct.participant_charge_name,
         ptd.activity_date ASC;
		 

---------------------------------------------------------------------------------------------------------------------------
participant metrics
---------------------------------------------------------------------------------------------------------------------------
select * from participant_allocation_wise_transaction_history;
---------------------------------------------------------------------------------------------------------------------------
Global Loan Statistics
---------------------------------------------------------------------------------------------------------------------------
SELECT * 
    FROM 
      (
        SELECT 
          pl.id, 
          pl.loan_name,
          pls.loan_collaboration,
          '' as allocation_number, 
          IF (al.name is null, ' ', al.name) as participant_name,
          pls.role_loan_deal AS participant_role, 
          mls.client as client_name, 
          mls.client_id as client_no, 
          COALESCE(mls.outstanding_loan_balance, 0) as outstanding_balance, 
          COALESCE(mls.disbursement, 0) as disbursement_amount, 
          (
            COALESCE(mls.principal_paid, 0) + COALESCE(mls.fee_paid, 0) + COALESCE(mls.interest_paid, 0)
          ) AS repayment,
          COALESCE(mls.disbursement, 0) - COALESCE(mls.principal_paid, 0) as disbursement_balance,
          COALESCE(mls.principal_paid, 0) as principal_paid, 
          COALESCE(mls.principal_balance, 0) as principal_balance, 
          COALESCE(mls.fee_paid, 0) as fee_paid, 
          COALESCE(mls.fee_balance, 0) as fee_balance, 
          COALESCE(mls.interest_paid, 0) as interest_paid, 
          COALESCE(mls.interest_balance, 0) as interest_balance, 
          COALESCE(mls.interest_accrued, 0) as interest_accrued, 
          COALESCE(mls.fee_accrued, 0) as fee_accrued, 
          COALESCE(mls.other_fees, 0) as other_fees, 
          COALESCE(mls.interest_amount, 0) as interest_amount, 
          COALESCE(mls.interest_pct, 0) as interest_pct, 
          pl.id as correlation_column, 
          'p' as relation_type
        FROM 
          participant_loans pl 
          join base_multi_loan_summary mls on pl.borrower_id = mls.borrower_id 
          AND pl.original_pk = mls.loan_number_id 
          AND mls.loan_type = (
            case when pl.loan_type = 'MCL' then 'NABL' else pl.loan_type end
          ) 
          inner join participant_loan_settings pls on pl.id = pls.loan_id 
          AND pl.borrower_id = pls.client_id
          LEFT JOIN base_affiliate_lenders al on al.id = pls.affiliate_lender
        where 
          pls.loan_deal = 'Sold'
          AND pls.loan_collaboration != "Syndication"
          AND EXISTS(
            SELECT 
              1 
            FROM 
              participant_loan_allocations pla 
            WHERE 
              pla.loan_id = pl.id
          ) 
        UNION 
        SELECT 
          pla.id, 
          pl.loan_name,
          pls.loan_collaboration,
          pla.allocation_number, 
          pp.participant_name, 
          pp.participant_role, 
          pb.client_name, 
          pb.client_number, 
          pl_sum.outstanding_balance, 
          pl_sum.disbursement, 
          (
            pl_sum.principal_paid + pl_sum.fee_paid + pl_sum.interest_paid
          ),
          pl_sum.disbursement - pl_sum.principal_paid,
          pl_sum.principal_paid, 
          pl_sum.closing_balance, 
          pl_sum.fee_paid, 
          pl_sum.fee_balance, 
          pl_sum.interest_paid, 
          pl_sum.interest_balance, 
          pl_sum.interest_accrued, 
          pl_sum.fee_accrued, 
          pl_sum.other_fees, 
          pl_sum.interest_amount, 
          pl_sum.interest_pct, 
          pla.loan_id, 
          'c' as relation_type
        FROM 
          participant_loan_allocations pla 
          join participant_loans pl on pla.loan_id = pl.id 
          join participant_parties pp on pla.participation_party_id = pp.id 
          left join participant_loan_summaries pl_sum on pla.id = pl_sum.participant_loan_allocation_id 
          join participant_loan_settings pls on pla.loan_id = pls.loan_id 
          join participant_borrowers pb on pb.id = pls.client_id 
        where 
          pls.loan_deal = 'Sold'
          AND pls.loan_collaboration != "Syndication"
      ) grp 
    ORDER BY 
      correlation_column, 
      relation_type desc;

---------------------------------------------------------------------------------------------------------------------------
Global Loan
---------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
  (SELECT pl.id AS loan_id,
          pl.loan_name,
          pls.loan_collaboration,
          IF (al.name IS NULL,
                         ' ',
                         al.name) AS participant_name,
             pls.role_loan_deal AS participant_role,
             pl.borrower_id AS client_id,
             bb.client_name,
             bh.activity_date,
             COALESCE(bh.disbursement, 0.00000) AS disbursement_amount,
             COALESCE(bh.payments, 0.00000) AS repayment,
             COALESCE(bh.interest, 0.00000) AS interest_amount,
             COALESCE(bh.other_fees, 0.00000) AS other_fees,
             COALESCE(bh.interest, 0.00000)+COALESCE(bh.other_fees, 0.00000) AS total_charge_amount,
             COALESCE(bh.loan_balance, 0.00000) AS outstanding_balance
   FROM participant_loans pl
   JOIN base_loan_bal_hist bh ON pl.original_pk = bh.loan_number_id
   AND IF(pl.loan_type = 'MCL', 'NABL', pl.loan_type) = bh.loan_type
   JOIN participant_loan_settings pls ON pl.id = pls.loan_id
   AND pl.borrower_id = pls.client_id
   JOIN base_borrowers bb ON bb.id=pl.borrower_id
   LEFT JOIN base_affiliate_lenders al ON al.id = pls.affiliate_lender
   WHERE pls.loan_deal = 'Sold'
     AND pls.loan_collaboration != "Syndication"
     AND EXISTS
       (SELECT 1
        FROM participant_loan_allocations pla
        WHERE pla.loan_id = pl.id)
   UNION SELECT loan_id,
                loan_name,
                loan_collaboration,
                participant_name,
                participant_role,
                client_id,
                client_name,
                activity_date,
                COALESCE(disbursement_amount, 0.00000) AS disbursement_amount,
                COALESCE(repayment, 0.00000) AS repayment,
                COALESCE(interest_amount, 0.00000) AS interest_amount,
                COALESCE(other_fees, 0.00000) AS other_fees,
                COALESCE(interest_amount, 0.00000)+COALESCE(other_fees, 0.00000) AS total_charge_amount,
                COALESCE(outstanding_balance, 0.00000) AS outstanding_balance
   FROM
     (SELECT pl.id AS loan_id,
             pl.loan_name,
             pls.loan_collaboration,
             pp.participant_name,
             pp.participant_role,
             pl.borrower_id AS client_id,
             pb.client_name,
             ptd.activity_date,
             COALESCE(SUM(CASE
                              WHEN pct.source_type = 'advance' THEN ptd.charge_amount
                              ELSE 0
                          END), 0) AS disbursement_amount,
             COALESCE(SUM(CASE
                              WHEN pct.source_type = 'collection' THEN ptd.charge_amount
                              ELSE 0
                          END), 0) AS repayment,
             COALESCE(SUM(CASE
                              WHEN pct.natural_sign = '-'
                                   AND pct.add_to_balance = 1
                                   AND (pct.source_type = 'interest'
                                        OR pct.source_type = 'accrued interest') THEN (ptd.charge_amount + ptd.float_amount) * -1
                              WHEN pct.natural_sign = '+'
                                   AND pct.add_to_balance = 1
                                   AND (pct.source_type = 'interest'
                                        OR pct.source_type = 'accrued interest') THEN ptd.charge_amount + ptd.float_amount
                              ELSE 0
                          END), 0) AS interest_amount,
             COALESCE(SUM(CASE
                              WHEN pct.natural_sign = '-'
                                   AND pct.add_to_balance = 1
                                   AND (pct.source_type = 'fees/charges'
                                        OR pct.source_type = 'accrued fees/charges') THEN (ptd.charge_amount + ptd.float_amount) * -1
                              WHEN pct.natural_sign = '+'
                                   AND pct.add_to_balance = 1
                                   AND (pct.source_type = 'fees/charges'
                                        OR pct.source_type = 'accrued fees/charges')THEN ptd.charge_amount + ptd.float_amount
                              ELSE 0
                          END), 0) AS other_fees,
             ptd.outstanding_balance
      FROM participant_loan_allocations pla
      JOIN participant_loans pl ON pl.id=pla.loan_id
      JOIN participant_parties pp ON pla.participation_party_id = pp.id
      JOIN participant_charge_templates pct ON pct.participant_loan_allocation_id=pla.id
      JOIN participant_loan_settings pls ON pla.loan_id = pls.loan_id
      JOIN participant_borrowers pb ON pb.id = pls.client_id
      LEFT JOIN participant_transaction_details ptd ON pla.participation_party_id = ptd.participant_party_id
      AND ptd.participant_loan_allocation_id = pla.id
      AND ptd.participant_charge_template_id=pct.id
      AND pct.source_type IN ('advance',
                              'collection',
                              'fees/charges',
                              'interest',
                              'accrued interest',
                              'accrued fees/charges')
      WHERE pls.loan_deal = 'Sold'
        AND pls.loan_collaboration != "Syndication"
      GROUP BY pl.id ,
             pl.loan_name,
             pls.loan_collaboration,
             pp.participant_name,
             pp.participant_role,
             pl.borrower_id ,
             pb.client_name,
             ptd.activity_date,ptd.outstanding_balance)grp)grp1
ORDER BY loan_id,
         activity_date ASC;

---------------------------------------------------------------------------------------------------------------------------
Delay Statistics
---------------------------------------------------------------------------------------------------------------------------
SELECT participant_name,
       allocation_number,
       CONCAT(allocation_number, ' - ', participant_name) participant_allocation,
       Last_Disbursement_date,
       Datediff(current_Activity_date, COALESCE (Last_Disbursement_date, effective_date_loan_participation)) Disbursement_pending_days,
       COALESCE(net_exp_amount_today, 0)net_settlement_amount_today,
       COALESCE(settlement_amt_as_today, 0)settlement_amt_as_today
FROM (
SELECT pp.participant_name,
       pla.allocation_number,
       pla.id participant_loan_allocation_id,
       CURRENT_DATE AS current_Activity_date,
                       pla.effective_date_loan_participation,
                       pd.activity_date AS Last_Disbursement_date,
                       ns.net_exp_amount AS net_exp_amount_today,
                       ns.net_settlemt_amount_today AS settlement_amt_as_today
FROM participant_loan_allocations pla
JOIN participant_parties pp ON pla.participation_party_id=pp.id
JOIN net_settlement_amount_daily_transaction_details_history ns ON ns.participant_party_id=pla.participation_party_id
AND ns.participant_loan_allocation_id=pla.id
AND ns.Transaction_date = CURRENT_DATE
LEFT JOIN
  (SELECT ptd.participant_party_id,
          ptd.participant_loan_allocation_id,
          max(activity_date) activity_date
   FROM participant_transaction_details ptd
   JOIN participant_charge_templates ct ON ct.id = ptd.participant_charge_template_id
   AND ct.source_type = 'advance'
   GROUP BY participant_party_id,
            ptd.participant_loan_allocation_id) pd ON pd.participant_party_id = pla.participation_party_id
AND pd.participant_loan_allocation_id=pla.id)pp;


---------------------------------------------------------------------------------------------------------------------------
Days Since Last payment and current Outstanding 
---------------------------------------------------------------------------------------------------------------------------
SELECT participant_name,
       allocation_number,
       CONCAT(allocation_number, " - ", participant_name) AS participant_allocation,
       DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) AS date,
       Last_payment_date,
       Datediff(CURRENT_DATE, Last_payment_date) payment_pending_days,
       balance_at_Last_payment_date,
       current_date_outstanding_balance
FROM
  (SELECT pp.id participant_party_id,
          pp.participant_name,
          pla.id participant_loan_allocation_id,
          pla.allocation_number,
          pla.effective_date_loan_participation,
          COALESCE (cd.Last_payment_date,
                    pla.effective_date_loan_participation)Last_payment_date,
                   COALESCE(cd.balance_at_Last_payment_date, 0)balance_at_Last_payment_date,
                   COALESCE(pls.outstanding_balance, 0)current_date_outstanding_balance
   FROM participant_loan_allocations pla
   JOIN participant_loan_summaries pls ON pls.participant_party_id=pla.participation_party_id
   AND pls.participant_loan_allocation_id=pla.id
   JOIN participant_parties pp ON pla.participation_party_id=pp.id
   LEFT JOIN
     (SELECT ptd.participant_party_id,
             ptd.participant_loan_allocation_id,
             ptd.activity_date AS Last_payment_date,
             ptd.outstanding_balance AS balance_at_Last_payment_date
      FROM participant_transaction_details ptd
      WHERE ptd.id IN
          (SELECT max(ptd.id) AS transaction_id
           FROM participant_transaction_details ptd
           JOIN participant_charge_templates pct ON pct.id = ptd.participant_charge_template_id
           AND pct.source_type = 'collection'
           GROUP BY ptd.participant_party_id,
                    ptd.participant_loan_allocation_id)) cd ON pla.participation_party_id =cd.participant_party_id
   AND pla.id =cd.participant_loan_allocation_id order by pp.id,pla.id) grp ;

---------------------------------------------------------------------------------------------------------------------------
Number Of Participants grouped on "Days Since Last payment" buckets
---------------------------------------------------------------------------------------------------------------------------
SELECT participant_name,
       allocation_number,
       CONCAT(allocation_number, " - ", participant_name) AS participant_allocation,
       DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) AS date,
       Last_payment_date,
       Datediff(CURRENT_DATE, Last_payment_date) payment_pending_days,
       balance_at_Last_payment_date,
       current_date_outstanding_balance
FROM
  (SELECT pp.id participant_party_id,
          pp.participant_name,
          pla.id participant_loan_allocation_id,
          pla.allocation_number,
          pla.effective_date_loan_participation,
          COALESCE (cd.Last_payment_date,
                    pla.effective_date_loan_participation)Last_payment_date,
                   COALESCE(cd.balance_at_Last_payment_date, 0)balance_at_Last_payment_date,
                   COALESCE(pls.outstanding_balance, 0)current_date_outstanding_balance
   FROM participant_loan_allocations pla
   JOIN participant_loan_summaries pls ON pls.participant_party_id=pla.participation_party_id
   AND pls.participant_loan_allocation_id=pla.id
   JOIN participant_parties pp ON pla.participation_party_id=pp.id
   LEFT JOIN
     (SELECT ptd.participant_party_id,
             ptd.participant_loan_allocation_id,
             ptd.activity_date AS Last_payment_date,
             ptd.outstanding_balance AS balance_at_Last_payment_date
      FROM participant_transaction_details ptd
      WHERE ptd.id IN
          (SELECT max(ptd.id) AS transaction_id
           FROM participant_transaction_details ptd
           JOIN participant_charge_templates pct ON pct.id = ptd.participant_charge_template_id
           AND pct.source_type = 'collection'
           GROUP BY ptd.participant_party_id,
                    ptd.participant_loan_allocation_id)) cd ON pla.participation_party_id =cd.participant_party_id
   AND pla.id =cd.participant_loan_allocation_id order by pp.id,pla.id) grp ;
   
   
======================================================================================================================================================
  