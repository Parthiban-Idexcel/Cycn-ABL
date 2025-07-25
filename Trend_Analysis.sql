>>Trend Analysis:

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
          sum(grp1.outstanding_balance) as outstanding_balance,
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
   JOIN (SELECT activity_date,
                                     participation_party_id,
                                     participant_loan_allocation_id,
                                     closing_balance as outstanding_balance,
                                     @rownum :=(CASE
                                                     WHEN@pp_id = participation_party_id
                                                         AND @pla_id = participant_loan_allocation_id
                                                         AND @activity_date = activity_date
                                                         AND @seq = sequence
                                                         AND @id = id  THEN
                                                                @rownum

                                                     WHEN @pp_id = participation_party_id
                                                          AND @pla_id = participant_loan_allocation_id
                                                          AND @activity_date = activity_date
                                                          AND @seq = sequence
                                                          AND (@id :=id) IS NOT NULL THEN
                                                              @rownum+1

                                                     WHEN @pp_id = participation_party_id
                                                           AND @pla_id = participant_loan_allocation_id
                                                           AND @activity_date = activity_date
                                                           AND (@seq :=sequence) IS NOT NULL
                                                           AND (@id :=id) IS NOT NULL THEN
                                                                @rownum+1

                                                     WHEN @pp_id = participation_party_id
                                                          AND @pla_id=participant_loan_allocation_id
                                                          AND (@activity_date := activity_date) IS NOT NULL
                                                          AND (@seq :=sequence) IS NOT NULL
                                                          AND (@id :=id) IS NOT NULL THEN
                                                        @rownum+1

                                                    WHEN (@pp_id := participation_party_id) IS NOT NULL
                                                         AND (@pla_id := participant_loan_allocation_id) IS NOT NULL
                                                         AND (@activity_date := activity_date) IS NOT NULL THEN
                                                        1
                                                      END) as rnk
                                  FROM (SELECT td.id,
                                               COALESCE(ct.sequence,0) sequence,
                                               td.activity_date,
                                               la.participation_party_id,
                                               la.id participant_loan_allocation_id,
                                               td.outstanding_balance closing_balance,
                                               ct.source_type
                                        FROM participant_loan_allocations la
                                             JOIN participant_charge_templates ct ON  ct.participant_loan_allocation_id=la.id
                                                  AND (ct.add_to_balance = 1 OR (ct.add_to_balance = 0 AND ct.accrued_to_loan = 0))
                                            left Join  participant_transaction_details td ON la.id=td.participant_loan_allocation_id
                                                   AND td.participant_party_id= la.participation_party_id
                                                   AND td.participant_charge_template_id=ct.id
                                                   AND td.activity_date between DATE_SUB(CURRENT_DATE, INTERVAL 365 DAY) and CURRENT_DATE
                                        
                                        ORDER BY la.participation_party_id,la.id,
                                                 td.activity_date desc,COALESCE(ct.sequence,0) desc,td.id desc) grp
        JOIN (SELECT @rownum :=0,@pp_id :=NULL,@pla_id :=NULL,@activity_date :=NULL,@seq :=NULL,@id :=NUll) r)grp1 on grp1.participation_party_id= pp.id and grp1.rnk=1
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