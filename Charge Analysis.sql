>>>>Charge Analysis

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