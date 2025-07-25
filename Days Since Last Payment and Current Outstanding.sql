>>>Days Since Last Payment and Current Outstanding

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
   JOIN participant_parties pp ON pla.participation_party_id=pp.id and pp.deleted_at IS NULL and pp.is_active=1
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