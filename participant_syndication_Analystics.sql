-- Total Number of Active Loans

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


-- Total Number of participants

SELECT 
  SUM(CASE WHEN is_active = 1  and participant_role in ('Lead Lender','Participant') THEN 1 ELSE 0 END) AS active_count,
  SUM(CASE WHEN is_active = 0  and participant_role in ('Participant') THEN 1 ELSE 0 END) AS inactive_count
FROM participant_parties 
WHERE deleted_at IS NULL;