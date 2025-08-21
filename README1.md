# material

-- 1. Base meeting data
WITH Meeting_Prep AS (
  SELECT
    customer_number,
    agent_type_2,
    meeting_type,
    appointment_id,
    appointment_date
  FROM F_HEALTH_PROD.RMP_Appointments
  WHERE meeting_type IN (
    '01 Individual review',
    '02 Discovery meeting',
    '03 Recommendation meeting',
    '04 Annual review',
    '05 Ongoing Advice Annual Review'
  )
    AND appointment_status_code IN ('Completed', 'Open')
    AND agent_type_2 NOT IN ('Specialist Team Manager', 'AWM')
    AND appointment_date <= DATE '2025-07-23'
),

-- 2. Assign 'role type' as in your code1 suggestion
Enhanced_Meetings AS (
  SELECT
    *,
    CASE 
      WHEN agent_type_2 NOT IN ('PCPM', 'IPM') THEN 'Advisor'
      ELSE 'Referrer'
    END AS role_type
  FROM Meeting_Prep
),

-- 3. Create links between meetings for journey mapping
Linked_Meetings AS (
  SELECT
    A.customer_number,
    A.appointment_id,
    A.meeting_type,
    A.appointment_date,
    A.role_type,
    B.appointment_id AS linked_appointment_id,
    B.meeting_type AS linked_meeting_type,
    B.appointment_date AS linked_appointment_date,
    B.role_type AS linked_role_type,
    DATE_DIFF(B.appointment_date, A.appointment_date, DAY) AS date_difference
  FROM Enhanced_Meetings A
  JOIN Enhanced_Meetings B
    ON A.customer_number = B.customer_number
   AND B.appointment_date > A.appointment_date
   AND DATE_DIFF(B.appointment_date, A.appointment_date, DAY) <= 92  -- 92-day window
),

-- 4. Only link specific meeting types as per mapping logic: ideally ind review >= annual review > discovery > recommend
Journey_Links AS (
  SELECT
    L.customer_number,
    L.appointment_id,
    L.meeting_type,
    L.appointment_date,
    L.role_type,
    L.linked_appointment_id,
    L.linked_meeting_type,
    L.linked_appointment_date,
    L.linked_role_type,
    L.date_difference
  FROM Linked_Meetings L
  WHERE (
    (L.meeting_type = '01 Individual review'     AND L.linked_meeting_type IN ('04 Annual review', '05 Ongoing Advice Annual Review', '02 Discovery meeting', '03 Recommendation meeting'))
    OR
    (L.meeting_type IN ('04 Annual review', '05 Ongoing Advice Annual Review') AND L.linked_meeting_type IN ('02 Discovery meeting', '03 Recommendation meeting'))
    OR
    (L.meeting_type = '02 Discovery meeting'     AND L.linked_meeting_type = '03 Recommendation meeting')
  )
),

-- 5. Rank journeys for each customer to get sequences and not duplicate meetings
Journey_Ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY customer_number ORDER BY appointment_date) AS journey_seq_num,
    ROW_NUMBER() OVER (PARTITION BY customer_number, linked_appointment_id ORDER BY appointment_date) AS journey_seq_num_linked
  FROM Journey_Links
),

-- 6. Summarize journeys and assign journey ids
Journeys AS (
  SELECT
    customer_number,
    journey_seq_num,
    MIN(appointment_date) AS journey_start_date,
    MAX(linked_appointment_date) AS journey_end_date,
    MIN(CASE WHEN meeting_type = '01 Individual review' THEN appointment_date END) AS FIRST_IND_RV,
    MAX(CASE WHEN meeting_type = '01 Individual review' THEN appointment_date END) AS LAST_IND_RV,
    COUNT(CASE WHEN meeting_type = '01 Individual review' THEN 1 END) AS No_of_Ind_RV,
    MIN(CASE WHEN meeting_type IN ('04 Annual review', '05 Ongoing Advice Annual Review') THEN appointment_date END) AS FIRST_ANR,
    MAX(CASE WHEN meeting_type IN ('04 Annual review', '05 Ongoing Advice Annual Review') THEN appointment_date END) AS LAST_ANR,
    COUNT(CASE WHEN meeting_type IN ('04 Annual review', '05 Ongoing Advice Annual Review') THEN 1 END) AS No_of_ANR,
    MIN(CASE WHEN meeting_type = '02 Discovery meeting' THEN appointment_date END) AS FIRST_DISCOVERY,
    MAX(CASE WHEN meeting_type = '02 Discovery meeting' THEN appointment_date END) AS LAST_DISCOVERY,
    COUNT(CASE WHEN meeting_type = '02 Discovery meeting' THEN 1 END) AS No_of_DISCOVERY,
    MIN(CASE WHEN meeting_type = '03 Recommendation meeting' THEN appointment_date END) AS FIRST_REC,
    MAX(CASE WHEN meeting_type = '03 Recommendation meeting' THEN appointment_date END) AS LAST_REC,
    COUNT(CASE WHEN meeting_type = '03 Recommendation meeting' THEN 1 END) AS No_of_REC
  FROM Journey_Ranked
  GROUP BY customer_number, journey_seq_num
),

-- 7. Map in fee data per journey, as in code2 logic
Fees_Matched AS (
  SELECT
    fd.CIN1 AS matched_cin,
    fd.invoice_date,
    fd.case_no,
    fd.agent_type_2,
    fd.fee_discription,
    fd.fee_discription_2,
    fd.`wealth wizards`,
    fd.centralaccount,
    fd.lvl4code,
    j.customer_number,
    j.journey_seq_num,
    j.FIRST_REC
  FROM fees_data fd
  JOIN Journeys j
    ON fd.CIN1 = j.customer_number
   AND j.FIRST_REC IS NOT NULL
   AND PARSE_DATE('%d/%m/%Y', fd.invoice_date) BETWEEN j.FIRST_REC AND DATE_ADD(j.FIRST_REC, INTERVAL 3 MONTH)
  UNION ALL
  SELECT
    fd.CIN2 AS matched_cin,
    fd.invoice_date,
    fd.case_no,
    fd.agent_type_2,
    fd.fee_discription,
    fd.fee_discription_2,
    fd.`wealth wizards`,
    fd.centralaccount,
    fd.lvl4code,
    j.customer_number,
    j.journey_seq_num,
    j.FIRST_REC
  FROM fees_data fd
  JOIN Journeys j
    ON fd.CIN2 = j.customer_number
   AND j.FIRST_REC IS NOT NULL
   AND PARSE_DATE('%d/%m/%Y', fd.invoice_date) BETWEEN j.FIRST_REC AND DATE_ADD(j.FIRST_REC, INTERVAL 3 MONTH)
),

Fees_Final AS (
  SELECT
    customer_number,
    journey_seq_num,
    invoice_date,
    agent_type_2,
    fee_discription,
    fee_discription_2,
    `wealth wizards`,
    centralaccount,
    lvl4code,
    case_no
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY customer_number, journey_seq_num ORDER BY case_no ASC) AS rn
    FROM Fees_Matched
  )
  WHERE rn = 1
)

-- Final output: multiple journeys per customer, each with summary columns and fee data
SELECT
  j.*,
  ff.invoice_date,
  ff.case_no,
  ff.agent_type_2 AS fees_agent_type_2,
  ff.fee_discription,
  ff.fee_discription_2,
  ff.`wealth wizards`,
  ff.centralaccount,
  ff.lvl4code
FROM Journeys j
LEFT JOIN Fees_Final ff
  ON j.customer_number = ff.customer_number AND j.journey_seq_num = ff.journey_seq_num
ORDER BY j.customer_number, j.journey_seq_num
