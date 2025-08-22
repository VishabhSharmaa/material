#material

-- Step 1: Prepare and order meetings
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
    AND appointment_date IS NOT NULL
    AND appointment_date <= DATE '2025-07-23'
),

Ordered_Meetings AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY customer_number ORDER BY appointment_date) AS rn
  FROM Meeting_Prep
),

-- Step 2: Assign journey block number using your exact rules.
Block_With_Intervals AS (
  SELECT *,
    -- For partitioning, collect previous important meetings for comparison for interval decisions:
    LAG(appointment_date) OVER (PARTITION BY customer_number ORDER BY appointment_date) AS prev_dt,
    LAG(meeting_type) OVER (PARTITION BY customer_number ORDER BY appointment_date) AS prev_type,
    -- Is this new journey? (1 = yes)
    CASE
      WHEN LAG(appointment_date) OVER (PARTITION BY customer_number ORDER BY appointment_date) IS NULL
        THEN 1
      WHEN DATE_DIFF(appointment_date, LAG(appointment_date) OVER (PARTITION BY customer_number ORDER BY appointment_date), day) > 92 -- >3 months gap
        THEN 1
      WHEN meeting_type IN ('01 Individual review', '04 Annual review', '05 Ongoing Advice Annual Review', '02 Discovery meeting')
           AND DATE_DIFF(appointment_date, LAG(appointment_date) OVER (PARTITION BY customer_number ORDER BY appointment_date), day) > 92
        THEN 1
      ELSE 0
    END AS is_new_journey
  FROM Ordered_Meetings
),

Journey_Chains AS (
  -- Assign a journey_id per customer using cumulative sum of "is_new_journey"
  SELECT *,
    SUM(is_new_journey) OVER (PARTITION BY customer_number ORDER BY appointment_date) AS journey_id
  FROM Block_With_Intervals
),

-- Gather meeting blocks per customer/journey
Journey_Blocks AS (
  SELECT
    customer_number,
    journey_id,
    ARRAY_AGG(meeting_type ORDER BY appointment_date) AS meeting_types,
    ARRAY_AGG(appointment_date ORDER BY appointment_date) AS meeting_dates,
    MIN(appointment_date) AS journey_start_date,
    MAX(appointment_date) AS journey_end_date
  FROM Journey_Chains
  GROUP BY customer_number, journey_id
),

-- Step 3: For each journey block, find discovery & first recommend & validate by rec timing
Block_With_Rec AS (
  SELECT
    jb.*,
    ARRAY(
      SELECT appointment_date FROM UNNEST(jb.meeting_types) AS mt
      WITH OFFSET idx
      WHERE mt = '02 Discovery meeting'
    ) AS discoveries,
    ARRAY(
      SELECT appointment_date FROM UNNEST(jb.meeting_types) AS mt
      WITH OFFSET idx
      WHERE mt = '03 Recommendation meeting'
    ) AS recs
  FROM Journey_Blocks jb
),

Key_Journey_Events AS (
  SELECT
    customer_number,
    journey_id,
    journey_start_date,
    journey_end_date,
    ARRAY_LENGTH(discoveries) AS n_discovery,
    ARRAY_LENGTH(recs) AS n_rec,
    SAFE_OFFSET(discoveries, 0) AS discovery_date,
    SAFE_OFFSET(recs, 0) AS first_rec_date,  -- first rec meeting in block
    meeting_dates,
    meeting_types,
    discoveries,
    recs
  FROM Block_With_Rec
),

-- Step 4: Filter journeys to only those with correct recommend intervals
Valid_Journeys AS (
  SELECT
    *,
    CASE
      WHEN n_discovery > 0
        AND first_rec_date IS NOT NULL
        AND DATE_DIFF(first_rec_date, discovery_date, day) <= 183 -- 6 months window
        THEN 1
      WHEN n_discovery = 0
        AND first_rec_date IS NOT NULL
        AND DATE_DIFF(first_rec_date, journey_start_date, day) <= 92 -- 3 months window
        THEN 1
      ELSE 0
    END AS valid_recommend_in_window
  FROM Key_Journey_Events
  WHERE
    -- Only journeys with at least one valid rec in window, or journeys with no rec at all (if you wish to keep those)
    (
      (n_rec > 0 AND valid_recommend_in_window = 1)
      OR (n_rec = 0) -- If you want to also keep journeys with no recommendation at all, otherwise remove this line
    )
),

-- Step 5: Attach fee data ONLY if invoice is within 3 months of recommend meeting
Fees_Link AS (
  SELECT
    ff.*,
    vj.customer_number,
    vj.journey_id,
    vj.first_rec_date
  FROM fees_data ff
  JOIN Valid_Journeys vj
    ON (CAST(ff.CIN1 AS STRING) = vj.customer_number OR CAST(ff.CIN2 AS STRING) = vj.customer_number)
   AND vj.first_rec_date IS NOT NULL
   AND PARSE_DATE('%d/%m/%Y', ff.invoice_date) BETWEEN vj.first_rec_date AND DATE_ADD(vj.first_rec_date, INTERVAL 3 MONTH)
),

Fees_Final AS (
  SELECT
    customer_number,
    journey_id,
    invoice_date,
    case_no, agent_type_2, fee_discription, fee_discription_2,
    `wealth wizards`, centralaccount, lvl4code,
    ROW_NUMBER() OVER (PARTITION BY customer_number, journey_id ORDER BY case_no ASC) AS rn
  FROM Fees_Link
)

-- FINAL OUTPUT 
SELECT
  vj.customer_number,
  vj.journey_id,
  vj.journey_start_date,
  vj.journey_end_date,
  vj.discovery_date,
  vj.first_rec_date,
  vj.meeting_types,
  vj.meeting_dates,
  ff.invoice_date, ff.case_no, ff.agent_type_2, ff.fee_discription, ff.fee_discription_2, ff.`wealth wizards`, ff.centralaccount, ff.lvl4code
FROM Valid_Journeys vj
LEFT JOIN Fees_Final ff
  ON vj.customer_number = ff.customer_number
 AND vj.journey_id = ff.journey_id
 AND ff.rn=1
ORDER BY vj.customer_number, vj.journey_id

