#### material ###

WITH Meeting_Prep AS (
    SELECT
        customer_number,
        agent_type_2,
        meeting_type,
        appointment_date
    FROM DS_UKP_WPB_DNA_F_HEALTH_PROD.RMP_appointments
    WHERE Agent_Type_2 NOT IN ('Specialist Team Manager', 'AWM')
      AND Appointment_Status_Code IN ('Completed', 'Open')
      AND Appointment_Date <= DATE '2025-05-22'
      AND meeting_type IN (
            '01 Individual review',
            '04 Annual review',
            '05 Ongoing advice Annual Review',
            '02 Discovery meeting',
            '03 Recommendation meeting'
      )
),
Meeting_Timeline AS (
    SELECT
        customer_number,
        agent_type_2,
        meeting_type,
        appointment_date,
        ROW_NUMBER() OVER (PARTITION BY customer_number ORDER BY appointment_date) as meeting_sequence,
        LAG(appointment_date) OVER (PARTITION BY customer_number ORDER BY appointment_date) as prev_meeting_date,
        LAG(meeting_type) OVER (PARTITION BY customer_number ORDER BY appointment_date) as prev_meeting_type
    FROM Meeting_Prep
),
Journey_Breakpoints AS (
    SELECT
        customer_number,
        agent_type_2,
        meeting_type,
        appointment_date,
        meeting_sequence,
        prev_meeting_date,
        prev_meeting_type,
        CASE 
            WHEN meeting_sequence = 1 THEN 1  -- First meeting always starts journey 1
            WHEN prev_meeting_date IS NULL THEN 1
            WHEN meeting_type IN ('01 Individual review', '04 Annual review', '05 Ongoing advice Annual Review', '02 Discovery meeting')
                 AND prev_meeting_type IN ('01 Individual review', '04 Annual review', '05 Ongoing advice Annual Review', '02 Discovery meeting')
                 AND DATE_DIFF(appointment_date, prev_meeting_date, DAY) > 90 THEN 1  -- New journey if gap > 3 months
            ELSE 0  -- Continue current journey
        END as is_journey_start
    FROM Meeting_Timeline
),
Journey_Numbers AS (
    SELECT
        customer_number,
        agent_type_2,
        meeting_type,
        appointment_date,
        meeting_sequence,
        is_journey_start,
        SUM(is_journey_start) OVER (
            PARTITION BY customer_number 
            ORDER BY appointment_date 
            ROWS UNBOUNDED PRECEDING
        ) as journey_number
    FROM Journey_Breakpoints
),
Journey_Start_Points AS (
    SELECT
        customer_number,
        journey_number,
        MIN(appointment_date) AS journey_start_date
    FROM Journey_Numbers
    WHERE meeting_type IN ('01 Individual review', '04 Annual review', '05 Ongoing advice Annual Review', '02 Discovery meeting')
    GROUP BY customer_number, journey_number
),
Discovery_Journey_Info AS (
    SELECT DISTINCT
        customer_number,
        journey_number,
        MIN(CASE WHEN meeting_type = '02 Discovery meeting' THEN appointment_date END) as discovery_date
    FROM Journey_Numbers
    WHERE meeting_type = '02 Discovery meeting'
    GROUP BY customer_number, journey_number
),
Valid_Rec_Meetings_Journey AS (
    SELECT DISTINCT
        jn.customer_number,
        jn.journey_number,
        jn.appointment_date AS rec_date,
        jn.agent_type_2,
        jsp.journey_start_date,
        dji.discovery_date
    FROM Journey_Numbers jn
    JOIN Journey_Start_Points jsp 
        ON jn.customer_number = jsp.customer_number 
        AND jn.journey_number = jsp.journey_number
    LEFT JOIN Discovery_Journey_Info dji 
        ON jn.customer_number = dji.customer_number 
        AND jn.journey_number = dji.journey_number
    WHERE jn.meeting_type = '03 Recommendation meeting'
      AND (
        -- Rule 2: If discovery meeting exists, recommendation within 6 months
        (dji.discovery_date IS NOT NULL 
         AND jn.appointment_date BETWEEN dji.discovery_date AND DATE_ADD(dji.discovery_date, INTERVAL 6 MONTH))
        OR
        -- Rule 3: If no discovery meeting, recommendation within 3 months of journey start
        (dji.discovery_date IS NULL 
         AND jn.appointment_date BETWEEN jsp.journey_start_date AND DATE_ADD(jsp.journey_start_date, INTERVAL 3 MONTH))
      )
),
Journey_Aggregates AS (
    SELECT
        jn.customer_number AS CIN,
        jn.journey_number,
        -- Individual Review Metrics (Non-Advisory)
        MIN(CASE WHEN meeting_type = '01 Individual review' AND agent_type_2 NOT IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
            THEN appointment_date END) AS FIRST_IND_RV,
        MAX(CASE WHEN meeting_type = '01 Individual review' AND agent_type_2 NOT IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
            THEN appointment_date END) AS LAST_IND_RV,
        COUNT(CASE WHEN meeting_type = '01 Individual review' AND agent_type_2 NOT IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
            THEN 1 END) AS NO_OF_IND_RV,
        
        -- Individual Review Metrics (Advisory)
        MIN(CASE WHEN meeting_type = '01 Individual review' AND agent_type_2 IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
            THEN appointment_date END) AS FIRST_ADV_IND_RV,
        MAX(CASE WHEN meeting_type = '01 Individual review' AND agent_type_2 IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
            THEN appointment_date END) AS LAST_ADV_IND_RV,
        COUNT(CASE WHEN meeting_type = '01 Individual review' AND agent_type_2 IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
            THEN 1 END) AS NO_OF_ADV_IND_RV,
        
        -- Discovery Meeting Metrics
        MIN(CASE WHEN meeting_type = '02 Discovery meeting' AND agent_type_2 IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
            THEN appointment_date END) AS FIRST_ADV_DIS,
        MAX(CASE WHEN meeting_type = '02 Discovery meeting' AND agent_type_2 IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
            THEN appointment_date END) AS LAST_ADV_DIS,
        COUNT(CASE WHEN meeting_type = '02 Discovery meeting' AND agent_type_2 IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
            THEN 1 END) AS NO_OF_ADV_DIS,
        
        -- Recommendation Meeting Metrics
        MIN(CASE WHEN meeting_type = '03 Recommendation meeting' AND agent_type_2 IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
                  AND appointment_date IN (SELECT rec_date FROM Valid_Rec_Meetings_Journey vrm 
                                          WHERE vrm.customer_number = jn.customer_number 
                                          AND vrm.journey_number = jn.journey_number)
            THEN appointment_date END) AS FIRST_adv_rec,
        MAX(CASE WHEN meeting_type = '03 Recommendation meeting' AND agent_type_2 IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
                  AND appointment_date IN (SELECT rec_date FROM Valid_Rec_Meetings_Journey vrm 
                                          WHERE vrm.customer_number = jn.customer_number 
                                          AND vrm.journey_number = jn.journey_number)
            THEN appointment_date END) AS LAST_adv_rec,
        COUNT(CASE WHEN meeting_type = '03 Recommendation meeting' AND agent_type_2 IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
                   AND appointment_date IN (SELECT rec_date FROM Valid_Rec_Meetings_Journey vrm 
                                           WHERE vrm.customer_number = jn.customer_number 
                                           AND vrm.journey_number = jn.journey_number)
            THEN 1 END) AS NO_OF_adv_rec,
        
        -- Annual Review Metrics
        MIN(CASE WHEN meeting_type IN ('04 Annual review','05 Ongoing advice Annual Review') AND agent_type_2 IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
            THEN appointment_date END) AS FIRST_ADV_ANR,
        MAX(CASE WHEN meeting_type IN ('04 Annual review','05 Ongoing advice Annual Review') AND agent_type_2 IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
            THEN appointment_date END) AS LAST_ADV_ANR,
        COUNT(CASE WHEN meeting_type IN ('04 Annual review','05 Ongoing advice Annual Review') AND agent_type_2 IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
            THEN 1 END) AS NO_OF_ADV_ANR
    FROM Journey_Numbers jn
    GROUP BY jn.customer_number, jn.journey_number
),
First_RM_Type_Journey AS (
    SELECT DISTINCT
        jn.customer_number,
        jn.journey_number,
        FIRST_VALUE(agent_type_2) OVER (
            PARTITION BY customer_number, journey_number 
            ORDER BY appointment_date ASC
        ) AS FIRST_RM_TYPE
    FROM Journey_Numbers jn
    WHERE jn.meeting_type = '01 Individual review'
        AND jn.agent_type_2 NOT IN ('WA','WM','WM HNW','WM HNW PB')
),
Final_RMP_Journey AS (
    SELECT
        ja.*,
        frt.FIRST_RM_TYPE,
        CONCAT('Journey_', CAST(ja.journey_number AS STRING)) as journey_id
    FROM Journey_Aggregates ja
    LEFT JOIN First_RM_Type_Journey frt
        ON ja.CIN = frt.customer_number 
        AND ja.journey_number = frt.journey_number
),
Fees_Journey AS (
    SELECT
        fd.CIN1 as matched_cin,
        fd.invoice_date,
        fd.case_no,
        fd.agent_type_2,
        fd.fee_description,
        fd.wealth_wizards_case,
        fd.central_Account,
        fd.Lv1_4_code,
        vrm.journey_number
    FROM DS_UKP_WPB_DNA_F_HEALTH_PROD.FEES_Data_Import fd
    JOIN Valid_Rec_Meetings_Journey vrm
        ON SAFE_CAST(fd.CIN1 AS INT64) = vrm.customer_number
        AND PARSE_DATE('%d/%m/%y', fd.invoice_date) BETWEEN vrm.rec_date AND DATE_ADD(vrm.rec_date, INTERVAL 3 MONTH)
    UNION ALL
    SELECT
        fd.CIN2 as matched_cin,
        fd.invoice_date,
        fd.case_no,
        fd.agent_type_2,
        fd.fee_description,
        fd.wealth_wizards_case,
        fd.central_Account,
        fd.Lv1_4_code,
        vrm.journey_number
    FROM DS_UKP_WPB_DNA_F_HEALTH_PROD.FEES_Data_Import fd
    JOIN Valid_Rec_Meetings_Journey vrm
        ON SAFE_CAST(fd.CIN2 AS INT64) = vrm.customer_number
        AND PARSE_DATE('%d/%m/%y', fd.invoice_date) BETWEEN vrm.rec_date AND DATE_ADD(vrm.rec_date, INTERVAL 3 MONTH)
),
Fees_Final_Journey AS (
    SELECT
        matched_cin,
        journey_number,
        invoice_date,
        fee_description,
        fee_description2,
        wealth_wizards_case,
        central_Account,
        Lv1_4_code,
        case_no,
        agent_type_2
    FROM (
        SELECT
            matched_cin,
            journey_number,
            invoice_date,
            fee_description,
            fee_description2,
            wealth_wizards_case,
            central_Account,
            Lv1_4_code,
            case_no,
            agent_type_2,
            ROW_NUMBER() OVER (
                PARTITION BY matched_cin, journey_number
                ORDER BY case_no ASC
            ) as rn
        FROM Fees_Journey
    )
    WHERE rn = 1
)
SELECT 
    frmp.FIRST_RM_TYPE,
    frmp.CIN,
    frmp.journey_number,
    frmp.journey_id,
    frmp.FIRST_IND_RV,
    frmp.LAST_IND_RV,
    frmp.NO_OF_IND_RV,
    frmp.FIRST_ADV_IND_RV,
    frmp.LAST_ADV_IND_RV,
    frmp.NO_OF_ADV_IND_RV,
    frmp.FIRST_ADV_DIS,
    frmp.LAST_ADV_DIS,
    frmp.NO_OF_ADV_DIS,
    frmp.FIRST_adv_rec,
    frmp.LAST_adv_rec,
    frmp.NO_OF_adv_rec,
    frmp.FIRST_ADV_ANR,
    frmp.LAST_ADV_ANR,
    frmp.NO_OF_ADV_ANR,
    ff.invoice_date,
    ff.case_no,
    ff.agent_type_2 AS Fees_agent_type,
    ff.fee_description,
    ff.fee_description2,
    ff.wealth_wizards_case,
    ff.central_Account,
    ff.Lv1_4_code
FROM Final_RMP_Journey frmp
LEFT JOIN Fees_Final_Journey ff
    ON frmp.CIN = SAFE_CAST(ff.matched_cin AS INT64)
    AND frmp.journey_number = ff.journey_number
ORDER BY frmp.CIN, frmp.journey_number;
