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

Start_point AS (
    SELECT
        customer_number,
        MIN(appointment_date) AS start_date
    FROM Meeting_Prep
    WHERE meeting_type IN ('01 Individual review', '04 Annual review', '05 Ongoing advice Annual Review', '02 Discovery meeting')
    GROUP BY customer_number
),

Discovery_customers AS (
    SELECT DISTINCT customer_number
    FROM Meeting_Prep
    WHERE meeting_type = '02 Discovery meeting'
),

Discovery_meetings AS (
    SELECT
        customer_number,
        appointment_date AS discovery_date
    FROM Meeting_Prep
    WHERE meeting_type = '02 Discovery meeting'
),

Valid_Rec_Meetings AS (
    SELECT DISTINCT
        mp.customer_number,
        mp.appointment_date
As Rec_date
    FROM Meeting_Prep mp
    JOIN Start_point sp
        ON mp.customer_number = sp.customer_number
    LEFT JOIN Discovery_customers dc
        ON mp.customer_number = dc.customer_number
    WHERE mp.meeting_type = '03 Recommendation meeting'
      AND (
        (dc.customer_number IS NOT NULL AND EXISTS(
            SELECT 1 FROM Discovery_meetings d
            WHERE d.customer_number = mp.customer_number
              AND mp.appointment_date BETWEEN d.discovery_date AND DATE_ADD(d.discovery_date, INTERVAL 6 MONTH)
        ))
        OR
        (dc.customer_number IS NULL
        AND mp.appointment_date BETWEEN sp.start_date AND DATE_ADD(sp.start_date, INTERVAL 9 MONTH))
      )
),

Qualified_CINs AS (
    SELECT DISTINCT
        sp.customer_number
    FROM Start_point sp
    JOIN Meeting_Prep mp
        ON sp.customer_number = mp.customer_number
),

First_RM_Type AS (
    SELECT DISTINCT
        mp.customer_number,
        FIRST_VALUE(agent_type_2) OVER (PARTITION BY customer_number ORDER BY appointment_date ASC) AS FIRST_RM_TYPE
    FROM Meeting_Prep mp
    WHERE mp.meeting_type =
        '01 Individual review'
        AND mp.agent_type_2 not in ('WA','WM','WM HNW','WM HNW PB')
),

Aggregates AS (
    SELECT
        mp.customer_number AS CIN,
        MIN(CASE WHEN meeting_type = '01 Individual review' AND agent_type_2 NOT IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
		THEN appointment_date END) AS FIRST_IND_RV,
		MAX(CASE WHEN meeting_type = '01 Individual review' AND agent_type_2 NOT IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
		THEN appointment_date END) AS LAST_IND_RV,
		COUNT(CASE WHEN meeting_type = '01 Individual review' AND agent_type_2 NOT IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
		THEN 1 END) AS NO_OF_IND_RV,
		
		 MIN(CASE WHEN meeting_type = '01 Individual review' AND agent_type_2  IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
		THEN appointment_date END) AS FIRST_ADV_IND_RV,
		MAX(CASE WHEN meeting_type = '01 Individual review' AND agent_type_2 IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
		THEN appointment_date END) AS LAST_ADV_IND_RV,
		COUNT(CASE WHEN meeting_type = '01 Individual review' AND agent_type_2  IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
		THEN 1 END) AS NO_OF_ADV_IND_RV,
		
		 MIN(CASE WHEN meeting_type = '02 Discovery meeting' AND agent_type_2  IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
		THEN appointment_date END) AS FIRST_ADV_DIS,
		MAX(CASE WHEN meeting_type = '02 Discovery meeting' AND agent_type_2  IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
		THEN appointment_date END) AS LAST_ADV_DIS,
		COUNT(CASE WHEN meeting_type = '02 Discovery meeting' AND agent_type_2  IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
		THEN 1 END) AS NO_OF_ADV_DIS,
		
		 MIN(CASE WHEN meeting_type = '03 Recommendation meeting' AND agent_type_2  IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')AND appointment_date IN(
		 SELECT Rec_date from Valid_rec_meetings VRM where vrm.customer_number = mp.customer_number)
		THEN appointment_date END) AS FIRST_adv_rec,

		MAX(CASE WHEN meeting_type = '03 Recommendation meeting' AND agent_type_2  IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')AND appointment_date IN(
		 SELECT Rec_date from Valid_rec_meetings VRM where vrm.customer_number = mp.customer_number)
		THEN appointment_date END) AS LAST_adv_rec,
		
		COUNT(CASE WHEN meeting_type  = '03 Recommendation meeting' AND agent_type_2  IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')AND appointment_date IN(
		 SELECT Rec_date from Valid_rec_meetings VRM where vrm.customer_number = mp.customer_number)
		THEN 1 END) AS NO_OF_adv_rec,

		 MIN(CASE WHEN meeting_type IN ('04 Annual review','05 Ongoing advice annual review') AND agent_type_2  IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
		THEN appointment_date END) AS FIRST_ADV_ANR,
		MAX(CASE WHEN meeting_type IN ('04 Annual review','05 Ongoing advice annual review') AND agent_type_2  IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
		THEN appointment_date END) AS LAST_ADV_ANR,
		COUNT(CASE WHEN meeting_type IN ('04 Annual review','05 Ongoing advice annual review') AND agent_type_2  IN ('WA', 'WM', 'WM HNW', 'WM HNW PB')
		THEN 1 END) AS NO_OF_ADV_ANR,
    FROM Meeting_Prep mp
	JOIN Qualified_CINs q ON  mp.customer_number =  q.customer_number
    GROUP BY mp.customer_number
),

Final_RMP AS (
    SELECT
        frt.FIRST_RM_TYPE,
        a.*
    FROM Aggregates a
    LEFT JOIN FIRST_RM_Type frt
        ON frt.customer_number = a.CIN
),

Fees AS (
    SELECT
        fd.CIN1 as matched_cin,
        fd.invoice_date,
        fd.case_no,
        fd.agent_type_2,
        fd.fee_description,
        fd.wealth_wizards_case,
        fd.central_Account,
        fd.Lv1_4_code
    FROM DS_UKP_WPB_DNA_F_HEALTH_PROD.FEES_Data_Import fd
    JOIN Valid_Rec_Meetings vrm
        ON SAFE_CAST(fd.CIN1 AS INT64) = vrm.customer_number
        AND PARSE_DATE('%d/%m/%y', fd.invoice_date) BETWEEN vrm.Rec_date AND DATE_ADD(vrm.rec_date, INTERVAL 3 MONTH)

    UNION ALL

    SELECT
        fd.CIN2 as matched_cin,
        fd.invoice_date,
        fd.case_no,
        fd.agent_type_2,
        fd.fee_description,
        fd.wealth_wizards_case,
        fd.central_Account,
        fd.Lv1_4_code
    FROM DS_UKP_WPB_DNA_F_HEALTH_PROD.FEES_Data_Import fd
    JOIN Valid_Rec_Meetings vrm
        ON SAFE_CAST(fd.CIN2 AS INT64) = vrm.customer_number
        AND PARSE_DATE('%d/%m/%y', fd.invoice_date) BETWEEN vrm.Rec_date AND DATE_ADD(vrm.rec_date, INTERVAL 3 MONTH)
),

Fees_Final AS (
    SELECT
        matched_cin,
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
        invoice_date,
        fee_description,
		fee_description2,
        wealth_wizards_case,
        central_Account,
        Lv1_4_code,
		case_no,
        agent_type_2,
            ROW_NUMBER() OVER (
                PARTITION BY matched_cin
                ORDER BY case_no ASC
            ) as rn
        FROM Fees
    )
    WHERE rn = 1
)

SELECT frmp.*,
        ff.invoice_date,
		ff.case_no,
        ff.agent_type_2 AS Fees_agent_type,
        ff.fee_description,
		ff.fee_description2,
        ff.wealth_wizards_case,
        ff.central_Account,
        ff.Lv1_4_code
	
FROM Final_RMP frmp
left JOIN Fees_Final ff
    ON frmp.cin = SAFE_CAST(ff.matched_cin AS INT64) 
    ORDER BY frmp.CIN
	;

