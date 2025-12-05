-- ============================================================================
-- SNOWFLAKE CORTEX AI FOR DEMOGRAPHIC DATA CLEANSING
-- ============================================================================
-- This script demonstrates how to use Snowflake's Cortex AI Functions 
-- to automate cleansing of demographic fields (sex, race, age)
-- Target: 80%+ automation with human review for edge cases
-- ============================================================================

-- Step 0: Setup - Use appropriate role, warehouse, database, and schema
-- ============================================================================
USE ROLE SF_INTELLIGENCE_DEMO;
USE WAREHOUSE APP_WH;
USE DATABASE SANDBOX;

-- Create a dedicated schema for demographic cleansing demo
CREATE SCHEMA IF NOT EXISTS DEMOGRAPHIC_CLEANSING_DEMO;
USE SCHEMA DEMOGRAPHIC_CLEANSING_DEMO;

-- Verify setup
SELECT 
    CURRENT_ROLE() AS current_role,
    CURRENT_WAREHOUSE() AS current_warehouse,
    CURRENT_DATABASE() AS current_database,
    CURRENT_SCHEMA() AS current_schema;

-- Step 1: Create a demo table with messy demographic data
-- ============================================================================
CREATE OR REPLACE TABLE raw_patient_demographics (
    patient_id INT,
    sex VARCHAR,
    race VARCHAR,
    age VARCHAR,
    original_record_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample data with various messy formats
INSERT INTO raw_patient_demographics (patient_id, sex, race, age) VALUES
    -- Clean data
    (1, 'Male', 'White', '45'),
    (2, 'Female', 'Black or African American', '32'),
    
    -- Messy sex values
    (3, 'M', 'Asian', '28'),
    (4, 'F', 'White', '67'),
    (5, 'male', 'Hispanic or Latino', '51'),
    (6, 'FEMALE', 'Native Hawaiian or Pacific Islander', '39'),
    (7, 'Man', 'White', '44'),
    (8, 'Woman', 'Black or African American', '55'),
    (9, 'Boy', 'Asian', '12'),
    (10, 'Girl', 'White', '8'),
    (11, 'm.', 'Hispanic or Latino', '63'),
    (12, 'f.', 'White', '71'),
    
    -- Messy race values
    (13, 'Male', 'Caucasian', '34'),
    (14, 'Female', 'African American', '29'),
    (15, 'Male', 'Black', '41'),
    (16, 'Female', 'AA', '36'),
    (17, 'Male', 'Latino', '27'),
    (18, 'Female', 'Hispanic', '52'),
    (19, 'Male', 'Pacific Islander', '48'),
    (20, 'Female', 'Native American', '31'),
    (21, 'Male', 'American Indian', '59'),
    (22, 'Female', 'Asian/Pacific Islander', '43'),
    (23, 'Male', 'Multi-racial', '38'),
    (24, 'Female', 'Mixed', '25'),
    (25, 'Male', 'Other', '47'),
    
    -- Messy age values
    (26, 'Male', 'White', '45 years'),
    (27, 'Female', 'Asian', '32 yrs'),
    (28, 'Male', 'Black or African American', 'forty-five'),
    (29, 'Female', 'White', '35yo'),
    (30, 'Male', 'Hispanic or Latino', 'Age: 28'),
    (31, 'Female', 'White', 'unknown'),
    (32, 'Male', 'Asian', 'N/A'),
    (33, 'Female', 'White', ''),
    (34, 'Male', 'Black or African American', '150'),  -- Invalid
    (35, 'Female', 'White', '-5'),  -- Invalid
    (36, 'Male', 'Asian', 'infant'),
    
    -- Null/empty values
    (37, NULL, 'White', '45'),
    (38, 'Male', NULL, '32'),
    (39, 'Female', 'Asian', NULL),
    (40, '', 'Black or African American', '28'),
    (41, 'Male', '', '67'),
    
    -- Edge cases
    (42, 'Non-binary', 'White', '33'),
    (43, 'Unknown', 'Prefer not to say', '40'),
    (44, 'Other', 'Unknown', '55'),
    (45, 'X', 'Asian', '29'),
    (46, 'Transgender Male', 'Hispanic or Latino', '36'),
    (47, 'Transgender Female', 'White', '42'),
    (48, 'Prefer not to answer', 'Decline to state', '50');

-- ============================================================================
-- Step 2: Create cleansed table with AI-powered transformations
-- ============================================================================

CREATE OR REPLACE TABLE cleansed_patient_demographics AS
WITH sex_classification AS (
    SELECT 
        patient_id,
        sex AS original_sex,
        race,
        age,
        -- Use AI_CLASSIFY to standardize sex values
        -- Categories based on common healthcare standards (HL7, FHIR)
        SNOWFLAKE.CORTEX.AI_CLASSIFY(
            COALESCE(sex, 'Unknown'),
            ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'label', 'Male',
                    'description', 'Male sex including variations like M, man, boy'
                ),
                OBJECT_CONSTRUCT(
                    'label', 'Female', 
                    'description', 'Female sex including variations like F, woman, girl'
                ),
                OBJECT_CONSTRUCT(
                    'label', 'Other',
                    'description', 'Non-binary, transgender, or other gender identities'
                ),
                OBJECT_CONSTRUCT(
                    'label', 'Unknown',
                    'description', 'Unknown, not specified, null, or prefer not to answer'
                )
            )
        ):labels[0] AS sex_cleansed
    FROM raw_patient_demographics
),
race_classification AS (
    SELECT 
        patient_id,
        original_sex,
        race AS original_race,
        age,
        sex_cleansed,
        -- Use AI_CLASSIFY to standardize race values
        -- Categories based on OMB (Office of Management and Budget) standards
        SNOWFLAKE.CORTEX.AI_CLASSIFY(
            COALESCE(race, 'Unknown'),
            ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'label', 'White',
                    'description', 'White or Caucasian'
                ),
                OBJECT_CONSTRUCT(
                    'label', 'Black or African American',
                    'description', 'Black, African American, or AA'
                ),
                OBJECT_CONSTRUCT(
                    'label', 'Asian',
                    'description', 'Asian including East Asian, South Asian, Southeast Asian'
                ),
                OBJECT_CONSTRUCT(
                    'label', 'Hispanic or Latino',
                    'description', 'Hispanic, Latino, or Latina'
                ),
                OBJECT_CONSTRUCT(
                    'label', 'American Indian or Alaska Native',
                    'description', 'Native American, American Indian, Alaska Native'
                ),
                OBJECT_CONSTRUCT(
                    'label', 'Native Hawaiian or Pacific Islander',
                    'description', 'Pacific Islander, Native Hawaiian'
                ),
                OBJECT_CONSTRUCT(
                    'label', 'Two or More Races',
                    'description', 'Multi-racial, mixed race, or multiple races'
                ),
                OBJECT_CONSTRUCT(
                    'label', 'Other',
                    'description', 'Other race not specified above'
                ),
                OBJECT_CONSTRUCT(
                    'label', 'Unknown',
                    'description', 'Unknown, not specified, prefer not to say, or decline to state'
                )
            )
        ):labels[0] AS race_cleansed
    FROM sex_classification
),
age_cleansing AS (
    SELECT 
        patient_id,
        original_sex,
        original_race,
        age AS original_age,
        sex_cleansed,
        race_cleansed,
        -- Use AI_COMPLETE to extract and validate age
        SNOWFLAKE.CORTEX.AI_COMPLETE(
            'llama3.1-8b',  -- Fast, cost-effective model
            CONCAT(
                'Extract the patient age as a 2 or 3 digit number from this text: "', COALESCE(age, 'unknown'), '"', CHAR(10), CHAR(10),
                'STRICT RULES:', CHAR(10),
                '1. Return ONLY a number (0-120) or the word "INVALID" - no other text, punctuation, or explanation', CHAR(10),
                '2. Valid age range: 0 to 120 years', CHAR(10),
                '3. If age is >120, <0, unclear, or missing: return "INVALID"', CHAR(10), CHAR(10),
                'SPECIAL CASES:', CHAR(10),
                '- Infants/newborns/babies: return 0', CHAR(10),
                '- Age ranges (e.g., "mid-30s", "early 20s"): return midpoint (35, 22)', CHAR(10),
                '- Text numbers (e.g., "forty-five"): convert to digits (45)', CHAR(10),
                '- Units ignored (e.g., "45 years", "32 yrs", "28yo"): extract number only', CHAR(10),
                '- Prefixes ignored (e.g., "Age: 45"): extract number only', CHAR(10), CHAR(10),
                'EXAMPLES:', CHAR(10),
                '"45 years" → 45', CHAR(10),
                '"infant" → 0', CHAR(10),
                '"forty-five" → 45', CHAR(10),
                '"mid-30s" → 35', CHAR(10),
                '"150" → INVALID', CHAR(10),
                '"unknown" → INVALID'
            )
        ) AS age_extracted
    FROM race_classification
)
SELECT 
    patient_id,
    original_sex,
    original_race,
    original_age,
    sex_cleansed,
    race_cleansed,
    age_extracted,
    -- Convert to integer, NULL if invalid
    TRY_CAST(age_extracted AS INTEGER) AS age_cleansed,
    -- Flag records that need human review
    CASE 
        WHEN sex_cleansed IN ('Other', 'Unknown') 
            OR race_cleansed IN ('Other', 'Unknown')
            OR age_extracted = 'INVALID'
            OR TRY_CAST(age_extracted AS INTEGER) IS NULL
            OR TRY_CAST(age_extracted AS INTEGER) < 0
            OR TRY_CAST(age_extracted AS INTEGER) > 120
        THEN TRUE
        ELSE FALSE
    END AS requires_human_review,
    -- Confidence level (simple heuristic)
    CASE
        WHEN original_sex = sex_cleansed 
            AND original_race = race_cleansed 
            AND original_age = age_extracted
        THEN 'HIGH'
        WHEN sex_cleansed NOT IN ('Unknown', 'Other')
            AND race_cleansed NOT IN ('Unknown', 'Other')
            AND age_extracted != 'INVALID'
        THEN 'MEDIUM'
        ELSE 'LOW'
    END AS cleansing_confidence,
    CURRENT_TIMESTAMP() AS cleansed_timestamp
FROM age_cleansing;

-- ============================================================================
-- Step 3: Query results and statistics
-- ============================================================================

-- View all cleansed records
SELECT * FROM cleansed_patient_demographics
ORDER BY patient_id;

-- Summary statistics
SELECT 
    COUNT(*) AS total_records,
    COUNT(CASE WHEN sex_cleansed != 'Unknown' THEN 1 END) AS sex_cleansed_count,
    COUNT(CASE WHEN race_cleansed != 'Unknown' THEN 1 END) AS race_cleansed_count,
    COUNT(CASE WHEN age_cleansed IS NOT NULL THEN 1 END) AS age_cleansed_count,
    COUNT(CASE WHEN NOT requires_human_review THEN 1 END) AS auto_cleansed_count,
    COUNT(CASE WHEN requires_human_review THEN 1 END) AS needs_review_count,
    ROUND(COUNT(CASE WHEN NOT requires_human_review THEN 1 END) * 100.0 / COUNT(*), 2) AS auto_cleansed_percentage
FROM cleansed_patient_demographics;

-- Records requiring human review
SELECT 
    patient_id,
    original_sex,
    sex_cleansed,
    original_race,
    race_cleansed,
    original_age,
    age_cleansed,
    cleansing_confidence
FROM cleansed_patient_demographics
WHERE requires_human_review = TRUE
ORDER BY patient_id;

-- Cleansing confidence breakdown
SELECT 
    cleansing_confidence,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM cleansed_patient_demographics
GROUP BY cleansing_confidence
ORDER BY record_count DESC;

-- ============================================================================
-- Step 4: Alternative approach using AI_COMPLETE for batch processing
-- ============================================================================
-- For very large datasets, you might want to use AI_COMPLETE with a single
-- prompt to cleanse all fields at once (reduces API calls)

CREATE OR REPLACE TABLE cleansed_demographics_batch AS
SELECT 
    patient_id,
    sex AS original_sex,
    race AS original_race,
    age AS original_age,
    -- Single AI call to cleanse all fields
    SNOWFLAKE.CORTEX.AI_COMPLETE(
        'llama3.1-70b',  -- More powerful for complex reasoning
        CONCAT(
            'Cleanse this patient demographic data and return ONLY a JSON object with no additional text: ',
            '{"sex":"', COALESCE(sex, 'unknown'), '",',
            '"race":"', COALESCE(race, 'unknown'), '",',
            '"age":"', COALESCE(age, 'unknown'), '"}. ',
            'Rules: ',
            '1. Standardize sex to: Male, Female, Other, or Unknown. ',
            '2. Standardize race to OMB categories: White, Black or African American, Asian, Hispanic or Latino, ',
            'American Indian or Alaska Native, Native Hawaiian or Pacific Islander, Two or More Races, Other, or Unknown. ',
            '3. Extract age as a number 0-120, or "INVALID" if invalid. ',
            '4. Return format: {"sex":"standardized_value","race":"standardized_value","age":"number_or_INVALID"}'
        )
    ) AS cleansed_json,
    CURRENT_TIMESTAMP() AS cleansed_timestamp
FROM raw_patient_demographics;

-- Parse the JSON response
CREATE OR REPLACE TABLE cleansed_demographics_batch_parsed AS
SELECT 
    patient_id,
    original_sex,
    original_race,
    original_age,
    TRY_PARSE_JSON(cleansed_json) AS parsed_json,
    TRY_PARSE_JSON(cleansed_json):sex::VARCHAR AS sex_cleansed,
    TRY_PARSE_JSON(cleansed_json):race::VARCHAR AS race_cleansed,
    TRY_PARSE_JSON(cleansed_json):age::VARCHAR AS age_extracted,
    TRY_CAST(TRY_PARSE_JSON(cleansed_json):age::VARCHAR AS INTEGER) AS age_cleansed
FROM cleansed_demographics_batch;

-- ============================================================================
-- Step 5: Create a reusable stored procedure for production
-- ============================================================================

CREATE OR REPLACE PROCEDURE cleanse_demographics(
    source_table STRING,
    target_table STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    sql_stmt STRING;
    records_processed INTEGER;
BEGIN
    sql_stmt := 'CREATE OR REPLACE TABLE ' || target_table || ' AS
    WITH sex_classification AS (
        SELECT 
            *,
            SNOWFLAKE.CORTEX.AI_CLASSIFY(
                COALESCE(sex, ''Unknown''),
                ARRAY_CONSTRUCT(
                    OBJECT_CONSTRUCT(''label'', ''Male'', ''description'', ''Male sex including M, man, boy''),
                    OBJECT_CONSTRUCT(''label'', ''Female'', ''description'', ''Female sex including F, woman, girl''),
                    OBJECT_CONSTRUCT(''label'', ''Other'', ''description'', ''Non-binary or other identities''),
                    OBJECT_CONSTRUCT(''label'', ''Unknown'', ''description'', ''Unknown or not specified'')
                )
            ):labels[0] AS sex_cleansed
        FROM ' || source_table || '
    ),
    race_classification AS (
        SELECT 
            *,
            SNOWFLAKE.CORTEX.AI_CLASSIFY(
                COALESCE(race, ''Unknown''),
                ARRAY_CONSTRUCT(
                    OBJECT_CONSTRUCT(''label'', ''White'', ''description'', ''White or Caucasian''),
                    OBJECT_CONSTRUCT(''label'', ''Black or African American'', ''description'', ''Black, African American, or AA''),
                    OBJECT_CONSTRUCT(''label'', ''Asian'', ''description'', ''Asian''),
                    OBJECT_CONSTRUCT(''label'', ''Hispanic or Latino'', ''description'', ''Hispanic or Latino''),
                    OBJECT_CONSTRUCT(''label'', ''American Indian or Alaska Native'', ''description'', ''Native American''),
                    OBJECT_CONSTRUCT(''label'', ''Native Hawaiian or Pacific Islander'', ''description'', ''Pacific Islander''),
                    OBJECT_CONSTRUCT(''label'', ''Two or More Races'', ''description'', ''Multi-racial or mixed''),
                    OBJECT_CONSTRUCT(''label'', ''Other'', ''description'', ''Other race''),
                    OBJECT_CONSTRUCT(''label'', ''Unknown'', ''description'', ''Unknown or prefer not to say'')
                )
            ):labels[0] AS race_cleansed
        FROM sex_classification
    ),
    age_cleansing AS (
        SELECT 
            *,
            SNOWFLAKE.CORTEX.AI_COMPLETE(
                ''llama3.1-8b'',
                CONCAT(
                    ''Extract patient age (0-120) from: "'', COALESCE(age, ''unknown''), ''". '',
                    ''Return ONLY a number or "INVALID". '',
                    ''Rules: infants=0, ranges=midpoint, text numbers=digits, >120=INVALID, <0=INVALID. '',
                    ''Examples: "45 years"->45, "infant"->0, "mid-30s"->35, "150"->INVALID''
                )
            ) AS age_extracted
        FROM race_classification
    )
    SELECT 
        *,
        TRY_CAST(age_extracted AS INTEGER) AS age_cleansed,
        CASE 
            WHEN sex_cleansed IN (''Other'', ''Unknown'') 
                OR race_cleansed IN (''Other'', ''Unknown'')
                OR age_extracted = ''INVALID''
            THEN TRUE ELSE FALSE
        END AS requires_human_review,
        CURRENT_TIMESTAMP() AS cleansed_timestamp
    FROM age_cleansing';
    
    EXECUTE IMMEDIATE sql_stmt;
    
    SELECT COUNT(*) INTO records_processed FROM IDENTIFIER(:target_table);
    
    RETURN 'Successfully processed ' || records_processed || ' records from ' || 
           source_table || ' to ' || target_table;
END;
$$;

-- Test the stored procedure
-- CALL cleanse_demographics('raw_patient_demographics', 'cleansed_demographics_sp');

-- ============================================================================
-- Step 6: Cost optimization - Batch processing with filters
-- ============================================================================
-- Only apply AI functions to records that actually need cleansing

CREATE OR REPLACE TABLE cleansed_demographics_optimized AS
SELECT 
    patient_id,
    -- Only cleanse sex if it's not already in standard format
    CASE 
        WHEN sex IN ('Male', 'Female', 'Other', 'Unknown') THEN sex
        ELSE SNOWFLAKE.CORTEX.AI_CLASSIFY(
            COALESCE(sex, 'Unknown'),
            ARRAY_CONSTRUCT('Male', 'Female', 'Other', 'Unknown')
        ):labels[0]
    END AS sex_cleansed,
    -- Only cleanse race if it's not already in standard format
    CASE 
        WHEN race IN (
            'White', 
            'Black or African American', 
            'Asian', 
            'Hispanic or Latino',
            'American Indian or Alaska Native',
            'Native Hawaiian or Pacific Islander',
            'Two or More Races',
            'Other',
            'Unknown'
        ) THEN race
        ELSE SNOWFLAKE.CORTEX.AI_CLASSIFY(
            COALESCE(race, 'Unknown'),
            ARRAY_CONSTRUCT(
                'White',
                'Black or African American',
                'Asian',
                'Hispanic or Latino',
                'American Indian or Alaska Native',
                'Native Hawaiian or Pacific Islander',
                'Two or More Races',
                'Other',
                'Unknown'
            )
        ):labels[0]
    END AS race_cleansed,
    -- Only cleanse age if it's not a valid number
    CASE 
        WHEN TRY_CAST(age AS INTEGER) BETWEEN 0 AND 120 
            THEN TRY_CAST(age AS INTEGER)
        ELSE TRY_CAST(
            SNOWFLAKE.CORTEX.AI_COMPLETE(
                'llama3.1-8b',
                CONCAT(
                    'Extract patient age (0-120) from: "', age, '". ',
                    'Return ONLY number or "INVALID". ',
                    'Rules: infants=0, ranges=midpoint, >120=INVALID'
                )
            ) AS INTEGER
        )
    END AS age_cleansed
FROM raw_patient_demographics;

-- ============================================================================
-- Step 7: Human review workflow table
-- ============================================================================

CREATE OR REPLACE TABLE human_review_queue AS
SELECT 
    patient_id,
    original_sex,
    sex_cleansed,
    original_race,
    race_cleansed,
    original_age,
    age_cleansed,
    cleansing_confidence,
    NULL AS reviewer_name,
    NULL AS review_status,
    NULL AS corrected_sex,
    NULL AS corrected_race,
    NULL AS corrected_age,
    NULL AS review_notes,
    NULL AS review_timestamp,
    cleansed_timestamp AS queued_timestamp
FROM cleansed_patient_demographics
WHERE requires_human_review = TRUE;

-- View the human review queue
SELECT * FROM human_review_queue
ORDER BY patient_id;

-- ============================================================================
-- USAGE NOTES:
-- ============================================================================
-- 1. Adjust categories based on your specific healthcare standards
-- 2. Use llama3.1-8b for cost-effective processing of large datasets
-- 3. Use llama3.1-70b or mistral-large2 for higher accuracy on complex cases
-- 4. Monitor costs using: SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
-- 5. Consider creating a scheduled task to process new records automatically
-- 6. Implement feedback loop: use corrected records to improve prompts
-- ============================================================================

