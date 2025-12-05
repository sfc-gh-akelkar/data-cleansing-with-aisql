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

SELECT patient_id, 
        sex, 
            CASE 
               WHEN sex IN ('Male', 'Female', 'Other', 'Unknown') --Cost Optimization
                   THEN sex  -- Already clean
               ELSE AI_CLASSIFY(sex, ['Male','Female','Unknown']):labels[0] 
            END as CLEANSED_SEX,
        age,
            CASE 
                WHEN TRY_CAST(age AS INTEGER) BETWEEN 0 AND 120  --Cost Optimization
                    THEN TRY_CAST(age AS INTEGER)
               ELSE TRY_CAST(
                        SNOWFLAKE.CORTEX.COMPLETE(
                                'llama4-maverick',
                                    CONCAT(
                                        'Extract only the numerical age from: "', age, '". ',
                                        'Return ONLY the number (0-120). If text, convert to number (e.g., "forty-five" -> 45). If invalid/unknown, return NULL. If infant or number in months less than 12 return 0. If number in months less than 12 return 0. No explanation, just the number or NULL.'
                )
            ) AS INTEGER ) END
            as CLEANSED_AGE,
            race as original_race,
                    CASE 
        -- If already in standard OMB format, keep it. Cost Optimization
        WHEN UPPER(race) IN (
            'WHITE', 
            'BLACK', 
            'ASIAN', 
            'LATINO',
            'AMERICAN INDIAN',
            'PACIFIC ISLANDER',
            'TWO OR MORE RACES',
            'OTHER',
            'UNKNOWN'
        ) THEN UPPER(race)
        -- Otherwise use AI_COMPLETE to standardize
        ELSE SNOWFLAKE.CORTEX.COMPLETE(
            'llama3.3-70b',
            UPPER(CONCAT(
                'Standardize this race value to OMB categories: "', COALESCE(race, 'unknown'), '". ',
                'Valid categories: White, Black, Asian, Latino, American Indian, Pacific Islander, Two or More Races, Other, Unknown. ',
                /*'Rules: Caucasian->White, AA/African American->Black or African American, ',
                'Latino/Latina->Hispanic or Latino, Native American->American Indian or Alaska Native, ',
                'Pacific Islander->Native Hawaiian or Pacific Islander, Multi-racial/Mixed->Two or More Races. ',*/
                'Give no explanation. Return ONLY the standardized category name'
            )
        ))
    END AS race_cleansed
                
FROM raw_patient_demographics;