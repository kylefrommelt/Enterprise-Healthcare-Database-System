-- ============================================================================
-- PBM Database Sample Data Generation
-- Targeting: Goodroot/Navion Database Administrator Role
-- Phase 4: Realistic Test Data (1000+ records)
-- ============================================================================

-- ============================================================================
-- SAMPLE MEMBERS DATA (500 members across different plans)
-- ============================================================================

INSERT INTO members (
    first_name, last_name, date_of_birth, gender, 
    plan_id, group_number, effective_date, termination_date,
    address_line1, city, state, zip_code, phone, email
) VALUES
-- Plan A members (Commercial)
('John', 'Smith', '1985-03-15', 'M', 'PLAN_A_COMM', 'GRP001', '2024-01-01', NULL, '123 Main St', 'Chicago', 'IL', '60601', '312-555-0101', 'john.smith@email.com'),
('Sarah', 'Johnson', '1990-07-22', 'F', 'PLAN_A_COMM', 'GRP001', '2024-01-01', NULL, '456 Oak Ave', 'Chicago', 'IL', '60602', '312-555-0102', 'sarah.johnson@email.com'),
('Michael', 'Brown', '1978-11-30', 'M', 'PLAN_A_COMM', 'GRP001', '2024-01-01', NULL, '789 Pine St', 'Chicago', 'IL', '60603', '312-555-0103', 'michael.brown@email.com'),
('Jennifer', 'Davis', '1982-05-08', 'F', 'PLAN_A_COMM', 'GRP001', '2024-01-01', NULL, '321 Elm Dr', 'Chicago', 'IL', '60604', '312-555-0104', 'jennifer.davis@email.com'),
('David', 'Wilson', '1995-12-18', 'M', 'PLAN_A_COMM', 'GRP001', '2024-01-01', NULL, '654 Maple Ln', 'Chicago', 'IL', '60605', '312-555-0105', 'david.wilson@email.com'),

-- Plan B members (Medicare)
('Mary', 'Miller', '1955-04-12', 'F', 'PLAN_B_MCARE', 'GRP002', '2024-01-01', NULL, '987 Cedar St', 'Springfield', 'IL', '62701', '217-555-0201', 'mary.miller@email.com'),
('Robert', 'Moore', '1950-09-25', 'M', 'PLAN_B_MCARE', 'GRP002', '2024-01-01', NULL, '147 Birch Ave', 'Springfield', 'IL', '62702', '217-555-0202', 'robert.moore@email.com'),
('Patricia', 'Taylor', '1948-02-14', 'F', 'PLAN_B_MCARE', 'GRP002', '2024-01-01', NULL, '258 Walnut St', 'Springfield', 'IL', '62703', '217-555-0203', 'patricia.taylor@email.com'),
('James', 'Anderson', '1952-08-07', 'M', 'PLAN_B_MCARE', 'GRP002', '2024-01-01', NULL, '369 Chestnut Dr', 'Springfield', 'IL', '62704', '217-555-0204', 'james.anderson@email.com'),
('Barbara', 'Thomas', '1957-01-29', 'F', 'PLAN_B_MCARE', 'GRP002', '2024-01-01', NULL, '741 Spruce Ln', 'Springfield', 'IL', '62705', '217-555-0205', 'barbara.thomas@email.com'),

-- Plan C members (Medicaid)
('Christopher', 'Jackson', '1992-06-03', 'M', 'PLAN_C_MCAID', 'GRP003', '2024-01-01', NULL, '852 Willow St', 'Peoria', 'IL', '61601', '309-555-0301', 'christopher.jackson@email.com'),
('Michelle', 'White', '1988-10-16', 'F', 'PLAN_C_MCAID', 'GRP003', '2024-01-01', NULL, '963 Poplar Ave', 'Peoria', 'IL', '61602', '309-555-0302', 'michelle.white@email.com'),
('Joshua', 'Harris', '1996-03-28', 'M', 'PLAN_C_MCAID', 'GRP003', '2024-01-01', NULL, '159 Sycamore Dr', 'Peoria', 'IL', '61603', '309-555-0303', 'joshua.harris@email.com'),
('Amanda', 'Martin', '1991-12-05', 'F', 'PLAN_C_MCAID', 'GRP003', '2024-01-01', NULL, '357 Hickory Ln', 'Peoria', 'IL', '61604', '309-555-0304', 'amanda.martin@email.com'),
('Daniel', 'Garcia', '1987-07-19', 'M', 'PLAN_C_MCAID', 'GRP003', '2024-01-01', NULL, '486 Ash St', 'Peoria', 'IL', '61605', '309-555-0305', 'daniel.garcia@email.com');

-- Generate additional members using a function
DO $$
DECLARE
    i INTEGER;
    first_names TEXT[] := ARRAY['Alex', 'Emma', 'Liam', 'Olivia', 'Noah', 'Ava', 'Ethan', 'Sophia', 'Mason', 'Isabella', 'William', 'Charlotte', 'Logan', 'Amelia', 'Lucas'];
    last_names TEXT[] := ARRAY['Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Perez', 'Sanchez', 'Ramirez', 'Cruz', 'Flores', 'Gomez', 'Morales', 'Rivera', 'Ramos', 'Torres'];
    plan_ids TEXT[] := ARRAY['PLAN_A_COMM', 'PLAN_B_MCARE', 'PLAN_C_MCAID'];
    cities TEXT[] := ARRAY['Chicago', 'Springfield', 'Peoria', 'Rockford', 'Elgin'];
    states TEXT[] := ARRAY['IL', 'IN', 'WI', 'IA', 'MO'];
BEGIN
    FOR i IN 16..500 LOOP
        INSERT INTO members (
            first_name, last_name, date_of_birth, gender,
            plan_id, group_number, effective_date,
            address_line1, city, state, zip_code, phone, email
        ) VALUES (
            first_names[1 + (i % array_length(first_names, 1))],
            last_names[1 + (i % array_length(last_names, 1))] || i,
            DATE '1950-01-01' + (i * 200) * INTERVAL '1 day',
            CASE WHEN i % 2 = 0 THEN 'M' ELSE 'F' END,
            plan_ids[1 + (i % array_length(plan_ids, 1))],
            'GRP' || LPAD((i % 3 + 1)::TEXT, 3, '0'),
            '2024-01-01',
            i || ' Generated St',
            cities[1 + (i % array_length(cities, 1))],
            states[1 + (i % array_length(states, 1))],
            LPAD((60000 + i)::TEXT, 5, '0'),
            '555-' || LPAD(i::TEXT, 7, '0'),
            'member' || i || '@email.com'
        );
    END LOOP;
END $$;

-- ============================================================================
-- SAMPLE DRUGS DATA (Common PBM medications)
-- ============================================================================

INSERT INTO drugs (
    ndc_code, name, generic_name, brand_name, strength, dosage_form,
    route_of_administration, therapeutic_class, tier, formulary_flag,
    prior_auth_required, step_therapy_required, quantity_limit, manufacturer
) VALUES
-- Tier 1 - Generic medications
('00093-0058-01', 'Metformin HCl', 'Metformin Hydrochloride', 'Glucophage', '500mg', 'Tablet', 'Oral', 'Antidiabetics', 1, true, false, false, 180, 'Teva Pharmaceuticals'),
('00378-0221-05', 'Lisinopril', 'Lisinopril', 'Prinivil', '10mg', 'Tablet', 'Oral', 'ACE Inhibitors', 1, true, false, false, 90, 'Mylan Pharmaceuticals'),
('00093-0115-98', 'Atorvastatin', 'Atorvastatin Calcium', 'Lipitor', '20mg', 'Tablet', 'Oral', 'Statins', 1, true, false, false, 90, 'Pfizer Inc'),
('00603-3859-21', 'Amlodipine', 'Amlodipine Besylate', 'Norvasc', '5mg', 'Tablet', 'Oral', 'Calcium Channel Blockers', 1, true, false, false, 90, 'Greenstone LLC'),
('00093-0074-01', 'Omeprazole', 'Omeprazole', 'Prilosec', '20mg', 'Capsule', 'Oral', 'Proton Pump Inhibitors', 1, true, false, false, 90, 'Teva Pharmaceuticals'),

-- Tier 2 - Preferred brand medications  
('00024-5910-30', 'Synthroid', 'Levothyroxine Sodium', 'Synthroid', '100mcg', 'Tablet', 'Oral', 'Thyroid Hormones', 2, true, false, false, 90, 'AbbVie Inc'),
('00078-0357-15', 'Plavix', 'Clopidogrel Bisulfate', 'Plavix', '75mg', 'Tablet', 'Oral', 'Antiplatelet Agents', 2, true, false, false, 90, 'Bristol-Myers Squibb'),
('00074-3368-90', 'Advair Diskus', 'Fluticasone/Salmeterol', 'Advair Diskus', '250/50mcg', 'Inhalation Powder', 'Inhalation', 'Bronchodilators', 2, true, false, false, 30, 'GlaxoSmithKline'),
('00186-0264-60', 'Crestor', 'Rosuvastatin Calcium', 'Crestor', '10mg', 'Tablet', 'Oral', 'Statins', 2, true, false, false, 90, 'AstraZeneca'),
('00045-0110-60', 'Lantus', 'Insulin Glargine', 'Lantus', '100units/mL', 'Injection', 'Subcutaneous', 'Insulin', 2, true, false, false, 5, 'Sanofi-Aventis'),

-- Tier 3 - Non-preferred brand medications
('50458-220-10', 'Humira', 'Adalimumab', 'Humira', '40mg/0.8mL', 'Injection', 'Subcutaneous', 'TNF Blockers', 3, true, true, true, 2, 'AbbVie Inc'),
('00054-0225-25', 'Lyrica', 'Pregabalin', 'Lyrica', '75mg', 'Capsule', 'Oral', 'Anticonvulsants', 3, true, false, true, 60, 'Pfizer Inc'),
('00186-0781-09', 'Nexium', 'Esomeprazole Magnesium', 'Nexium', '40mg', 'Capsule', 'Oral', 'Proton Pump Inhibitors', 3, true, false, true, 30, 'AstraZeneca'),
('00469-0572-99', 'Enbrel', 'Etanercept', 'Enbrel', '50mg/mL', 'Injection', 'Subcutaneous', 'TNF Blockers', 3, true, true, true, 4, 'Amgen Inc'),
('00078-0526-15', 'Eliquis', 'Apixaban', 'Eliquis', '5mg', 'Tablet', 'Oral', 'Anticoagulants', 3, true, false, false, 60, 'Bristol-Myers Squibb'),

-- Tier 4 - Specialty medications
('50242-100-01', 'Harvoni', 'Ledipasvir/Sofosbuvir', 'Harvoni', '90mg/400mg', 'Tablet', 'Oral', 'Hepatitis C Agents', 4, true, true, true, 28, 'Gilead Sciences'),
('50436-3011-1', 'Keytruda', 'Pembrolizumab', 'Keytruda', '100mg/4mL', 'Injection', 'Intravenous', 'Oncology Agents', 4, true, true, true, 1, 'Merck & Co'),
('59676-303-13', 'Tecfidera', 'Dimethyl Fumarate', 'Tecfidera', '240mg', 'Capsule', 'Oral', 'Multiple Sclerosis Agents', 4, true, true, true, 60, 'Biogen Inc'),
('50419-488-58', 'Spinraza', 'Nusinersen', 'Spinraza', '12mg/5mL', 'Injection', 'Intrathecal', 'Neuromuscular Agents', 4, true, true, true, 1, 'Biogen Inc'),
('61755-000-02', 'Zolgensma', 'Onasemnogene Abeparvovec', 'Zolgensma', '2E13vg/mL', 'Injection', 'Intravenous', 'Gene Therapy', 4, true, true, true, 1, 'Novartis Gene Therapies');

-- ============================================================================
-- SAMPLE PHARMACIES DATA (Network pharmacies)
-- ============================================================================

INSERT INTO pharmacies (
    npi, name, chain_name, address_line1, city, state, zip_code, 
    phone, pbm_network, network_type, services_offered
) VALUES
-- Retail chain pharmacies
('1234567890', 'CVS Pharmacy #1001', 'CVS Health', '100 State St', 'Chicago', 'IL', '60601', '312-555-1001', true, 'retail', ARRAY['immunizations', 'medication_therapy_mgmt', 'diabetes_education']),
('1234567891', 'Walgreens #2001', 'Walgreens', '200 Michigan Ave', 'Chicago', 'IL', '60611', '312-555-1002', true, 'retail', ARRAY['immunizations', 'medication_therapy_mgmt', 'travel_clinic']),
('1234567892', 'Rite Aid #3001', 'Rite Aid', '300 Lake St', 'Chicago', 'IL', '60601', '312-555-1003', true, 'retail', ARRAY['immunizations', 'compounding']),

-- Independent pharmacies
('1234567893', 'Main Street Pharmacy', NULL, '456 Main St', 'Springfield', 'IL', '62701', '217-555-2001', true, 'retail', ARRAY['compounding', 'medication_therapy_mgmt', 'home_delivery']),
('1234567894', 'Community Care Pharmacy', NULL, '789 Oak Ave', 'Peoria', 'IL', '61601', '309-555-3001', true, 'retail', ARRAY['immunizations', 'diabetes_education', 'medication_synchronization']),

-- Mail order pharmacies
('1234567895', 'Express Scripts Mail Pharmacy', 'Express Scripts', '1000 Mail Order Blvd', 'St. Louis', 'MO', '63101', '800-555-4001', true, 'mail_order', ARRAY['home_delivery', 'medication_therapy_mgmt', 'specialty_pharmacy']),
('1234567896', 'CVS Caremark Mail Service', 'CVS Health', '2000 Caremark Dr', 'Nashville', 'TN', '37201', '800-555-4002', true, 'mail_order', ARRAY['home_delivery', 'specialty_pharmacy']),

-- Specialty pharmacies
('1234567897', 'Accredo Specialty Pharmacy', 'Express Scripts', '3000 Specialty Way', 'Memphis', 'TN', '38101', '800-555-5001', true, 'specialty', ARRAY['specialty_pharmacy', 'patient_education', 'clinical_monitoring']),
('1234567898', 'CVS Specialty Pharmacy', 'CVS Health', '4000 Specialty Dr', 'Pittsburgh', 'PA', '15201', '800-555-5002', true, 'specialty', ARRAY['specialty_pharmacy', 'patient_education', 'home_delivery']),

-- Hospital pharmacies
('1234567899', 'Northwestern Memorial Hospital Pharmacy', NULL, '251 E Huron St', 'Chicago', 'IL', '60611', '312-555-6001', true, 'hospital', ARRAY['inpatient_services', 'emergency_services', 'clinical_pharmacy']);

-- ============================================================================
-- SAMPLE PLAN FORMULARY DATA (Plan-specific overrides)
-- ============================================================================

INSERT INTO plan_formulary (
    plan_id, drug_id, tier_override, copay_amount, coinsurance_percentage,
    deductible_applies, effective_date
) VALUES
-- PLAN_A_COMM formulary (Commercial plan with lower copays)
('PLAN_A_COMM', 1, NULL, 5.00, NULL, false, '2024-01-01'),    -- Metformin
('PLAN_A_COMM', 2, NULL, 5.00, NULL, false, '2024-01-01'),    -- Lisinopril
('PLAN_A_COMM', 6, NULL, 30.00, NULL, true, '2024-01-01'),    -- Synthroid
('PLAN_A_COMM', 11, NULL, 75.00, NULL, true, '2024-01-01'),   -- Humira

-- PLAN_B_MCARE formulary (Medicare with different tier structure)
('PLAN_B_MCARE', 1, NULL, 3.00, NULL, false, '2024-01-01'),   -- Metformin (Medicare preferred)
('PLAN_B_MCARE', 2, NULL, 3.00, NULL, false, '2024-01-01'),   -- Lisinopril
('PLAN_B_MCARE', 11, 4, NULL, 25.0, true, '2024-01-01'),      -- Humira (moved to Tier 4)

-- PLAN_C_MCAID formulary (Medicaid with minimal copays)
('PLAN_C_MCAID', 1, NULL, 1.00, NULL, false, '2024-01-01'),   -- Metformin
('PLAN_C_MCAID', 2, NULL, 1.00, NULL, false, '2024-01-01'),   -- Lisinopril
('PLAN_C_MCAID', 11, NULL, 5.00, NULL, false, '2024-01-01');  -- Humira (special Medicaid pricing)

-- ============================================================================
-- SAMPLE PRIOR AUTHORIZATIONS DATA
-- ============================================================================

INSERT INTO prior_authorizations (
    member_id, drug_id, prescriber_npi, pa_number, request_date, 
    approval_date, status, diagnosis_codes, clinical_notes, 
    approved_quantity, approved_days_supply
) VALUES
-- Approved prior auths
(6, 11, '9876543210', 'PA-2024-001', '2024-01-15', '2024-01-17', 'approved', ARRAY['M79.3'], 'Rheumatoid arthritis, failed methotrexate therapy', 2, 30),
(8, 16, '9876543211', 'PA-2024-002', '2024-01-20', '2024-01-22', 'approved', ARRAY['B18.2'], 'Chronic hepatitis C, genotype 1', 28, 84),
(12, 11, '9876543212', 'PA-2024-003', '2024-02-01', '2024-02-03', 'approved', ARRAY['M05.9'], 'Seropositive rheumatoid arthritis', 2, 30),

-- Pending prior auths
(15, 17, '9876543213', 'PA-2024-004', '2024-02-10', NULL, 'pending', ARRAY['C78.0'], 'Metastatic lung cancer', NULL, NULL),
(25, 18, '9876543214', 'PA-2024-005', '2024-02-12', NULL, 'pending', ARRAY['G35'], 'Multiple sclerosis', NULL, NULL),

-- Denied prior auths
(35, 12, '9876543215', 'PA-2024-006', '2024-01-25', NULL, 'denied', ARRAY['M79.3'], 'Insufficient trial of alternative therapies', NULL, NULL);

-- ============================================================================
-- SAMPLE CLAIMS DATA (1000+ realistic claims)
-- ============================================================================

-- Sample processed claims
INSERT INTO claims (
    member_id, drug_id, pharmacy_id, prescription_number, date_prescribed, 
    date_filled, days_supply, quantity_dispensed, prescriber_npi,
    ingredient_cost, dispensing_fee, plan_paid_amount, member_copay, claim_status
) VALUES
-- January 2024 claims
(1, 1, 1, 'RX001001', '2024-01-05', '2024-01-06', 30, 60.000, '9876543210', 15.50, 2.00, 12.50, 5.00, 'processed'),
(2, 2, 2, 'RX002001', '2024-01-07', '2024-01-08', 30, 30.000, '9876543211', 8.25, 2.00, 5.25, 5.00, 'processed'),
(3, 3, 1, 'RX003001', '2024-01-10', '2024-01-11', 30, 30.000, '9876543212', 12.75, 2.00, 9.75, 5.00, 'processed'),
(6, 6, 3, 'RX006001', '2024-01-12', '2024-01-13', 30, 30.000, '9876543213', 45.80, 2.00, 17.80, 30.00, 'processed'),
(7, 7, 4, 'RX007001', '2024-01-15', '2024-01-16', 30, 30.000, '9876543214', 85.40, 2.00, 62.40, 25.00, 'processed'),

-- High-cost specialty claims
(6, 11, 8, 'RX011001', '2024-01-18', '2024-01-20', 30, 2.000, '9876543210', 5480.00, 15.00, 5420.00, 75.00, 'processed'),
(8, 16, 8, 'RX016001', '2024-01-22', '2024-01-25', 84, 28.000, '9876543211', 89250.00, 25.00, 84275.00, 5000.00, 'processed'),

-- Rejected claims
(25, 11, 1, 'RX025001', '2024-01-28', '2024-01-28', 30, 2.000, '9876543215', 5480.00, 2.00, 0.00, 0.00, 'rejected', 'P001', 'Prior authorization required'),
(45, 12, 2, 'RX045001', '2024-01-30', '2024-01-30', 30, 60.000, '9876543216', 275.50, 2.00, 0.00, 0.00, 'rejected', 'E001', 'Member not eligible');

-- Generate additional realistic claims using a function
DO $$
DECLARE
    i INTEGER;
    member_count INTEGER;
    drug_count INTEGER;
    pharmacy_count INTEGER;
    random_member INTEGER;
    random_drug INTEGER;
    random_pharmacy INTEGER;
    base_cost DECIMAL;
    claim_date DATE;
BEGIN
    SELECT COUNT(*) INTO member_count FROM members;
    SELECT COUNT(*) INTO drug_count FROM drugs;
    SELECT COUNT(*) INTO pharmacy_count FROM pharmacies;
    
    FOR i IN 1..1000 LOOP
        random_member := 1 + (random() * (member_count - 1))::INTEGER;
        random_drug := 1 + (random() * (drug_count - 1))::INTEGER;
        random_pharmacy := 1 + (random() * (pharmacy_count - 1))::INTEGER;
        
        -- Generate random claim date in last 6 months
        claim_date := CURRENT_DATE - (random() * 180)::INTEGER;
        
        -- Base cost varies by drug tier (simulate real pricing)
        SELECT 
            CASE 
                WHEN tier = 1 THEN 5.00 + (random() * 20)
                WHEN tier = 2 THEN 25.00 + (random() * 100)
                WHEN tier = 3 THEN 100.00 + (random() * 500)
                WHEN tier = 4 THEN 1000.00 + (random() * 10000)
            END
        INTO base_cost
        FROM drugs WHERE drug_id = random_drug;
        
        INSERT INTO claims (
            member_id, drug_id, pharmacy_id, prescription_number,
            date_prescribed, date_filled, days_supply, quantity_dispensed,
            prescriber_npi, ingredient_cost, dispensing_fee,
            plan_paid_amount, member_copay, claim_status
        ) VALUES (
            random_member,
            random_drug,
            random_pharmacy,
            'RX' || LPAD(i::TEXT, 6, '0'),
            claim_date - 1,
            claim_date,
            30,
            (15 + random() * 75)::DECIMAL(10,3),
            '98765432' || LPAD((10 + (i % 90))::TEXT, 2, '0'),
            base_cost,
            2.00 + (random() * 8)::DECIMAL(8,2),
            GREATEST(0, base_cost - (10 + random() * 90)::DECIMAL(8,2)),
            (10 + random() * 90)::DECIMAL(8,2),
            CASE WHEN random() < 0.95 THEN 'processed' ELSE 'rejected' END
        );
    END LOOP;
END $$;

-- ============================================================================
-- ETL STAGING TABLE WITH SAMPLE DATA
-- ============================================================================

-- Create ETL staging table for external data feeds
CREATE TABLE claims_staging (
    staging_id SERIAL PRIMARY KEY,
    file_name VARCHAR(200) NOT NULL,
    record_number INTEGER NOT NULL,
    raw_data JSONB NOT NULL,
    validation_status VARCHAR(20) DEFAULT 'pending' CHECK (validation_status IN ('pending', 'valid', 'invalid', 'processed')),
    error_messages TEXT[],
    processed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample incoming ETL data (JSON format simulating external feeds)
INSERT INTO claims_staging (file_name, record_number, raw_data, validation_status) VALUES
('pbm_feed_20240315_001.json', 1, '{"member_id": "M123456", "ndc": "00093-0058-01", "pharmacy_npi": "1234567890", "date_filled": "2024-03-15", "quantity": "60", "cost": "17.50"}', 'pending'),
('pbm_feed_20240315_001.json', 2, '{"member_id": "M123457", "ndc": "00378-0221-05", "pharmacy_npi": "1234567891", "date_filled": "2024-03-15", "quantity": "30", "cost": "10.25"}', 'pending'),
('pbm_feed_20240315_001.json', 3, '{"member_id": "INVALID", "ndc": "00093-0058-01", "pharmacy_npi": "1234567890", "date_filled": "2024-03-15", "quantity": "60", "cost": "17.50"}', 'invalid'),
('pbm_feed_20240315_002.json', 1, '{"member_id": "M123458", "ndc": "50458-220-10", "pharmacy_npi": "1234567897", "date_filled": "2024-03-15", "quantity": "2", "cost": "5495.00"}', 'pending');

-- ============================================================================
-- BUSINESS INTELLIGENCE SAMPLE QUERIES
-- ============================================================================

-- Create sample reporting views that would be used in BI tools

-- View: Top 10 most expensive drugs by total cost
CREATE VIEW top_expensive_drugs AS
SELECT 
    d.name as drug_name,
    d.therapeutic_class,
    COUNT(*) as claim_count,
    SUM(c.total_amount) as total_cost,
    AVG(c.total_amount) as avg_cost_per_claim,
    SUM(c.plan_paid_amount) as total_plan_cost
FROM claims c
JOIN drugs d ON c.drug_id = d.drug_id
WHERE c.claim_status = 'processed'
  AND c.date_filled >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY d.drug_id, d.name, d.therapeutic_class
ORDER BY total_cost DESC
LIMIT 10;

-- View: Member utilization patterns
CREATE VIEW member_utilization_summary AS
SELECT 
    m.plan_id,
    COUNT(DISTINCT m.member_id) as total_members,
    COUNT(DISTINCT c.member_id) as active_members,
    ROUND(COUNT(DISTINCT c.member_id)::NUMERIC / COUNT(DISTINCT m.member_id) * 100, 2) as utilization_rate,
    COUNT(*) as total_claims,
    SUM(c.total_amount) as total_cost,
    AVG(c.total_amount) as avg_cost_per_claim
FROM members m
LEFT JOIN claims c ON m.member_id = c.member_id 
    AND c.claim_status = 'processed'
    AND c.date_filled >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY m.plan_id;

-- View: Formulary compliance report
CREATE VIEW formulary_compliance AS
SELECT 
    m.plan_id,
    d.tier,
    COUNT(*) as total_claims,
    COUNT(*) FILTER (WHERE d.formulary_flag = true) as formulary_claims,
    COUNT(*) FILTER (WHERE d.formulary_flag = false) as non_formulary_claims,
    ROUND(COUNT(*) FILTER (WHERE d.formulary_flag = true)::NUMERIC / COUNT(*) * 100, 2) as compliance_rate
FROM claims c
JOIN members m ON c.member_id = m.member_id
JOIN drugs d ON c.drug_id = d.drug_id
WHERE c.claim_status = 'processed'
  AND c.date_filled >= CURRENT_DATE - INTERVAL '6 months'
GROUP BY m.plan_id, d.tier
ORDER BY m.plan_id, d.tier;

-- ============================================================================
-- DATA QUALITY CHECKS
-- ============================================================================

-- Function to run data quality checks
CREATE OR REPLACE FUNCTION run_data_quality_checks()
RETURNS TABLE(
    check_name TEXT,
    status TEXT,
    record_count BIGINT,
    details TEXT
) AS $$
BEGIN
    -- Check for members without claims
    RETURN QUERY
    SELECT 
        'Members Without Claims'::TEXT as check_name,
        CASE WHEN COUNT(*) > 0 THEN 'WARNING' ELSE 'PASS' END as status,
        COUNT(*) as record_count,
        'Members enrolled but no claims in last 12 months'::TEXT as details
    FROM members m
    LEFT JOIN claims c ON m.member_id = c.member_id 
        AND c.date_filled >= CURRENT_DATE - INTERVAL '12 months'
    WHERE c.member_id IS NULL
      AND m.eligibility_status = 'active';
    
    -- Check for claims with invalid drug NDCs
    RETURN QUERY
    SELECT 
        'Invalid Drug References'::TEXT as check_name,
        CASE WHEN COUNT(*) > 0 THEN 'ERROR' ELSE 'PASS' END as status,
        COUNT(*) as record_count,
        'Claims referencing non-existent drugs'::TEXT as details
    FROM claims c
    LEFT JOIN drugs d ON c.drug_id = d.drug_id
    WHERE d.drug_id IS NULL;
    
    -- Check for duplicate claims
    RETURN QUERY
    SELECT 
        'Duplicate Claims'::TEXT as check_name,
        CASE WHEN COUNT(*) > 0 THEN 'WARNING' ELSE 'PASS' END as status,
        COUNT(*) as record_count,
        'Potential duplicate claims based on member, drug, date'::TEXT as details
    FROM (
        SELECT member_id, drug_id, date_filled, COUNT(*) as duplicate_count
        FROM claims
        GROUP BY member_id, drug_id, date_filled
        HAVING COUNT(*) > 1
    ) duplicates;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- SAMPLE DATA GENERATION COMPLETE
-- ============================================================================

-- Update statistics after data load
ANALYZE members;
ANALYZE drugs;
ANALYZE pharmacies;
ANALYZE claims;
ANALYZE plan_formulary;
ANALYZE prior_authorizations;

-- Log data generation completion
INSERT INTO audit_log (table_name, operation, user_name, business_justification)
VALUES ('sample_data', 'GENERATE', current_user, 'Portfolio project sample data generation completed'); 