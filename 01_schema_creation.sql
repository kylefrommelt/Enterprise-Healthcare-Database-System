-- ============================================================================
-- PBM Database Schema Creation
-- Targeting: Goodroot/Navion Database Administrator Role
-- Phase 1: Core Database Design
-- ============================================================================

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "btree_gist";

-- ============================================================================
-- MEMBERS TABLE - Patient/Subscriber Information
-- ============================================================================
CREATE TABLE members (
    member_id SERIAL PRIMARY KEY,
    member_uuid UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    date_of_birth DATE NOT NULL,
    ssn_encrypted TEXT, -- Encrypted SSN for HIPAA compliance
    gender CHAR(1) CHECK (gender IN ('M', 'F', 'O')),
    eligibility_status VARCHAR(20) DEFAULT 'active' CHECK (eligibility_status IN ('active', 'inactive', 'suspended', 'terminated')),
    plan_id VARCHAR(20) NOT NULL,
    group_number VARCHAR(50),
    effective_date DATE NOT NULL,
    termination_date DATE,
    address_line1 VARCHAR(100),
    address_line2 VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    phone VARCHAR(15),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Business rules constraints
    CONSTRAINT members_effective_termination_check 
        CHECK (termination_date IS NULL OR termination_date >= effective_date),
    CONSTRAINT members_dob_reasonable 
        CHECK (date_of_birth >= '1900-01-01' AND date_of_birth <= CURRENT_DATE),
    CONSTRAINT members_future_effective_check 
        CHECK (effective_date <= CURRENT_DATE + INTERVAL '90 days')
);

-- ============================================================================
-- DRUGS TABLE - Formulary and Medication Information
-- ============================================================================
CREATE TABLE drugs (
    drug_id SERIAL PRIMARY KEY,
    ndc_code VARCHAR(11) UNIQUE NOT NULL, -- National Drug Code (format: 99999-999-99)
    name VARCHAR(200) NOT NULL,
    generic_name VARCHAR(200),
    brand_name VARCHAR(200),
    strength VARCHAR(50),
    dosage_form VARCHAR(50), -- tablet, capsule, injection, etc.
    route_of_administration VARCHAR(50), -- oral, IV, topical, etc.
    therapeutic_class VARCHAR(100),
    tier INTEGER NOT NULL CHECK (tier BETWEEN 1 AND 4),
    formulary_flag BOOLEAN DEFAULT true,
    prior_auth_required BOOLEAN DEFAULT false,
    step_therapy_required BOOLEAN DEFAULT false,
    quantity_limit INTEGER CHECK (quantity_limit > 0),
    days_supply_limit INTEGER CHECK (days_supply_limit > 0),
    generic_available BOOLEAN DEFAULT false,
    manufacturer VARCHAR(100),
    fda_approval_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Business rules
    CONSTRAINT drugs_ndc_format_check 
        CHECK (ndc_code ~ '^\d{5}-\d{3}-\d{2}$' OR ndc_code ~ '^\d{5}-\d{4}-\d{1}$' OR ndc_code ~ '^\d{4}-\d{4}-\d{2}$')
);

-- ============================================================================
-- PHARMACIES TABLE - Pharmacy Network Information
-- ============================================================================
CREATE TABLE pharmacies (
    pharmacy_id SERIAL PRIMARY KEY,
    npi VARCHAR(10) UNIQUE NOT NULL, -- National Provider Identifier
    name VARCHAR(200) NOT NULL,
    chain_name VARCHAR(100),
    address_line1 VARCHAR(100) NOT NULL,
    address_line2 VARCHAR(100),
    city VARCHAR(50) NOT NULL,
    state VARCHAR(2) NOT NULL,
    zip_code VARCHAR(10) NOT NULL,
    phone VARCHAR(15),
    fax VARCHAR(15),
    email VARCHAR(100),
    pbm_network BOOLEAN DEFAULT true,
    network_type VARCHAR(20) DEFAULT 'retail' CHECK (network_type IN ('retail', 'mail_order', 'specialty', 'hospital')),
    contract_effective_date DATE,
    contract_termination_date DATE,
    hours_of_operation JSONB, -- Store hours in JSON format
    services_offered TEXT[], -- Array of services: ['immunizations', 'medication_therapy_mgmt', 'compounding']
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Business rules
    CONSTRAINT pharmacies_npi_format_check 
        CHECK (npi ~ '^\d{10}$'),
    CONSTRAINT pharmacies_contract_dates_check 
        CHECK (contract_termination_date IS NULL OR contract_termination_date >= contract_effective_date)
);

-- ============================================================================
-- CLAIMS TABLE - Pharmacy Claims Transactions
-- ============================================================================
CREATE TABLE claims (
    claim_id SERIAL PRIMARY KEY,
    claim_uuid UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
    member_id INTEGER NOT NULL REFERENCES members(member_id),
    drug_id INTEGER NOT NULL REFERENCES drugs(drug_id),
    pharmacy_id INTEGER NOT NULL REFERENCES pharmacies(pharmacy_id),
    prescription_number VARCHAR(50) NOT NULL,
    date_filled DATE NOT NULL,
    date_prescribed DATE NOT NULL,
    days_supply INTEGER NOT NULL CHECK (days_supply > 0 AND days_supply <= 365),
    quantity_dispensed DECIMAL(10,3) NOT NULL CHECK (quantity_dispensed > 0),
    metric_quantity DECIMAL(10,3), -- For liquids/creams in ml/grams
    refill_number INTEGER DEFAULT 0 CHECK (refill_number >= 0),
    refills_remaining INTEGER DEFAULT 0 CHECK (refills_remaining >= 0),
    prescriber_npi VARCHAR(10) NOT NULL,
    
    -- Financial information
    ingredient_cost DECIMAL(10,2) NOT NULL CHECK (ingredient_cost >= 0),
    dispensing_fee DECIMAL(8,2) NOT NULL CHECK (dispensing_fee >= 0),
    sales_tax DECIMAL(8,2) DEFAULT 0 CHECK (sales_tax >= 0),
    total_amount DECIMAL(10,2) GENERATED ALWAYS AS (ingredient_cost + dispensing_fee + sales_tax) STORED,
    plan_paid_amount DECIMAL(10,2) NOT NULL CHECK (plan_paid_amount >= 0),
    member_copay DECIMAL(8,2) NOT NULL CHECK (member_copay >= 0),
    deductible_amount DECIMAL(8,2) DEFAULT 0 CHECK (deductible_amount >= 0),
    
    -- Claim processing information
    claim_status VARCHAR(20) DEFAULT 'processed' CHECK (claim_status IN ('processed', 'rejected', 'pending', 'reversed', 'voided')),
    rejection_code VARCHAR(10),
    rejection_description TEXT,
    reversal_claim_id INTEGER REFERENCES claims(claim_id),
    original_claim_id INTEGER REFERENCES claims(claim_id),
    
    -- Processing metadata
    submission_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_date TIMESTAMP,
    bin_number VARCHAR(6), -- Bank Identification Number for routing
    pcn VARCHAR(10), -- Processor Control Number
    group_id VARCHAR(15),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Business rules and constraints
    CONSTRAINT claims_financial_logic_check 
        CHECK (plan_paid_amount + member_copay + deductible_amount <= total_amount),
    CONSTRAINT claims_date_logic_check 
        CHECK (date_filled >= date_prescribed AND date_filled <= CURRENT_DATE),
    CONSTRAINT claims_prescriber_npi_format 
        CHECK (prescriber_npi ~ '^\d{10}$'),
    CONSTRAINT claims_refill_logic_check 
        CHECK (refill_number <= 5), -- Typical maximum refills
    CONSTRAINT claims_rejection_logic_check 
        CHECK ((claim_status = 'rejected' AND rejection_code IS NOT NULL) OR 
               (claim_status != 'rejected' AND rejection_code IS NULL))
);

-- ============================================================================
-- PLAN_FORMULARY TABLE - Plan-specific formulary rules
-- ============================================================================
CREATE TABLE plan_formulary (
    plan_formulary_id SERIAL PRIMARY KEY,
    plan_id VARCHAR(20) NOT NULL,
    drug_id INTEGER NOT NULL REFERENCES drugs(drug_id),
    tier_override INTEGER CHECK (tier_override BETWEEN 1 AND 4),
    copay_amount DECIMAL(8,2) CHECK (copay_amount >= 0),
    coinsurance_percentage DECIMAL(5,2) CHECK (coinsurance_percentage >= 0 AND coinsurance_percentage <= 100),
    deductible_applies BOOLEAN DEFAULT true,
    prior_auth_override BOOLEAN DEFAULT false,
    quantity_limit_override INTEGER CHECK (quantity_limit_override > 0),
    effective_date DATE NOT NULL,
    termination_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint - one formulary entry per plan/drug combination at a time
    CONSTRAINT plan_formulary_unique_active 
        EXCLUDE USING gist (plan_id WITH =, drug_id WITH =, 
                           daterange(effective_date, COALESCE(termination_date, 'infinity')) WITH &&),
    
    CONSTRAINT plan_formulary_date_check 
        CHECK (termination_date IS NULL OR termination_date > effective_date)
);

-- ============================================================================
-- PRIOR_AUTHORIZATIONS TABLE - Prior authorization tracking
-- ============================================================================
CREATE TABLE prior_authorizations (
    pa_id SERIAL PRIMARY KEY,
    member_id INTEGER NOT NULL REFERENCES members(member_id),
    drug_id INTEGER NOT NULL REFERENCES drugs(drug_id),
    prescriber_npi VARCHAR(10) NOT NULL,
    pa_number VARCHAR(50) UNIQUE NOT NULL,
    request_date DATE NOT NULL,
    approval_date DATE,
    denial_date DATE,
    expiration_date DATE,
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'denied', 'expired')),
    diagnosis_codes TEXT[], -- Array of ICD-10 codes
    clinical_notes TEXT,
    approved_quantity INTEGER,
    approved_days_supply INTEGER,
    denial_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT pa_status_logic_check 
        CHECK ((status = 'approved' AND approval_date IS NOT NULL) OR
               (status = 'denied' AND denial_date IS NOT NULL) OR
               status IN ('pending', 'expired')),
    CONSTRAINT pa_prescriber_npi_format 
        CHECK (prescriber_npi ~ '^\d{10}$')
);

-- ============================================================================
-- INDEXES for Performance (Basic set - more in Phase 3)
-- ============================================================================

-- Members table indexes
CREATE INDEX idx_members_eligibility_status ON members(eligibility_status);
CREATE INDEX idx_members_plan_id ON members(plan_id);
CREATE INDEX idx_members_effective_dates ON members(effective_date, termination_date);

-- Drugs table indexes
CREATE INDEX idx_drugs_ndc_code ON drugs(ndc_code);
CREATE INDEX idx_drugs_tier ON drugs(tier);
CREATE INDEX idx_drugs_formulary_flag ON drugs(formulary_flag);
CREATE INDEX idx_drugs_therapeutic_class ON drugs(therapeutic_class);

-- Claims table indexes
CREATE INDEX idx_claims_member_id ON claims(member_id);
CREATE INDEX idx_claims_drug_id ON claims(drug_id);
CREATE INDEX idx_claims_pharmacy_id ON claims(pharmacy_id);
CREATE INDEX idx_claims_date_filled ON claims(date_filled);
CREATE INDEX idx_claims_status ON claims(claim_status);

-- Pharmacies table indexes
CREATE INDEX idx_pharmacies_npi ON pharmacies(npi);
CREATE INDEX idx_pharmacies_state ON pharmacies(state);
CREATE INDEX idx_pharmacies_network ON pharmacies(pbm_network);

-- ============================================================================
-- TRIGGERS for Updated_at timestamps
-- ============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply triggers to all tables with updated_at columns
CREATE TRIGGER update_members_updated_at BEFORE UPDATE ON members 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_drugs_updated_at BEFORE UPDATE ON drugs 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_pharmacies_updated_at BEFORE UPDATE ON pharmacies 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_claims_updated_at BEFORE UPDATE ON claims 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_prior_authorizations_updated_at BEFORE UPDATE ON prior_authorizations 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- COMMENTS for Documentation
-- ============================================================================

COMMENT ON TABLE members IS 'Patient/member demographic and eligibility information with HIPAA-compliant design';
COMMENT ON TABLE drugs IS 'National drug code database with formulary tier information and utilization controls';
COMMENT ON TABLE pharmacies IS 'Network pharmacy information with NPI validation and contract tracking';
COMMENT ON TABLE claims IS 'Pharmacy claims transactions with comprehensive financial and clinical data';
COMMENT ON TABLE plan_formulary IS 'Plan-specific formulary overrides and benefit design rules';
COMMENT ON TABLE prior_authorizations IS 'Prior authorization requests and approvals for restricted medications';

COMMENT ON COLUMN members.ssn_encrypted IS 'Encrypted SSN using pgcrypto for HIPAA compliance';
COMMENT ON COLUMN drugs.ndc_code IS 'National Drug Code in standard FDA format (5-4-1, 5-3-2, or 4-4-2)';
COMMENT ON COLUMN pharmacies.npi IS 'National Provider Identifier - unique 10-digit identifier';
COMMENT ON COLUMN claims.bin_number IS 'Bank Identification Number for claims routing to correct processor';
COMMENT ON COLUMN claims.total_amount IS 'Computed column: ingredient_cost + dispensing_fee + sales_tax';

-- ============================================================================
-- SCHEMA CREATION COMPLETE
-- ============================================================================ 