-- Fix the remaining schema issues

-- Enable btree_gist extension
CREATE EXTENSION IF NOT EXISTS "btree_gist";

-- Drop and recreate plan_formulary table with simpler constraint
DROP TABLE IF EXISTS plan_formulary CASCADE;

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
    
    -- Simpler unique constraint instead of GIST
    CONSTRAINT plan_formulary_unique_simple 
        UNIQUE (plan_id, drug_id, effective_date),
    
    CONSTRAINT plan_formulary_date_check 
        CHECK (termination_date IS NULL OR termination_date > effective_date)
);

-- Add comment
COMMENT ON TABLE plan_formulary IS 'Plan-specific formulary overrides and benefit design rules'; 