-- ============================================================================
-- PBM Database Performance Optimization
-- Targeting: Goodroot/Navion Database Administrator Role
-- Phase 3: Performance Optimization & Query Tuning
-- ============================================================================

-- ============================================================================
-- ADVANCED INDEXING STRATEGY FOR PBM OPERATIONS
-- ============================================================================

-- Composite indexes for common PBM query patterns
CREATE INDEX idx_claims_member_date_status ON claims(member_id, date_filled, claim_status)
WHERE claim_status = 'processed';

CREATE INDEX idx_claims_drug_date_cost ON claims(drug_id, date_filled, total_amount);

-- Partial index for high-tier drugs (performance critical)
CREATE INDEX idx_claims_high_tier_drugs ON claims(drug_id, date_filled, total_amount)
WHERE drug_id IN (SELECT drug_id FROM drugs WHERE tier >= 3);

-- Index for prior authorization lookups
CREATE INDEX idx_prior_auth_member_drug_status ON prior_authorizations(member_id, drug_id, status, expiration_date)
WHERE status = 'approved';

-- Functional index for date range queries (common in PBM reporting)
CREATE INDEX idx_claims_year_month ON claims(EXTRACT(year FROM date_filled), EXTRACT(month FROM date_filled));

-- Index for formulary compliance queries
CREATE INDEX idx_plan_formulary_effective ON plan_formulary(plan_id, drug_id, effective_date, termination_date)
WHERE termination_date IS NULL OR termination_date > CURRENT_DATE;

-- GIN index for pharmacy services (array searches)
CREATE INDEX idx_pharmacy_services_gin ON pharmacies USING gin(services_offered);

-- Covering index for claims summary reports (includes all needed columns)
CREATE INDEX idx_claims_summary_covering ON claims(date_filled, member_id) 
INCLUDE (drug_id, total_amount, plan_paid_amount, member_copay);

-- Partial index for rejected claims (for investigations)
CREATE INDEX idx_claims_rejected ON claims(date_filled, rejection_code, member_id)
WHERE claim_status = 'rejected';

-- ============================================================================
-- STORED PROCEDURES FOR PBM BUSINESS LOGIC
-- ============================================================================

-- Procedure: Check member eligibility
CREATE OR REPLACE FUNCTION check_member_eligibility(
    p_member_id INTEGER,
    p_service_date DATE DEFAULT CURRENT_DATE
)
RETURNS TABLE(
    is_eligible BOOLEAN,
    eligibility_status TEXT,
    plan_id TEXT,
    reason TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        CASE 
            WHEN m.eligibility_status = 'active' 
             AND p_service_date >= m.effective_date 
             AND (m.termination_date IS NULL OR p_service_date <= m.termination_date)
            THEN true 
            ELSE false 
        END as is_eligible,
        m.eligibility_status,
        m.plan_id,
        CASE 
            WHEN m.eligibility_status != 'active' THEN 'Member status: ' || m.eligibility_status
            WHEN p_service_date < m.effective_date THEN 'Service date before effective date'
            WHEN m.termination_date IS NOT NULL AND p_service_date > m.termination_date THEN 'Service date after termination'
            ELSE 'Eligible'
        END as reason
    FROM members m
    WHERE m.member_id = p_member_id;
END;
$$ LANGUAGE plpgsql STABLE;

-- Procedure: Calculate member copay based on formulary rules
CREATE OR REPLACE FUNCTION calculate_copay(
    p_member_id INTEGER,
    p_drug_id INTEGER,
    p_quantity DECIMAL,
    p_service_date DATE DEFAULT CURRENT_DATE
)
RETURNS TABLE(
    copay_amount DECIMAL(8,2),
    tier INTEGER,
    requires_prior_auth BOOLEAN,
    calculation_method TEXT
) AS $$
DECLARE
    member_plan TEXT;
    drug_tier INTEGER;
    base_copay DECIMAL(8,2);
    quantity_limit INTEGER;
BEGIN
    -- Get member's plan
    SELECT plan_id INTO member_plan FROM members WHERE member_id = p_member_id;
    
    -- Check for plan-specific formulary override
    SELECT 
        COALESCE(pf.tier_override, d.tier),
        COALESCE(pf.copay_amount, 
            CASE 
                WHEN COALESCE(pf.tier_override, d.tier) = 1 THEN 10.00
                WHEN COALESCE(pf.tier_override, d.tier) = 2 THEN 25.00
                WHEN COALESCE(pf.tier_override, d.tier) = 3 THEN 50.00
                WHEN COALESCE(pf.tier_override, d.tier) = 4 THEN 100.00
            END
        ),
        COALESCE(pf.prior_auth_override, d.prior_auth_required),
        COALESCE(pf.quantity_limit_override, d.quantity_limit)
    INTO drug_tier, base_copay, requires_prior_auth, quantity_limit
    FROM drugs d
    LEFT JOIN plan_formulary pf ON d.drug_id = pf.drug_id 
        AND pf.plan_id = member_plan
        AND p_service_date >= pf.effective_date
        AND (pf.termination_date IS NULL OR p_service_date <= pf.termination_date)
    WHERE d.drug_id = p_drug_id;
    
    RETURN QUERY
    SELECT 
        CASE 
            WHEN quantity_limit IS NOT NULL AND p_quantity > quantity_limit 
            THEN base_copay * 2 -- Double copay for exceeding quantity limits
            ELSE base_copay
        END as copay_amount,
        drug_tier as tier,
        requires_prior_auth,
        CASE 
            WHEN quantity_limit IS NOT NULL AND p_quantity > quantity_limit 
            THEN 'Tier ' || drug_tier || ' with quantity limit penalty'
            ELSE 'Tier ' || drug_tier || ' standard copay'
        END as calculation_method;
END;
$$ LANGUAGE plpgsql STABLE;

-- Procedure: Process pharmacy claim (main business logic)
CREATE OR REPLACE FUNCTION process_claim(
    p_member_id INTEGER,
    p_drug_id INTEGER,
    p_pharmacy_id INTEGER,
    p_prescription_number TEXT,
    p_date_prescribed DATE,
    p_date_filled DATE,
    p_days_supply INTEGER,
    p_quantity_dispensed DECIMAL,
    p_prescriber_npi TEXT,
    p_ingredient_cost DECIMAL,
    p_dispensing_fee DECIMAL
)
RETURNS TABLE(
    claim_id INTEGER,
    claim_status TEXT,
    member_copay DECIMAL(8,2),
    plan_paid_amount DECIMAL(8,2),
    rejection_code TEXT,
    rejection_description TEXT
) AS $$
DECLARE
    v_claim_id INTEGER;
    v_is_eligible BOOLEAN;
    v_eligibility_reason TEXT;
    v_calculated_copay DECIMAL(8,2);
    v_requires_prior_auth BOOLEAN;
    v_prior_auth_status TEXT;
    v_total_amount DECIMAL(10,2);
    v_plan_paid DECIMAL(10,2);
BEGIN
    -- Calculate total amount
    v_total_amount := p_ingredient_cost + p_dispensing_fee;
    
    -- Check member eligibility
    SELECT is_eligible, reason INTO v_is_eligible, v_eligibility_reason
    FROM check_member_eligibility(p_member_id, p_date_filled);
    
    IF NOT v_is_eligible THEN
        -- Insert rejected claim
        INSERT INTO claims (
            member_id, drug_id, pharmacy_id, prescription_number,
            date_prescribed, date_filled, days_supply, quantity_dispensed,
            prescriber_npi, ingredient_cost, dispensing_fee,
            plan_paid_amount, member_copay, claim_status,
            rejection_code, rejection_description
        ) VALUES (
            p_member_id, p_drug_id, p_pharmacy_id, p_prescription_number,
            p_date_prescribed, p_date_filled, p_days_supply, p_quantity_dispensed,
            p_prescriber_npi, p_ingredient_cost, p_dispensing_fee,
            0, 0, 'rejected', 'E001', v_eligibility_reason
        ) RETURNING claims.claim_id INTO v_claim_id;
        
        RETURN QUERY SELECT v_claim_id, 'rejected'::TEXT, 0::DECIMAL(8,2), 0::DECIMAL(8,2), 'E001'::TEXT, v_eligibility_reason;
        RETURN;
    END IF;
    
    -- Calculate copay and check prior auth requirements
    SELECT copay_amount, requires_prior_auth 
    INTO v_calculated_copay, v_requires_prior_auth
    FROM calculate_copay(p_member_id, p_drug_id, p_quantity_dispensed, p_date_filled);
    
    -- Check prior authorization if required
    IF v_requires_prior_auth THEN
        SELECT status INTO v_prior_auth_status
        FROM prior_authorizations
        WHERE member_id = p_member_id 
          AND drug_id = p_drug_id
          AND status = 'approved'
          AND p_date_filled <= COALESCE(expiration_date, p_date_filled);
        
        IF v_prior_auth_status IS NULL THEN
            -- Insert rejected claim for missing prior auth
            INSERT INTO claims (
                member_id, drug_id, pharmacy_id, prescription_number,
                date_prescribed, date_filled, days_supply, quantity_dispensed,
                prescriber_npi, ingredient_cost, dispensing_fee,
                plan_paid_amount, member_copay, claim_status,
                rejection_code, rejection_description
            ) VALUES (
                p_member_id, p_drug_id, p_pharmacy_id, p_prescription_number,
                p_date_prescribed, p_date_filled, p_days_supply, p_quantity_dispensed,
                p_prescriber_npi, p_ingredient_cost, p_dispensing_fee,
                0, 0, 'rejected', 'P001', 'Prior authorization required'
            ) RETURNING claims.claim_id INTO v_claim_id;
            
            RETURN QUERY SELECT v_claim_id, 'rejected'::TEXT, 0::DECIMAL(8,2), 0::DECIMAL(8,2), 'P001'::TEXT, 'Prior authorization required'::TEXT;
            RETURN;
        END IF;
    END IF;
    
    -- Calculate plan paid amount
    v_plan_paid := v_total_amount - v_calculated_copay;
    
    -- Insert successful claim
    INSERT INTO claims (
        member_id, drug_id, pharmacy_id, prescription_number,
        date_prescribed, date_filled, days_supply, quantity_dispensed,
        prescriber_npi, ingredient_cost, dispensing_fee,
        plan_paid_amount, member_copay, claim_status
    ) VALUES (
        p_member_id, p_drug_id, p_pharmacy_id, p_prescription_number,
        p_date_prescribed, p_date_filled, p_days_supply, p_quantity_dispensed,
        p_prescriber_npi, p_ingredient_cost, p_dispensing_fee,
        v_plan_paid, v_calculated_copay, 'processed'
    ) RETURNING claims.claim_id INTO v_claim_id;
    
    RETURN QUERY SELECT v_claim_id, 'processed'::TEXT, v_calculated_copay, v_plan_paid, NULL::TEXT, NULL::TEXT;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- PERFORMANCE MONITORING VIEWS
-- ============================================================================

-- View: Database performance metrics
CREATE VIEW performance_metrics AS
SELECT 
    schemaname,
    tablename,
    n_tup_ins as inserts,
    n_tup_upd as updates,
    n_tup_del as deletes,
    n_tup_hot_upd as hot_updates,
    seq_scan as sequential_scans,
    seq_tup_read as sequential_reads,
    idx_scan as index_scans,
    idx_tup_fetch as index_reads,
    ROUND(idx_tup_fetch::numeric / NULLIF(idx_scan, 0), 2) as avg_rows_per_index_scan
FROM pg_stat_user_tables
ORDER BY seq_scan + idx_scan DESC;

-- View: Index usage statistics
CREATE VIEW index_usage_stats AS
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan as times_used,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched,
    pg_size_pretty(pg_relation_size(indexrelid)) as size
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;

-- View: Claims processing performance
CREATE VIEW claims_performance_metrics AS
SELECT 
    DATE_TRUNC('hour', created_at) as hour,
    COUNT(*) as claims_processed,
    COUNT(*) FILTER (WHERE claim_status = 'processed') as successful_claims,
    COUNT(*) FILTER (WHERE claim_status = 'rejected') as rejected_claims,
    ROUND(AVG(EXTRACT(EPOCH FROM (updated_at - created_at))), 2) as avg_processing_seconds,
    SUM(total_amount) as total_amount_processed
FROM claims
WHERE created_at >= NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', created_at)
ORDER BY hour DESC;

-- ============================================================================
-- QUERY OPTIMIZATION EXAMPLES
-- ============================================================================

-- Optimized query: Member drug utilization (uses covering index)
CREATE OR REPLACE VIEW member_drug_utilization AS
SELECT 
    m.member_id,
    m.plan_id,
    d.therapeutic_class,
    COUNT(*) as prescription_count,
    SUM(c.quantity_dispensed) as total_quantity,
    SUM(c.total_amount) as total_cost,
    AVG(c.days_supply) as avg_days_supply,
    MAX(c.date_filled) as last_fill_date
FROM claims c
JOIN members m ON c.member_id = m.member_id
JOIN drugs d ON c.drug_id = d.drug_id
WHERE c.claim_status = 'processed'
  AND c.date_filled >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY m.member_id, m.plan_id, d.therapeutic_class;

-- Optimized query: High-cost claim alerts (uses partial index)
CREATE OR REPLACE FUNCTION get_high_cost_claims(cost_threshold DECIMAL DEFAULT 1000.00)
RETURNS TABLE(
    claim_id INTEGER,
    member_id INTEGER,
    drug_name TEXT,
    pharmacy_name TEXT,
    total_amount DECIMAL(10,2),
    date_filled DATE
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.claim_id,
        c.member_id,
        d.name as drug_name,
        p.name as pharmacy_name,
        c.total_amount,
        c.date_filled
    FROM claims c
    JOIN drugs d ON c.drug_id = d.drug_id
    JOIN pharmacies p ON c.pharmacy_id = p.pharmacy_id
    WHERE c.total_amount > cost_threshold
      AND c.claim_status = 'processed'
      AND c.date_filled >= CURRENT_DATE - INTERVAL '30 days'
    ORDER BY c.total_amount DESC;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- PARTITIONING STRATEGY FOR LARGE CLAIMS TABLE
-- ============================================================================

-- Create partitioned claims table for better performance with large datasets
CREATE TABLE claims_partitioned (
    LIKE claims INCLUDING ALL
) PARTITION BY RANGE (date_filled);

-- Create monthly partitions for current and future months
CREATE TABLE claims_2024_01 PARTITION OF claims_partitioned
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE claims_2024_02 PARTITION OF claims_partitioned
FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

CREATE TABLE claims_2024_03 PARTITION OF claims_partitioned
FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');

-- Function to automatically create future partitions
CREATE OR REPLACE FUNCTION create_monthly_partition(target_date DATE)
RETURNS VOID AS $$
DECLARE
    partition_name TEXT;
    start_date DATE;
    end_date DATE;
BEGIN
    start_date := DATE_TRUNC('month', target_date);
    end_date := start_date + INTERVAL '1 month';
    partition_name := 'claims_' || TO_CHAR(start_date, 'YYYY_MM');
    
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF claims_partitioned
                    FOR VALUES FROM (%L) TO (%L)',
                   partition_name, start_date, end_date);
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- AUTOMATED MAINTENANCE PROCEDURES
-- ============================================================================

-- Procedure: Update table statistics for optimal query plans
CREATE OR REPLACE FUNCTION update_pbm_statistics()
RETURNS VOID AS $$
BEGIN
    ANALYZE members;
    ANALYZE drugs;
    ANALYZE pharmacies;
    ANALYZE claims;
    ANALYZE plan_formulary;
    ANALYZE prior_authorizations;
    
    -- Log statistics update
    INSERT INTO audit_log (table_name, operation, user_name, business_justification)
    VALUES ('maintenance', 'ANALYZE', current_user, 'Automated statistics update for query optimization');
END;
$$ LANGUAGE plpgsql;

-- Procedure: Identify and recommend missing indexes
CREATE OR REPLACE FUNCTION recommend_indexes()
RETURNS TABLE(
    table_name TEXT,
    recommended_index TEXT,
    reason TEXT
) AS $$
BEGIN
    -- Identify tables with high sequential scan ratios
    RETURN QUERY
    SELECT 
        tablename::TEXT,
        'CREATE INDEX idx_' || tablename || '_optimization ON ' || tablename || ' (most_queried_column);' as recommended_index,
        'High sequential scan ratio: ' || ROUND((seq_scan::numeric / NULLIF(seq_scan + idx_scan, 0)) * 100, 1) || '%' as reason
    FROM pg_stat_user_tables
    WHERE seq_scan > idx_scan * 2
      AND n_tup_ins + n_tup_upd + n_tup_del > 1000; -- Only for active tables
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- CONNECTION POOLING AND QUERY OPTIMIZATION HINTS
-- ============================================================================

-- Function: Get connection and query performance recommendations
CREATE OR REPLACE FUNCTION get_performance_recommendations()
RETURNS TABLE(
    category TEXT,
    recommendation TEXT,
    current_value TEXT,
    suggested_value TEXT
) AS $$
BEGIN
    -- Check for slow queries
    RETURN QUERY
    SELECT 
        'Query Performance'::TEXT as category,
        'Review slow queries'::TEXT as recommendation,
        COUNT(*)::TEXT as current_value,
        'Should be < 10'::TEXT as suggested_value
    FROM pg_stat_statements 
    WHERE mean_exec_time > 1000 -- Queries taking more than 1 second
    HAVING COUNT(*) > 10;
    
    -- Check connection usage
    RETURN QUERY
    SELECT 
        'Connection Management'::TEXT as category,
        'Monitor connection usage'::TEXT as recommendation,
        COUNT(*)::TEXT as current_value,
        'Should be < 80% of max_connections'::TEXT as suggested_value
    FROM pg_stat_activity
    WHERE state = 'active';
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- MATERIALIZED VIEWS FOR REPORTING PERFORMANCE
-- ============================================================================

-- Materialized view for monthly claims summary (refresh daily)
CREATE MATERIALIZED VIEW monthly_claims_summary AS
SELECT 
    DATE_TRUNC('month', date_filled) as month,
    plan_id,
    therapeutic_class,
    tier,
    COUNT(*) as claim_count,
    SUM(total_amount) as total_cost,
    SUM(plan_paid_amount) as plan_cost,
    SUM(member_copay) as member_cost,
    AVG(days_supply) as avg_days_supply,
    COUNT(DISTINCT member_id) as unique_members
FROM claims c
JOIN members m ON c.member_id = m.member_id
JOIN drugs d ON c.drug_id = d.drug_id
WHERE claim_status = 'processed'
GROUP BY DATE_TRUNC('month', date_filled), plan_id, therapeutic_class, tier;

CREATE UNIQUE INDEX idx_monthly_claims_summary ON monthly_claims_summary(month, plan_id, therapeutic_class, tier);

-- ============================================================================
-- PERFORMANCE OPTIMIZATION COMPLETE
-- ============================================================================ 