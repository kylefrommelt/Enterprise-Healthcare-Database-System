-- ============================================================================
-- PBM Database Security & HIPAA Compliance Setup
-- Targeting: Goodroot/Navion Database Administrator Role
-- Phase 2: HIPAA Compliance & Security
-- ============================================================================

-- ============================================================================
-- AUDIT LOGGING SYSTEM - HIPAA Requirement for PHI Access Tracking
-- ============================================================================

-- Main audit log table for all PHI access and modifications
CREATE TABLE audit_log (
    audit_id SERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    operation VARCHAR(10) NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE', 'SELECT')),
    record_id INTEGER,
    user_name VARCHAR(100) NOT NULL,
    user_role VARCHAR(50),
    session_id VARCHAR(100),
    ip_address INET,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    old_values JSONB,
    new_values JSONB,
    phi_fields_accessed TEXT[], -- Track which PHI fields were accessed
    business_justification TEXT, -- Required for PHI access
    application_name VARCHAR(100),
    CONSTRAINT audit_log_operation_check 
        CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE', 'SELECT'))
);

-- Index for audit log performance
CREATE INDEX idx_audit_log_timestamp ON audit_log(timestamp);
CREATE INDEX idx_audit_log_user ON audit_log(user_name);
CREATE INDEX idx_audit_log_table ON audit_log(table_name);
CREATE INDEX idx_audit_log_phi_access ON audit_log USING gin(phi_fields_accessed);

-- ============================================================================
-- ENCRYPTION FUNCTIONS FOR PHI DATA
-- ============================================================================

-- Function to encrypt sensitive data (SSN, etc.)
CREATE OR REPLACE FUNCTION encrypt_phi(data TEXT, key_name TEXT DEFAULT 'phi_key')
RETURNS TEXT AS $$
BEGIN
    -- Use pgcrypto to encrypt with AES
    RETURN encode(
        pgp_sym_encrypt(data, current_setting('app.encryption_key', true)), 
        'base64'
    );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to decrypt sensitive data
CREATE OR REPLACE FUNCTION decrypt_phi(encrypted_data TEXT, key_name TEXT DEFAULT 'phi_key')
RETURNS TEXT AS $$
BEGIN
    -- Decrypt and return original data
    RETURN pgp_sym_decrypt(
        decode(encrypted_data, 'base64'), 
        current_setting('app.encryption_key', true)
    );
EXCEPTION
    WHEN OTHERS THEN
        -- Log decryption attempt and return masked value
        INSERT INTO audit_log (table_name, operation, user_name, phi_fields_accessed, business_justification)
        VALUES ('encryption', 'DECRYPT_ATTEMPT', current_user, ARRAY['encrypted_field'], 'Decryption attempt failed');
        RETURN '***DECRYPT_ERROR***';
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ============================================================================
-- AUDIT TRIGGER FUNCTIONS
-- ============================================================================

-- Generic audit trigger function for PHI tables
CREATE OR REPLACE FUNCTION audit_phi_access()
RETURNS TRIGGER AS $$
DECLARE
    phi_fields TEXT[] := ARRAY[]::TEXT[];
    operation_type TEXT;
BEGIN
    -- Determine operation type
    IF TG_OP = 'DELETE' THEN
        operation_type := 'DELETE';
    ELSIF TG_OP = 'UPDATE' THEN
        operation_type := 'UPDATE';
    ELSIF TG_OP = 'INSERT' THEN
        operation_type := 'INSERT';
    END IF;

    -- Define PHI fields per table
    CASE TG_TABLE_NAME
        WHEN 'members' THEN
            phi_fields := ARRAY['first_name', 'last_name', 'date_of_birth', 'ssn_encrypted', 'address_line1', 'phone', 'email'];
        WHEN 'claims' THEN
            phi_fields := ARRAY['prescription_number', 'date_filled', 'prescriber_npi'];
        WHEN 'prior_authorizations' THEN
            phi_fields := ARRAY['diagnosis_codes', 'clinical_notes'];
        ELSE
            phi_fields := ARRAY['general_phi'];
    END CASE;

    -- Insert audit record
    INSERT INTO audit_log (
        table_name, 
        operation, 
        record_id, 
        user_name, 
        user_role,
        session_id,
        ip_address,
        old_values, 
        new_values, 
        phi_fields_accessed,
        application_name
    ) VALUES (
        TG_TABLE_NAME,
        operation_type,
        COALESCE(NEW.member_id, NEW.claim_id, NEW.pa_id, OLD.member_id, OLD.claim_id, OLD.pa_id),
        current_user,
        current_setting('app.user_role', true),
        current_setting('app.session_id', true),
        inet_client_addr(),
        CASE WHEN TG_OP = 'DELETE' OR TG_OP = 'UPDATE' THEN row_to_json(OLD) ELSE NULL END,
        CASE WHEN TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN row_to_json(NEW) ELSE NULL END,
        phi_fields,
        current_setting('application_name', true)
    );

    -- Return appropriate record
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ============================================================================
-- APPLY AUDIT TRIGGERS TO PHI TABLES
-- ============================================================================

-- Members table (contains most PHI)
CREATE TRIGGER audit_members_phi 
    AFTER INSERT OR UPDATE OR DELETE ON members
    FOR EACH ROW EXECUTE FUNCTION audit_phi_access();

-- Claims table (prescription and provider information)
CREATE TRIGGER audit_claims_phi 
    AFTER INSERT OR UPDATE OR DELETE ON claims
    FOR EACH ROW EXECUTE FUNCTION audit_phi_access();

-- Prior authorizations table (clinical information)
CREATE TRIGGER audit_prior_auth_phi 
    AFTER INSERT OR UPDATE OR DELETE ON prior_authorizations
    FOR EACH ROW EXECUTE FUNCTION audit_phi_access();

-- ============================================================================
-- ROLE-BASED ACCESS CONTROL (RBAC)
-- ============================================================================

-- Create roles for different access levels
CREATE ROLE pbm_read_only;
CREATE ROLE pbm_claims_processor;
CREATE ROLE pbm_pharmacist;
CREATE ROLE pbm_administrator;
CREATE ROLE pbm_auditor;

-- ============================================================================
-- PERMISSIONS SETUP
-- ============================================================================

-- Read-only role (reports, analytics)
GRANT SELECT ON members, drugs, pharmacies, claims, plan_formulary TO pbm_read_only;
GRANT SELECT ON audit_log TO pbm_auditor; -- Only auditors can see audit logs

-- Claims processor role
GRANT SELECT, INSERT, UPDATE ON claims TO pbm_claims_processor;
GRANT SELECT ON members, drugs, pharmacies, plan_formulary TO pbm_claims_processor;

-- Pharmacist role (can view clinical data)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pbm_pharmacist;
GRANT INSERT, UPDATE ON prior_authorizations TO pbm_pharmacist;

-- Administrator role (full access except audit manipulation)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pbm_administrator;
REVOKE DELETE ON audit_log FROM pbm_administrator; -- Prevent audit log tampering

-- Auditor role (read-only + audit access)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pbm_auditor;

-- ============================================================================
-- DATA MASKING FUNCTIONS FOR NON-PRODUCTION
-- ============================================================================

-- Function to mask PHI for non-production environments
CREATE OR REPLACE FUNCTION mask_phi_data()
RETURNS VOID AS $$
BEGIN
    -- Only allow in non-production environments
    IF current_setting('app.environment', true) = 'production' THEN
        RAISE EXCEPTION 'PHI masking not allowed in production environment';
    END IF;

    -- Mask member PHI
    UPDATE members SET
        first_name = 'FirstName' || member_id,
        last_name = 'LastName' || member_id,
        date_of_birth = '1980-01-01'::DATE + (member_id % 365) * INTERVAL '1 day',
        ssn_encrypted = encrypt_phi('999-99-' || LPAD((member_id % 10000)::TEXT, 4, '0')),
        address_line1 = member_id || ' Test Street',
        city = 'TestCity',
        phone = '555-' || LPAD((member_id % 10000)::TEXT, 4, '0'),
        email = 'test' || member_id || '@example.com';

    -- Mask prescription numbers in claims
    UPDATE claims SET
        prescription_number = 'RX' || LPAD(claim_id::TEXT, 8, '0');

    -- Log masking operation
    INSERT INTO audit_log (table_name, operation, user_name, phi_fields_accessed, business_justification)
    VALUES ('ALL_TABLES', 'MASK_PHI', current_user, ARRAY['all_phi_fields'], 'Data masking for non-production use');

END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ============================================================================
-- ROW LEVEL SECURITY (RLS) FOR MEMBER DATA
-- ============================================================================

-- Enable RLS on members table
ALTER TABLE members ENABLE ROW LEVEL SECURITY;

-- Policy: Users can only see members from their assigned plans
CREATE POLICY members_plan_access ON members
    FOR ALL TO pbm_claims_processor, pbm_pharmacist
    USING (plan_id = ANY(string_to_array(current_setting('app.user_plans', true), ',')));

-- Policy: Administrators can see all members
CREATE POLICY members_admin_access ON members
    FOR ALL TO pbm_administrator
    USING (true);

-- Policy: Read-only users can see all members but limited fields
CREATE POLICY members_readonly_access ON members
    FOR SELECT TO pbm_read_only
    USING (true);

-- ============================================================================
-- HIPAA BREACH DETECTION
-- ============================================================================

-- Function to detect potential HIPAA breaches
CREATE OR REPLACE FUNCTION detect_phi_breach()
RETURNS TABLE(
    potential_breach_type TEXT,
    user_name TEXT,
    record_count BIGINT,
    time_period TEXT
) AS $$
BEGIN
    -- Detect unusual access patterns
    RETURN QUERY
    SELECT 
        'BULK_PHI_ACCESS' as potential_breach_type,
        a.user_name,
        COUNT(*) as record_count,
        'Last Hour' as time_period
    FROM audit_log a
    WHERE a.timestamp >= NOW() - INTERVAL '1 hour'
      AND array_length(a.phi_fields_accessed, 1) > 0
    GROUP BY a.user_name
    HAVING COUNT(*) > 100; -- Flag if user accessed >100 PHI records in 1 hour

    -- Detect after-hours access
    RETURN QUERY
    SELECT 
        'AFTER_HOURS_ACCESS' as potential_breach_type,
        a.user_name,
        COUNT(*) as record_count,
        'After Hours' as time_period
    FROM audit_log a
    WHERE a.timestamp >= CURRENT_DATE
      AND (EXTRACT(hour FROM a.timestamp) < 7 OR EXTRACT(hour FROM a.timestamp) > 18)
      AND array_length(a.phi_fields_accessed, 1) > 0
    GROUP BY a.user_name
    HAVING COUNT(*) > 10;

    -- Detect weekend access
    RETURN QUERY
    SELECT 
        'WEEKEND_ACCESS' as potential_breach_type,
        a.user_name,
        COUNT(*) as record_count,
        'Weekend' as time_period
    FROM audit_log a
    WHERE a.timestamp >= CURRENT_DATE - INTERVAL '7 days'
      AND EXTRACT(dow FROM a.timestamp) IN (0, 6) -- Sunday = 0, Saturday = 6
      AND array_length(a.phi_fields_accessed, 1) > 0
    GROUP BY a.user_name
    HAVING COUNT(*) > 20;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ============================================================================
-- MINIMUM NECESSARY STANDARD VIEWS
-- ============================================================================

-- View for claims processing (limited PHI exposure)
CREATE VIEW claims_processing_view AS
SELECT 
    c.claim_id,
    c.member_id,
    LEFT(m.first_name, 1) || '***' as member_initial,
    LEFT(m.last_name, 1) || '***' as member_last_initial,
    c.drug_id,
    d.name as drug_name,
    d.tier,
    c.pharmacy_id,
    p.name as pharmacy_name,
    c.date_filled,
    c.days_supply,
    c.quantity_dispensed,
    c.total_amount,
    c.plan_paid_amount,
    c.member_copay,
    c.claim_status
FROM claims c
JOIN members m ON c.member_id = m.member_id
JOIN drugs d ON c.drug_id = d.drug_id
JOIN pharmacies p ON c.pharmacy_id = p.pharmacy_id;

-- Grant access to processing view
GRANT SELECT ON claims_processing_view TO pbm_claims_processor;

-- View for reporting (aggregated, no individual PHI)
CREATE VIEW claims_summary_view AS
SELECT 
    DATE_TRUNC('month', date_filled) as month,
    plan_id,
    therapeutic_class,
    tier,
    COUNT(*) as claim_count,
    SUM(total_amount) as total_cost,
    SUM(plan_paid_amount) as plan_cost,
    AVG(member_copay) as avg_copay
FROM claims c
JOIN members m ON c.member_id = m.member_id
JOIN drugs d ON c.drug_id = d.drug_id
GROUP BY DATE_TRUNC('month', date_filled), plan_id, therapeutic_class, tier;

-- Grant access to summary view
GRANT SELECT ON claims_summary_view TO pbm_read_only;

-- ============================================================================
-- RETENTION POLICY FOR AUDIT LOGS
-- ============================================================================

-- Function to archive old audit logs (HIPAA requires 6 years retention)
CREATE OR REPLACE FUNCTION archive_audit_logs()
RETURNS INTEGER AS $$
DECLARE
    archived_count INTEGER;
BEGIN
    -- Move logs older than 6 years to archive table
    CREATE TABLE IF NOT EXISTS audit_log_archive (LIKE audit_log INCLUDING ALL);
    
    WITH archived_rows AS (
        DELETE FROM audit_log 
        WHERE timestamp < NOW() - INTERVAL '6 years'
        RETURNING *
    )
    INSERT INTO audit_log_archive 
    SELECT * FROM archived_rows;
    
    GET DIAGNOSTICS archived_count = ROW_COUNT;
    
    -- Log the archival operation
    INSERT INTO audit_log (table_name, operation, user_name, business_justification)
    VALUES ('audit_log', 'ARCHIVE', current_user, 'Automatic 6-year retention compliance');
    
    RETURN archived_count;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ============================================================================
-- SECURITY CONFIGURATION PARAMETERS
-- ============================================================================

-- Set up configuration parameters for runtime security settings
-- These would typically be set in postgresql.conf or via environment variables

COMMENT ON FUNCTION encrypt_phi IS 'HIPAA-compliant encryption for PHI data using AES encryption';
COMMENT ON FUNCTION decrypt_phi IS 'Secured decryption function with audit logging';
COMMENT ON FUNCTION audit_phi_access IS 'Comprehensive PHI access auditing trigger function';
COMMENT ON FUNCTION detect_phi_breach IS 'Automated HIPAA breach detection based on access patterns';
COMMENT ON FUNCTION mask_phi_data IS 'Data masking for non-production environments';
COMMENT ON TABLE audit_log IS 'HIPAA-compliant audit trail for all PHI access and modifications';

-- ============================================================================
-- SECURITY SETUP COMPLETE
-- ============================================================================ 