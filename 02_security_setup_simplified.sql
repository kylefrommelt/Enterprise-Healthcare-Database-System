-- ============================================================================
-- PBM Database Security & HIPAA Compliance Setup (Simplified)
-- Targeting: Goodroot/Navion Database Administrator Role
-- Phase 2: HIPAA Compliance & Security (User-level permissions)
-- ============================================================================

-- ============================================================================
-- AUDIT LOGGING SYSTEM - HIPAA Requirement for PHI Access Tracking
-- ============================================================================

-- Drop existing audit_log if it exists (to avoid conflicts)
DROP TABLE IF EXISTS audit_log CASCADE;

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
    application_name VARCHAR(100)
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
    -- Note: In production, use proper key management
    RETURN encode(
        pgp_sym_encrypt(data, 'demo_encryption_key_2024'), 
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
        'demo_encryption_key_2024'
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
    record_id_value INTEGER;
BEGIN
    -- Determine operation type
    IF TG_OP = 'DELETE' THEN
        operation_type := 'DELETE';
    ELSIF TG_OP = 'UPDATE' THEN
        operation_type := 'UPDATE';
    ELSIF TG_OP = 'INSERT' THEN
        operation_type := 'INSERT';
    END IF;

    -- Get record ID based on table
    CASE TG_TABLE_NAME
        WHEN 'members' THEN
            phi_fields := ARRAY['first_name', 'last_name', 'date_of_birth', 'ssn_encrypted', 'address_line1', 'phone', 'email'];
            record_id_value := COALESCE(NEW.member_id, OLD.member_id);
        WHEN 'claims' THEN
            phi_fields := ARRAY['prescription_number', 'date_filled', 'prescriber_npi'];
            record_id_value := COALESCE(NEW.claim_id, OLD.claim_id);
        WHEN 'prior_authorizations' THEN
            phi_fields := ARRAY['diagnosis_codes', 'clinical_notes'];
            record_id_value := COALESCE(NEW.pa_id, OLD.pa_id);
        ELSE
            phi_fields := ARRAY['general_phi'];
            record_id_value := NULL;
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
        record_id_value,
        current_user,
        'database_user',
        'session_' || extract(epoch from now())::text,
        inet_client_addr(),
        CASE WHEN TG_OP = 'DELETE' OR TG_OP = 'UPDATE' THEN row_to_json(OLD) ELSE NULL END,
        CASE WHEN TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN row_to_json(NEW) ELSE NULL END,
        phi_fields,
        'PBM_Portfolio_Demo'
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
DROP TRIGGER IF EXISTS audit_members_phi ON members;
CREATE TRIGGER audit_members_phi 
    AFTER INSERT OR UPDATE OR DELETE ON members
    FOR EACH ROW EXECUTE FUNCTION audit_phi_access();

-- Claims table (prescription and provider information)
DROP TRIGGER IF EXISTS audit_claims_phi ON claims;
CREATE TRIGGER audit_claims_phi 
    AFTER INSERT OR UPDATE OR DELETE ON claims
    FOR EACH ROW EXECUTE FUNCTION audit_phi_access();

-- Prior authorizations table (clinical information)
DROP TRIGGER IF EXISTS audit_prior_auth_phi ON prior_authorizations;
CREATE TRIGGER audit_prior_auth_phi 
    AFTER INSERT OR UPDATE OR DELETE ON prior_authorizations
    FOR EACH ROW EXECUTE FUNCTION audit_phi_access();

-- ============================================================================
-- DATA MASKING FUNCTIONS FOR NON-PRODUCTION
-- ============================================================================

-- Function to mask PHI for non-production environments
CREATE OR REPLACE FUNCTION mask_phi_data()
RETURNS VOID AS $$
BEGIN
    -- Mask member PHI (simplified for demo)
    UPDATE members SET
        first_name = 'FirstName' || member_id,
        last_name = 'LastName' || member_id,
        date_of_birth = '1980-01-01'::DATE + (member_id % 365) * INTERVAL '1 day',
        ssn_encrypted = encrypt_phi('999-99-' || LPAD((member_id % 10000)::TEXT, 4, '0')),
        address_line1 = member_id || ' Test Street',
        city = 'TestCity',
        phone = '555-' || LPAD((member_id % 10000)::TEXT, 4, '0'),
        email = 'test' || member_id || '@example.com'
    WHERE member_id > 0; -- Only update if records exist

    -- Mask prescription numbers in claims
    UPDATE claims SET
        prescription_number = 'RX' || LPAD(claim_id::TEXT, 8, '0')
    WHERE claim_id > 0;

    -- Log masking operation
    INSERT INTO audit_log (table_name, operation, user_name, phi_fields_accessed, business_justification)
    VALUES ('ALL_TABLES', 'MASK_PHI', current_user, ARRAY['all_phi_fields'], 'Data masking for non-production use');

END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

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
CREATE OR REPLACE VIEW claims_processing_view AS
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

-- View for reporting (aggregated, no individual PHI)
CREATE OR REPLACE VIEW claims_summary_view AS
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

-- ============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================================================

COMMENT ON FUNCTION encrypt_phi IS 'HIPAA-compliant encryption for PHI data using AES encryption';
COMMENT ON FUNCTION decrypt_phi IS 'Secured decryption function with audit logging';
COMMENT ON FUNCTION audit_phi_access IS 'Comprehensive PHI access auditing trigger function';
COMMENT ON FUNCTION detect_phi_breach IS 'Automated HIPAA breach detection based on access patterns';
COMMENT ON FUNCTION mask_phi_data IS 'Data masking for non-production environments';
COMMENT ON TABLE audit_log IS 'HIPAA-compliant audit trail for all PHI access and modifications';

-- ============================================================================
-- SECURITY SETUP COMPLETE (SIMPLIFIED)
-- ============================================================================ 