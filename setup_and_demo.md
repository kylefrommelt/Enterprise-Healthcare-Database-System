# 🏥 PBM Database Portfolio - Setup & Demonstration Guide

**Targeting: Goodroot/Navion Database Administrator Role**

This guide provides step-by-step instructions to set up and demonstrate your PBM database portfolio project.

---

## 📋 Prerequisites

### Required Software
- **PostgreSQL 15+** (with pg_stat_statements extension)
- **Python 3.8+** with packages:
  ```bash
  pip install psycopg2-binary pandas
  ```

### System Requirements
- 4GB+ RAM (for realistic data volumes)
- 2GB+ disk space

---

## 🚀 Quick Setup (15 minutes)

### 1. Database Setup
```bash
# Create database and user
sudo -u postgres psql
```

```sql
CREATE DATABASE pbm_portfolio;
CREATE USER pbm_admin WITH PASSWORD 'secure_password_123';
GRANT ALL PRIVILEGES ON DATABASE pbm_portfolio TO pbm_admin;

-- Enable extensions
\c pbm_portfolio
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
```

### 2. Schema and Data Loading
```bash
# Run SQL scripts in order
psql -h localhost -U pbm_admin -d pbm_portfolio -f 01_schema_creation.sql
psql -h localhost -U pbm_admin -d pbm_portfolio -f 02_security_setup.sql
psql -h localhost -U pbm_admin -d pbm_portfolio -f 03_performance_optimization.sql
psql -h localhost -U pbm_admin -d pbm_portfolio -f 04_sample_data.sql
```

### 3. ETL Demonstration
```bash
# Update database config in etl_data_processor.py
python etl_data_processor.py
```

---

## 🎯 Key Demonstrations

### **Demo 1: PBM Business Logic**
```sql
-- Test member eligibility checking
SELECT * FROM check_member_eligibility(1, CURRENT_DATE);

-- Calculate copay for different scenarios
SELECT * FROM calculate_copay(1, 1, 60, CURRENT_DATE);  -- Tier 1 generic
SELECT * FROM calculate_copay(6, 11, 2, CURRENT_DATE);  -- Tier 3 specialty

-- Process a pharmacy claim end-to-end
SELECT * FROM process_claim(
    1, 1, 1, 'DEMO_RX_001', 
    CURRENT_DATE-1, CURRENT_DATE, 30, 60, 
    '9876543210', 15.50, 2.00
);
```

### **Demo 2: HIPAA Compliance & Security**
```sql
-- View audit logs (shows PHI access tracking)
SELECT * FROM audit_log 
WHERE timestamp >= CURRENT_DATE 
ORDER BY timestamp DESC LIMIT 10;

-- Test breach detection
SELECT * FROM detect_phi_breach();

-- Demonstrate row-level security
SET app.user_plans = 'PLAN_A_COMM';
SELECT COUNT(*) FROM members;  -- Shows only Plan A members
```

### **Demo 3: Performance Optimization**
```sql
-- View performance metrics
SELECT * FROM performance_metrics ORDER BY sequential_scans DESC LIMIT 10;

-- Check index usage
SELECT * FROM index_usage_stats WHERE times_used > 0 ORDER BY times_used DESC;

-- Get performance recommendations
SELECT * FROM get_performance_recommendations();

-- High-cost claims analysis (uses optimized indexes)
SELECT * FROM get_high_cost_claims(1000.00);
```

### **Demo 4: Business Intelligence Queries**
```sql
-- Top expensive drugs driving costs
SELECT * FROM top_expensive_drugs;

-- Member utilization by plan
SELECT * FROM member_utilization_summary;

-- Formulary compliance rates
SELECT * FROM formulary_compliance;

-- Claims processing performance
SELECT * FROM claims_performance_metrics;
```

### **Demo 5: Data Quality & ETL**
```sql
-- Run comprehensive data quality checks
SELECT * FROM run_data_quality_checks();

-- View ETL staging table
SELECT 
    file_name,
    validation_status,
    COUNT(*) as records,
    MIN(created_at) as first_record,
    MAX(created_at) as last_record
FROM claims_staging 
GROUP BY file_name, validation_status;

-- Refresh materialized views
REFRESH MATERIALIZED VIEW monthly_claims_summary;
SELECT * FROM monthly_claims_summary ORDER BY month DESC, total_cost DESC;
```

---

## 📊 Portfolio Highlights for Interview

### **1. Enterprise-Scale Database Design**
- **6 core tables** with proper relationships and constraints
- **Healthcare industry standards** (NDC codes, NPI identifiers)
- **Audit trails** for HIPAA compliance
- **Scalable partitioning** strategy for large datasets

### **2. Advanced PostgreSQL Features**
- **Complex stored procedures** with business logic
- **Strategic indexing** including partial, composite, and covering indexes
- **Row-level security** for multi-tenant data access
- **Materialized views** for reporting performance
- **JSONB** for flexible data structures

### **3. PBM Domain Expertise**
- **Real formulary management** with tier-based copays
- **Prior authorization workflows**
- **Claims adjudication logic**
- **Network pharmacy validation**
- **Plan-specific benefit design**

### **4. Production-Ready Operations**
- **Comprehensive error handling**
- **Performance monitoring views**
- **Automated maintenance procedures**
- **Data quality validation**
- **ETL processing with staging**

---

## 🔍 Key Queries to Showcase

### **Claims Analytics Query**
```sql
-- Monthly drug spend trend by therapeutic class
SELECT 
    DATE_TRUNC('month', c.date_filled) as month,
    d.therapeutic_class,
    COUNT(*) as claim_count,
    SUM(c.total_amount) as total_spend,
    AVG(c.member_copay) as avg_member_copay,
    COUNT(DISTINCT c.member_id) as unique_members
FROM claims c
JOIN drugs d ON c.drug_id = d.drug_id
WHERE c.claim_status = 'processed'
  AND c.date_filled >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY DATE_TRUNC('month', c.date_filled), d.therapeutic_class
ORDER BY month DESC, total_spend DESC;
```

### **Prior Authorization Impact Analysis**
```sql
-- Compare costs for drugs requiring prior auth vs. not
SELECT 
    d.prior_auth_required,
    COUNT(*) as claim_count,
    AVG(c.total_amount) as avg_cost,
    SUM(c.total_amount) as total_cost,
    AVG(c.member_copay) as avg_copay
FROM claims c
JOIN drugs d ON c.drug_id = d.drug_id
WHERE c.claim_status = 'processed'
  AND c.date_filled >= CURRENT_DATE - INTERVAL '6 months'
GROUP BY d.prior_auth_required;
```

### **Network Pharmacy Performance**
```sql
-- Pharmacy performance metrics
SELECT 
    p.name as pharmacy_name,
    p.network_type,
    COUNT(*) as total_claims,
    COUNT(*) FILTER (WHERE c.claim_status = 'processed') as processed_claims,
    COUNT(*) FILTER (WHERE c.claim_status = 'rejected') as rejected_claims,
    ROUND(COUNT(*) FILTER (WHERE c.claim_status = 'processed')::NUMERIC / COUNT(*) * 100, 2) as success_rate,
    SUM(c.total_amount) as total_volume
FROM pharmacies p
JOIN claims c ON p.pharmacy_id = c.pharmacy_id
WHERE c.date_filled >= CURRENT_DATE - INTERVAL '3 months'
GROUP BY p.pharmacy_id, p.name, p.network_type
ORDER BY total_volume DESC;
```

---

## 💡 Interview Talking Points

### **What Makes This Project Stand Out:**

1. **Real-World Complexity**: Uses actual NDC codes, NPI numbers, and ICD-10 diagnosis codes
2. **Production Considerations**: Includes partitioning, indexing strategies, and monitoring
3. **Security First**: Built-in HIPAA compliance from the ground up
4. **Business Logic**: Demonstrates understanding of PBM operations and healthcare workflows
5. **Performance Focus**: Query optimization for large datasets (1000+ records)
6. **ETL Integration**: Real data processing pipelines with validation and error handling

### **Technical Depth:**
- Advanced PostgreSQL features (triggers, stored procedures, materialized views)
- Complex business rules implemented in database layer
- Performance optimization strategies for healthcare data volumes
- Comprehensive audit trails and data governance

### **Domain Knowledge:**
- Pharmacy benefit management business processes
- Healthcare data standards and compliance requirements
- Claims processing workflows and rejection handling
- Formulary management and prior authorization processes

---

## 🎤 Demo Script for Interviews

```sql
-- "Let me show you how this PBM system handles a real pharmacy claim..."

-- 1. Check member eligibility
SELECT * FROM check_member_eligibility(1);

-- 2. Calculate appropriate copay
SELECT * FROM calculate_copay(1, 11, 2);  -- High-cost specialty drug

-- 3. Process the claim with business logic
SELECT * FROM process_claim(1, 11, 8, 'DEMO001', CURRENT_DATE-1, CURRENT_DATE, 30, 2, '9876543210', 5400.00, 95.00);

-- 4. Show audit trail
SELECT * FROM audit_log WHERE table_name = 'claims' ORDER BY timestamp DESC LIMIT 3;

-- 5. Performance analysis
SELECT * FROM claims_performance_metrics WHERE hour >= CURRENT_DATE;
```

**"This demonstrates real PBM workflows, HIPAA-compliant auditing, and production-ready performance monitoring - exactly what Goodroot/Navion needs in their DBA role."**

---

## ✅ Success Metrics

Your portfolio project demonstrates:

- ✅ **PBM Systems Expertise**: Real claims processing, formulary management, prior auth
- ✅ **PostgreSQL Mastery**: Advanced features, optimization, stored procedures  
- ✅ **HIPAA Compliance**: Audit trails, encryption, access controls
- ✅ **ETL Competency**: Data validation, staging, error handling
- ✅ **Performance Optimization**: Strategic indexing, partitioning, monitoring
- ✅ **Business Intelligence**: Reporting views, data analytics, KPI tracking

**Perfect alignment with Goodroot/Navion's Database Administrator requirements.** 