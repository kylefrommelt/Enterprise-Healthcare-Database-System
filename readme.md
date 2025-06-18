# 🏥 PBM Database Administrator Portfolio Project
**Targeting: Goodroot/Navion Database Administrator Role**

This project demonstrates the skills: PBM systems expertise, PostgreSQL administration, HIPAA compliance, ETL processes, and claims data management.

---

## 🎯 Project Objectives

Create a production-ready PBM database system that showcases:
- **PBM Domain Expertise**: Claims processing, eligibility verification, formulary management
- **Database Administration**: PostgreSQL optimization, indexing, stored procedures
- **HIPAA Compliance**: PHI protection, audit logging, access controls
- **ETL & Data Integration**: Automated data feeds, validation, error handling
- **Performance Optimization**: Query tuning for large healthcare datasets

---

## 📋 Complete Project Plan

### **Phase 1: Core Database Design**
```sql
-- Enhanced PBM schema with production considerations
CREATE TABLE members (
  member_id SERIAL PRIMARY KEY,
  first_name VARCHAR(50) NOT NULL,
  last_name VARCHAR(50) NOT NULL,
  date_of_birth DATE NOT NULL,
  eligibility_status VARCHAR(20) DEFAULT 'active',
  plan_id VARCHAR(20) NOT NULL,
  effective_date DATE NOT NULL,
  termination_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE drugs (
  drug_id SERIAL PRIMARY KEY,
  ndc_code VARCHAR(11) UNIQUE NOT NULL, -- National Drug Code
  name VARCHAR(200) NOT NULL,
  generic_name VARCHAR(200),
  tier INTEGER CHECK (tier BETWEEN 1 AND 4),
  formulary_flag BOOLEAN DEFAULT true,
  prior_auth_required BOOLEAN DEFAULT false,
  quantity_limit INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE claims (
  claim_id SERIAL PRIMARY KEY,
  member_id INTEGER REFERENCES members(member_id),
  drug_id INTEGER REFERENCES drugs(drug_id),
  pharmacy_npi VARCHAR(10) NOT NULL, -- National Provider Identifier
  date_filled DATE NOT NULL,
  days_supply INTEGER NOT NULL,
  quantity_dispensed DECIMAL(10,3) NOT NULL,
  amount_paid DECIMAL(10,2) NOT NULL,
  copay_amount DECIMAL(8,2) NOT NULL,
  claim_status VARCHAR(20) DEFAULT 'processed',
  rejection_code VARCHAR(10),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE pharmacies (
  pharmacy_id SERIAL PRIMARY KEY,
  npi VARCHAR(10) UNIQUE NOT NULL,
  name VARCHAR(200) NOT NULL,
  address VARCHAR(500),
  city VARCHAR(100),
  state VARCHAR(2),
  zip_code VARCHAR(10),
  pbm_network BOOLEAN DEFAULT true
);
```

### **Phase 2: HIPAA Compliance & Security**
- **Audit Logging**: Track all PHI access
- **Data Encryption**: Encrypt sensitive fields
- **Access Controls**: Role-based permissions
- **Data Masking**: Non-production data protection

```sql
-- Audit table for HIPAA compliance
CREATE TABLE audit_log (
  audit_id SERIAL PRIMARY KEY,
  table_name VARCHAR(50) NOT NULL,
  operation VARCHAR(10) NOT NULL,
  user_name VARCHAR(100) NOT NULL,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  old_values JSONB,
  new_values JSONB
);
```

### **Phase 3: Performance Optimization**
- **Strategic Indexing**: Optimize common PBM queries
- **Stored Procedures**: Claims processing automation
- **Query Optimization**: Large dataset handling

```sql
-- Performance indexes for PBM operations
CREATE INDEX idx_claims_member_date ON claims(member_id, date_filled);
CREATE INDEX idx_claims_drug_tier ON claims(drug_id) 
  WHERE EXISTS (SELECT 1 FROM drugs WHERE drugs.drug_id = claims.drug_id AND tier >= 3);
CREATE INDEX idx_members_eligibility ON members(eligibility_status, effective_date, termination_date);
```

### **Phase 4: ETL & Data Integration**
- **Automated Data Feeds**: Simulate external PBM feeds
- **Data Validation**: Ensure data integrity
- **Error Handling**: Missing/corrupt data management

```sql
-- ETL staging and validation
CREATE TABLE claims_staging (
  staging_id SERIAL PRIMARY KEY,
  raw_data JSONB NOT NULL,
  validation_status VARCHAR(20) DEFAULT 'pending',
  error_messages TEXT[],
  processed_at TIMESTAMP
);
```

### **Phase 5: Business Intelligence & Reporting**
- **Claims Analytics**: Drug utilization reporting
- **Formulary Compliance**: Tier analysis
- **Member Eligibility**: Coverage verification
- **Cost Management**: Spend analysis by therapeutic class

---

## 🎯 Key Deliverables

### **1. Database Scripts**
- `01_schema_creation.sql` - Complete database design
- `02_security_setup.sql` - HIPAA compliance implementation
- `03_performance_optimization.sql` - Indexes and stored procedures
- `04_sample_data.sql` - Realistic test data (1000+ records)

### **2. PBM Business Logic**
- Eligibility verification procedures
- Formulary compliance checking
- Claims adjudication simulation
- Prior authorization workflow

### **3. Reporting Queries**
- Top 10 most expensive drugs by member
- Formulary compliance rates by plan
- Claims rejection analysis
- Member utilization patterns

### **4. ETL Demonstration**
- Python script simulating external PBM data feed
- Data validation and cleansing procedures
- Error logging and notification system

### **5. Documentation**
- Database design rationale
- Performance tuning strategies
- HIPAA compliance measures
- Operational procedures

---

## 🛠 Technical Stack

- **Database**: PostgreSQL 15+
- **ETL**: Python with pandas/sqlalchemy
- **Security**: Row-level security, encrypted connections
- **Monitoring**: PostgreSQL performance insights
- **Version Control**: Git with database migration scripts

---

## 🏆 Success Metrics

This project demonstrates:
- ✅ **PBM Systems Expertise**: Real-world claims processing logic
- ✅ **PostgreSQL Mastery**: Advanced features, optimization, administration
- ✅ **HIPAA Compliance**: Production-ready security measures
- ✅ **ETL Competency**: Automated data integration workflows
- ✅ **Performance Optimization**: Enterprise-scale query tuning
- ✅ **Business Intelligence**: Healthcare analytics and reporting

**Perfect alignment with Goodroot/Navion's Database Administrator requirements.**

---

## 📌 Project Scope

Simulate a basic PBM system with 3–4 relational tables:
- `members`: patient/member metadata
- `drugs`: formulary and tier info
- `claims`: pharmacy claims transactions
- (optional) `pharmacies`: which PBM or pharmacy fulfilled a claim

The system will support:
- Claims auditing
- Drug utilization tracking
- Formulary compliance reporting
- Basic eligibility logic

---

## 🧱 Database Schema Design

Use PostgreSQL (or SQLite) to create the following schema:

```sql
-- members table
CREATE TABLE members (
  member_id SERIAL PRIMARY KEY,
  name TEXT,
  eligibility_status TEXT, -- e.g. 'active', 'inactive'
  plan_id TEXT
);

-- drugs table
CREATE TABLE drugs (
  drug_id SERIAL PRIMARY KEY,
  name TEXT,
  tier INTEGER,
  formulary_flag BOOLEAN
);

-- claims table
CREATE TABLE claims (
  claim_id SERIAL PRIMARY KEY,
  member_id INTEGER REFERENCES members(member_id),
  drug_id INTEGER REFERENCES drugs(drug_id),
  date_filled DATE,
  amount_paid DECIMAL(10,2)
);

-- Optional: pharmacies table
CREATE TABLE pharmacies (
  pharmacy_id SERIAL PRIMARY KEY,
  name TEXT,
  pbm_partner TEXT
);
