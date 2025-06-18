#!/usr/bin/env python3
"""
PBM ETL Data Processor
Targeting: Goodroot/Navion Database Administrator Role
Simulates processing external PBM data feeds with validation and error handling
"""

import json
import csv
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import logging
import sys
import os
from typing import Dict, List, Tuple, Optional
import re
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_processor.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    """Data validation result"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]

class PBMDataValidator:
    """Validates incoming PBM data against business rules"""
    
    def __init__(self, db_connection):
        self.db_connection = db_connection
        
    def validate_member_id(self, member_id: str) -> bool:
        """Validate member ID exists and is active"""
        cursor = self.db_connection.cursor()
        try:
            cursor.execute("""
                SELECT eligibility_status 
                FROM members 
                WHERE member_id = %s
            """, (member_id,))
            result = cursor.fetchone()
            return result is not None and result[0] == 'active'
        except Exception as e:
            logger.error(f"Error validating member ID {member_id}: {e}")
            return False
        finally:
            cursor.close()
    
    def validate_ndc_code(self, ndc: str) -> bool:
        """Validate NDC code format and existence"""
        # Check NDC format (multiple valid formats)
        ndc_patterns = [
            r'^\d{5}-\d{3}-\d{2}$',  # 5-3-2
            r'^\d{5}-\d{4}-\d{1}$',  # 5-4-1  
            r'^\d{4}-\d{4}-\d{2}$'   # 4-4-2
        ]
        
        format_valid = any(re.match(pattern, ndc) for pattern in ndc_patterns)
        if not format_valid:
            return False
        
        # Check if NDC exists in drugs table
        cursor = self.db_connection.cursor()
        try:
            cursor.execute("SELECT 1 FROM drugs WHERE ndc_code = %s", (ndc,))
            return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error validating NDC {ndc}: {e}")
            return False
        finally:
            cursor.close()
    
    def validate_pharmacy_npi(self, npi: str) -> bool:
        """Validate pharmacy NPI format and network status"""
        # NPI should be 10 digits
        if not re.match(r'^\d{10}$', npi):
            return False
        
        cursor = self.db_connection.cursor()
        try:
            cursor.execute("""
                SELECT pbm_network 
                FROM pharmacies 
                WHERE npi = %s
            """, (npi,))
            result = cursor.fetchone()
            return result is not None and result[0] is True
        except Exception as e:
            logger.error(f"Error validating pharmacy NPI {npi}: {e}")
            return False
        finally:
            cursor.close()
    
    def validate_claim_data(self, claim_data: Dict) -> ValidationResult:
        """Comprehensive claim data validation"""
        errors = []
        warnings = []
        
        # Required fields check
        required_fields = ['member_id', 'ndc', 'pharmacy_npi', 'date_filled', 'quantity', 'cost']
        for field in required_fields:
            if field not in claim_data or not claim_data[field]:
                errors.append(f"Missing required field: {field}")
        
        if errors:  # Don't continue validation if required fields missing
            return ValidationResult(False, errors, warnings)
        
        # Member ID validation
        if not self.validate_member_id(claim_data['member_id']):
            errors.append(f"Invalid or inactive member ID: {claim_data['member_id']}")
        
        # NDC validation
        if not self.validate_ndc_code(claim_data['ndc']):
            errors.append(f"Invalid NDC code: {claim_data['ndc']}")
        
        # Pharmacy NPI validation
        if not self.validate_pharmacy_npi(claim_data['pharmacy_npi']):
            errors.append(f"Invalid or out-of-network pharmacy NPI: {claim_data['pharmacy_npi']}")
        
        # Date validation
        try:
            fill_date = datetime.strptime(claim_data['date_filled'], '%Y-%m-%d').date()
            if fill_date > datetime.now().date():
                errors.append("Fill date cannot be in the future")
            elif fill_date < datetime.now().date() - timedelta(days=365):
                warnings.append("Fill date is more than 1 year old")
        except ValueError:
            errors.append(f"Invalid date format: {claim_data['date_filled']}")
        
        # Quantity validation
        try:
            quantity = float(claim_data['quantity'])
            if quantity <= 0:
                errors.append("Quantity must be positive")
            elif quantity > 1000:
                warnings.append("Unusually high quantity dispensed")
        except ValueError:
            errors.append(f"Invalid quantity: {claim_data['quantity']}")
        
        # Cost validation
        try:
            cost = float(claim_data['cost'])
            if cost < 0:
                errors.append("Cost cannot be negative")
            elif cost > 50000:
                warnings.append("Unusually high cost - potential specialty drug")
        except ValueError:
            errors.append(f"Invalid cost: {claim_data['cost']}")
        
        return ValidationResult(len(errors) == 0, errors, warnings)

class PBMETLProcessor:
    """Main ETL processor for PBM data feeds"""
    
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.connection = None
        self.validator = None
        
    def connect_to_database(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.connection.autocommit = False
            self.validator = PBMDataValidator(self.connection)
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def close_connection(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def process_json_file(self, file_path: str) -> Tuple[int, int, int]:
        """Process JSON file containing claim data"""
        logger.info(f"Processing JSON file: {file_path}")
        
        valid_records = 0
        invalid_records = 0
        processed_records = 0
        
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
                
            # Handle both single objects and arrays
            claims_data = data if isinstance(data, list) else [data]
            
            cursor = self.connection.cursor()
            
            for i, claim_data in enumerate(claims_data, 1):
                try:
                    # Validate claim data
                    validation_result = self.validator.validate_claim_data(claim_data)
                    
                    # Insert into staging table
                    status = 'valid' if validation_result.is_valid else 'invalid'
                    error_messages = validation_result.errors + validation_result.warnings
                    
                    cursor.execute("""
                        INSERT INTO claims_staging (
                            file_name, record_number, raw_data, 
                            validation_status, error_messages
                        ) VALUES (%s, %s, %s, %s, %s)
                    """, (
                        os.path.basename(file_path),
                        i,
                        json.dumps(claim_data),
                        status,
                        error_messages if error_messages else None
                    ))
                    
                    if validation_result.is_valid:
                        valid_records += 1
                        # Process valid claims into main tables
                        self.process_valid_claim(claim_data, cursor)
                        processed_records += 1
                    else:
                        invalid_records += 1
                        logger.warning(f"Invalid record {i}: {validation_result.errors}")
                
                except Exception as e:
                    logger.error(f"Error processing record {i}: {e}")
                    invalid_records += 1
            
            self.connection.commit()
            logger.info(f"File processing complete: {valid_records} valid, {invalid_records} invalid, {processed_records} processed")
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
        
        return valid_records, invalid_records, processed_records
    
    def process_valid_claim(self, claim_data: Dict, cursor):
        """Process validated claim into main claims table"""
        try:
            # Get database IDs from external identifiers
            member_id = self.get_member_id(claim_data['member_id'], cursor)
            drug_id = self.get_drug_id(claim_data['ndc'], cursor)
            pharmacy_id = self.get_pharmacy_id(claim_data['pharmacy_npi'], cursor)
            
            # Use the process_claim stored procedure
            cursor.execute("""
                SELECT * FROM process_claim(
                    %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s
                )
            """, (
                member_id,
                drug_id, 
                pharmacy_id,
                claim_data.get('prescription_number', f"ETL-{datetime.now().strftime('%Y%m%d')}-{member_id}"),
                claim_data.get('date_prescribed', claim_data['date_filled']),
                claim_data['date_filled'],
                claim_data.get('days_supply', 30),
                float(claim_data['quantity']),
                claim_data.get('prescriber_npi', '9999999999'),
                float(claim_data['cost']) * 0.9,  # Assume 90% is ingredient cost
                float(claim_data['cost']) * 0.1   # Assume 10% is dispensing fee
            ))
            
            result = cursor.fetchone()
            if result:
                claim_id, status, copay, plan_paid, rejection_code, rejection_desc = result
                logger.info(f"Claim processed: ID={claim_id}, Status={status}")
                
        except Exception as e:
            logger.error(f"Error processing valid claim: {e}")
            raise
    
    def get_member_id(self, external_member_id: str, cursor) -> int:
        """Get internal member ID from external identifier"""
        # For demo purposes, extract numeric part of member ID
        member_num = re.findall(r'\d+', external_member_id)
        if member_num:
            return int(member_num[0])
        return 1  # Default fallback
    
    def get_drug_id(self, ndc_code: str, cursor) -> int:
        """Get drug ID from NDC code"""
        cursor.execute("SELECT drug_id FROM drugs WHERE ndc_code = %s", (ndc_code,))
        result = cursor.fetchone()
        return result[0] if result else 1
    
    def get_pharmacy_id(self, npi: str, cursor) -> int:
        """Get pharmacy ID from NPI"""
        cursor.execute("SELECT pharmacy_id FROM pharmacies WHERE npi = %s", (npi,))
        result = cursor.fetchone()
        return result[0] if result else 1
    
    def generate_sample_files(self):
        """Generate sample JSON files for testing"""
        logger.info("Generating sample ETL files...")
        
        # Sample valid claims
        valid_claims = [
            {
                "member_id": "M000001",
                "ndc": "00093-0058-01",
                "pharmacy_npi": "1234567890",
                "date_filled": "2024-03-15",
                "quantity": "60",
                "cost": "17.50",
                "prescription_number": "RX123456",
                "prescriber_npi": "9876543210"
            },
            {
                "member_id": "M000002", 
                "ndc": "00378-0221-05",
                "pharmacy_npi": "1234567891",
                "date_filled": "2024-03-15",
                "quantity": "30",
                "cost": "10.25"
            },
            {
                "member_id": "M000006",
                "ndc": "50458-220-10",
                "pharmacy_npi": "1234567897",
                "date_filled": "2024-03-16",
                "quantity": "2",
                "cost": "5495.00"
            }
        ]
        
        # Sample invalid claims (for testing error handling)
        invalid_claims = [
            {
                "member_id": "INVALID_MEMBER",
                "ndc": "00093-0058-01",
                "pharmacy_npi": "1234567890",
                "date_filled": "2024-03-15",
                "quantity": "60",
                "cost": "17.50"
            },
            {
                "member_id": "M000001",
                "ndc": "INVALID-NDC-CODE",
                "pharmacy_npi": "1234567890",
                "date_filled": "2024-03-15",
                "quantity": "60",
                "cost": "17.50"
            }
        ]
        
        # Write sample files
        with open('sample_valid_claims.json', 'w') as f:
            json.dump(valid_claims, f, indent=2)
        
        with open('sample_invalid_claims.json', 'w') as f:
            json.dump(invalid_claims, f, indent=2)
        
        logger.info("Sample files generated: sample_valid_claims.json, sample_invalid_claims.json")
    
    def run_data_quality_report(self):
        """Run data quality checks and generate report"""
        logger.info("Running data quality checks...")
        
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT * FROM run_data_quality_checks()")
            results = cursor.fetchall()
            
            print("\n" + "="*60)
            print("DATA QUALITY REPORT")
            print("="*60)
            
            for check_name, status, record_count, details in results:
                status_symbol = "✅" if status == "PASS" else "⚠️" if status == "WARNING" else "❌"
                print(f"{status_symbol} {check_name}: {status}")
                print(f"   Records: {record_count}")
                print(f"   Details: {details}")
                print()
            
        except Exception as e:
            logger.error(f"Error running data quality checks: {e}")
        finally:
            cursor.close()
    
    def generate_etl_summary_report(self):
        """Generate ETL processing summary"""
        logger.info("Generating ETL summary report...")
        
        cursor = self.connection.cursor()
        try:
            # Staging table summary
            cursor.execute("""
                SELECT 
                    validation_status,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT file_name) as file_count
                FROM claims_staging 
                GROUP BY validation_status
            """)
            staging_results = cursor.fetchall()
            
            # Recent claims summary
            cursor.execute("""
                SELECT 
                    claim_status,
                    COUNT(*) as claim_count,
                    SUM(total_amount) as total_amount
                FROM claims 
                WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY claim_status
            """)
            claims_results = cursor.fetchall()
            
            print("\n" + "="*60)
            print("ETL PROCESSING SUMMARY")
            print("="*60)
            
            print("\nStaging Table Status:")
            for status, count, files in staging_results:
                print(f"  {status.upper()}: {count} records from {files} files")
            
            print("\nRecent Claims Processing (Last 7 days):")
            for status, count, amount in claims_results:
                amount_str = f"${amount:,.2f}" if amount else "$0.00"
                print(f"  {status.upper()}: {count} claims, {amount_str}")
            
        except Exception as e:
            logger.error(f"Error generating ETL summary: {e}")
        finally:
            cursor.close()

def main():
    """Main ETL execution function"""
    # Database configuration (would normally come from environment variables)
    db_config = {
        'host': 'localhost',
        'database': 'pbm_database',
        'user': 'pbm_user',
        'password': 'pbm_password',
        'port': '5432'
    }
    
    processor = PBMETLProcessor(db_config)
    
    try:
        # Connect to database
        processor.connect_to_database()
        
        # Generate sample files for demonstration
        processor.generate_sample_files()
        
        # Process sample files
        print("Processing valid claims file...")
        valid_count, invalid_count, processed_count = processor.process_json_file('sample_valid_claims.json')
        print(f"Valid claims processed: {processed_count}/{valid_count}")
        
        print("\nProcessing invalid claims file...")
        valid_count, invalid_count, processed_count = processor.process_json_file('sample_invalid_claims.json')
        print(f"Invalid claims found: {invalid_count}")
        
        # Generate reports
        processor.run_data_quality_report()
        processor.generate_etl_summary_report()
        
        print("\n✅ ETL processing completed successfully!")
        
    except Exception as e:
        logger.error(f"ETL processing failed: {e}")
        sys.exit(1)
    finally:
        processor.close_connection()

if __name__ == "__main__":
    main() 