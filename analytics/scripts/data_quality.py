import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any

class DataQualityChecker:
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.setup_logging()
    
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def get_connection(self):
        return psycopg2.connect(**self.db_config)
    
    def check_data_freshness(self, table: str, timestamp_col: str, max_age_hours: int = 24) -> Dict[str, Any]:
        """Check if data is fresh within specified time window"""
        query = f"""
        SELECT 
            MAX({timestamp_col}) as latest_record,
            COUNT(*) as total_records,
            COUNT(*) FILTER (WHERE {timestamp_col} > NOW() - INTERVAL '{max_age_hours} hours') as recent_records
        FROM {table}
        """
        
        with self.get_connection() as conn:
            df = pd.read_sql(query, conn)
            
        latest_record = df['latest_record'].iloc[0]
        age_hours = (datetime.now() - latest_record).total_seconds() / 3600
        
        return {
            'table': table,
            'latest_record': latest_record,
            'age_hours': age_hours,
            'is_fresh': age_hours <= max_age_hours,
            'total_records': df['total_records'].iloc[0],
            'recent_records': df['recent_records'].iloc[0]
        }
    
    def check_null_rates(self, table: str, columns: List[str]) -> Dict[str, Any]:
        """Check null rates for specified columns"""
        null_checks = []
        for col in columns:
            null_checks.append(f"AVG(CASE WHEN {col} IS NULL THEN 1.0 ELSE 0.0 END) as {col}_null_rate")
        
        query = f"""
        SELECT 
            COUNT(*) as total_records,
            {', '.join(null_checks)}
        FROM {table}
        """
        
        with self.get_connection() as conn:
            df = pd.read_sql(query, conn)
        
        return df.to_dict('records')[0]
    
    def check_referential_integrity(self, child_table: str, child_col: str, 
                                  parent_table: str, parent_col: str) -> Dict[str, Any]:
        """Check referential integrity between tables"""
        query = f"""
        SELECT 
            COUNT(*) as orphaned_records
        FROM {child_table} c
        LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
        WHERE p.{parent_col} IS NULL AND c.{child_col} IS NOT NULL
        """
        
        with self.get_connection() as conn:
            df = pd.read_sql(query, conn)
        
        return {
            'child_table': child_table,
            'parent_table': parent_table,
            'orphaned_records': df['orphaned_records'].iloc[0],
            'has_orphans': df['orphaned_records'].iloc[0] > 0
        }
    
    def run_all_checks(self) -> Dict[str, Any]:
        """Run comprehensive data quality checks"""
        checks = {}
        
        # Data freshness checks
        checks['freshness'] = {
            'users': self.check_data_freshness('users', 'created_at'),
            'orders': self.check_data_freshness('orders', 'created_at'),
            'products': self.check_data_freshness('products', 'updated_at')
        }
        
        # Null rate checks
        checks['null_rates'] = {
            'users': self.check_null_rates('users', ['email', 'first_name', 'last_name']),
            'products': self.check_null_rates('products', ['name', 'price', 'sku']),
            'orders': self.check_null_rates('orders', ['user_id', 'total_amount', 'status'])
        }
        
        # Referential integrity checks
        checks['referential_integrity'] = [
            self.check_referential_integrity('products', 'category_id', 'categories', 'id'),
            self.check_referential_integrity('orders', 'user_id', 'users', 'id'),
            self.check_referential_integrity('order_items', 'order_id', 'orders', 'id')
        ]
        
        return checks

# Usage example
if __name__ == "__main__":
    db_config = {
        'host': 'postgres',
        'database': 'ecommerce_dev',
        'user': 'postgres',
        'password': 'postgres123',
        'port': 5432
    }
    
    checker = DataQualityChecker(db_config)
    results = checker.run_all_checks()
    print("Data Quality Check Results:", results)