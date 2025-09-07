import time
import requests
from datetime import datetime, timedelta
import logging

class DataMonitoring:
    def __init__(self, alert_webhook_url=None):
        self.alert_webhook_url = alert_webhook_url
        self.setup_logging()
    
    def setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def send_alert(self, title: str, message: str, severity: str = "warning"):
        """Send alert via webhook (Slack, Teams, etc.)"""
        if not self.alert_webhook_url:
            self.logger.warning(f"Alert: {title} - {message}")
            return
        
        payload = {
            "text": f"🚨 Data Alert: {title}",
            "attachments": [
                {
                    "color": "danger" if severity == "critical" else "warning",
                    "fields": [
                        {"title": "Message", "value": message, "short": False},
                        {"title": "Severity", "value": severity.upper(), "short": True},
                        {"title": "Time", "value": datetime.now().isoformat(), "short": True}
                    ]
                }
            ]
        }
        
        try:
            response = requests.post(self.alert_webhook_url, json=payload)
            response.raise_for_status()
        except Exception as e:
            self.logger.error(f"Failed to send alert: {e}")
    
    def check_pipeline_health(self):
        """Monitor ETL pipeline health"""
        # Check if daily pipeline ran successfully
        try:
            # This would typically check your Airflow API or database
            last_run = self.get_last_pipeline_run()
            if not last_run or last_run < datetime.now() - timedelta(hours=26):
                self.send_alert(
                    "Pipeline Failure", 
                    "Daily analytics pipeline hasn't run in over 24 hours",
                    "critical"
                )
        except Exception as e:
            self.send_alert("Monitoring Error", f"Failed to check pipeline health: {e}")
    
    def check_data_quality_metrics(self):
        """Monitor data quality KPIs"""
        # Example: Check for data anomalies
        try:
            # Revenue anomaly detection
            current_revenue = self.get_daily_revenue()
            avg_revenue = self.get_average_daily_revenue(days=30)
            
            if current_revenue < avg_revenue * 0.5:  # 50% drop
                self.send_alert(
                    "Revenue Anomaly",
                    f"Daily revenue ({current_revenue}) is significantly below average ({avg_revenue})",
                    "critical"
                )
            elif current_revenue > avg_revenue * 2:  # 100% increase
                self.send_alert(
                    "Revenue Spike",
                    f"Daily revenue ({current_revenue}) is significantly above average ({avg_revenue})",
                    "warning"
                )
        except Exception as e:
            self.send_alert("Data Quality Check Failed", f"Error: {e}")

# Usage
monitor = DataMonitoring(alert_webhook_url="YOUR_SLACK_WEBHOOK")
monitor.check_pipeline_health()
monitor.check_data_quality_metrics()