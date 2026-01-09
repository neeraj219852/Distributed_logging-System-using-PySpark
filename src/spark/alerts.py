"""
Alert System Module
Implements configurable alerting based on thresholds
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional
from pyspark.sql import DataFrame
import sys
import os

# Handle imports for both direct execution and module import
try:
    from src.spark.spark_session import load_config
    from src.spark.analytics import compute_error_rate, compute_error_count
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
    from src.spark.spark_session import load_config
    from src.spark.analytics import compute_error_rate, compute_error_count

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AlertManager:
    """Manages alert generation and history"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize AlertManager with configuration
        
        Args:
            config_path: Path to configuration file
        """
        self.config = load_config(config_path)
        self.alert_config = self.config.get('alerts', {})
        self.alert_history: List[Dict] = []
        
        # Setup alert log file
        self.alert_log_file = "reports/alerts.log"
        import os
        os.makedirs("reports", exist_ok=True)
    
    def log_alert(self, alert_type: str, message: str, severity: str = "WARNING"):
        """
        Log alert to console and file
        
        Args:
            alert_type: Type of alert
            message: Alert message
            severity: Alert severity level
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        alert_entry = {
            "timestamp": timestamp,
            "type": alert_type,
            "message": message,
            "severity": severity
        }
        
        self.alert_history.append(alert_entry)
        
        # Print to console
        alert_str = f"[{timestamp}] [{severity}] {alert_type}: {message}"
        logger.warning(alert_str)
        print(f"\n⚠️  ALERT: {alert_str}\n")
        
        # Write to file
        try:
            with open(self.alert_log_file, 'a') as f:
                f.write(f"{alert_str}\n")
        except Exception as e:
            logger.error(f"Error writing to alert log: {e}")
    
    def check_error_rate_alert(self, df: DataFrame) -> bool:
        """
        Check if error rate exceeds threshold
        
        Args:
            df: Parsed log DataFrame
            
        Returns:
            True if alert was triggered
        """
        threshold = self.alert_config.get('error_rate_threshold', 0.1)
        error_rate = compute_error_rate(df)
        
        if error_rate > threshold:
            self.log_alert(
                "HIGH_ERROR_RATE",
                f"Error rate {error_rate:.2%} exceeds threshold {threshold:.2%}",
                "CRITICAL"
            )
            return True
        return False
    
    def check_error_count_alert(self, df: DataFrame) -> bool:
        """
        Check if error count exceeds threshold
        
        Args:
            df: Parsed log DataFrame
            
        Returns:
            True if alert was triggered
        """
        threshold = self.alert_config.get('error_count_threshold', 100)
        error_count = compute_error_count(df)
        
        if error_count > threshold:
            self.log_alert(
                "HIGH_ERROR_COUNT",
                f"Total error count {error_count} exceeds threshold {threshold}",
                "CRITICAL"
            )
            return True
        return False
    
    def check_critical_errors_alert(self, df: DataFrame) -> bool:
        """
        Check if critical errors appear
        
        Args:
            df: Parsed log DataFrame
            
        Returns:
            True if alert was triggered
        """
        threshold = self.alert_config.get('critical_error_count', 5)
        
        from pyspark.sql.functions import col, count
        
        critical_errors = df.filter(
            (col("log_level") == "ERROR") & 
            (col("severity") >= 3)
        ).count()
        
        if critical_errors >= threshold:
            self.log_alert(
                "CRITICAL_ERRORS",
                f"Found {critical_errors} critical errors (threshold: {threshold})",
                "CRITICAL"
            )
            return True
        return False
    
    def check_all_alerts(self, df: DataFrame) -> List[Dict]:
        """
        Run all alert checks
        
        Args:
            df: Parsed log DataFrame
            
        Returns:
            List of triggered alerts
        """
        logger.info("Running alert checks...")
        
        alerts_triggered = []
        
        # Check error rate
        if self.check_error_rate_alert(df):
            alerts_triggered.append({
                "type": "HIGH_ERROR_RATE",
                "timestamp": datetime.now().isoformat()
            })
        
        # Check error count
        if self.check_error_count_alert(df):
            alerts_triggered.append({
                "type": "HIGH_ERROR_COUNT",
                "timestamp": datetime.now().isoformat()
            })
        
        # Check critical errors
        if self.check_critical_errors_alert(df):
            alerts_triggered.append({
                "type": "CRITICAL_ERRORS",
                "timestamp": datetime.now().isoformat()
            })
        
        if not alerts_triggered:
            logger.info("No alerts triggered. System is healthy.")
        
        return alerts_triggered
    
    def get_recent_alerts(self, limit: int = 20) -> List[Dict]:
        """
        Get recent alerts from history
        
        Args:
            limit: Maximum number of alerts to return
            
        Returns:
            List of recent alerts
        """
        return self.alert_history[-limit:]
    
    def get_alert_summary(self) -> Dict:
        """
        Get summary of alert history
        
        Returns:
            Dictionary with alert statistics
        """
        total_alerts = len(self.alert_history)
        
        alert_types = {}
        for alert in self.alert_history:
            alert_type = alert.get("type", "UNKNOWN")
            alert_types[alert_type] = alert_types.get(alert_type, 0) + 1
        
        return {
            "total_alerts": total_alerts,
            "alert_types": alert_types,
            "recent_alerts": self.get_recent_alerts(10)
        }


def check_alerts(df: DataFrame, config_path: str = "config/config.yaml") -> AlertManager:
    """
    Convenience function to check alerts
    
    Args:
        df: Parsed log DataFrame
        config_path: Path to configuration file
        
    Returns:
        AlertManager instance
    """
    alert_manager = AlertManager(config_path)
    alert_manager.check_all_alerts(df)
    return alert_manager


if __name__ == "__main__":
    from ingest_logs import ingest_logs
    from parse_logs import parse_logs
    
    from spark_session import get_spark_session
    
    spark = get_spark_session()
    df_raw = ingest_logs()
    df_parsed = parse_logs(df_raw)
    
    # Check alerts
    alert_manager = check_alerts(df_parsed)
    
    # Display alert summary
    summary = alert_manager.get_alert_summary()
    print(f"\nAlert Summary: {summary}")

