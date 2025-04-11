import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
    datefmt='%I:%M:%S'
)
logger = logging.getLogger("Results-Visualizer")

class ResultsVisualizer:
    """Class to visualize results from fault tolerance tests."""
    
    def __init__(self, results_dir: str):
        """Initialize the results visualizer.
        
        Args:
            results_dir: Directory containing test result logs
        """
        self.results_dir = results_dir
        self.results_data = []
        self._parse_result_files()
    
    def _parse_result_files(self):
        """Parse all result files in the directory."""
        if not os.path.exists(self.results_dir):
            logger.error(f"Results directory '{self.results_dir}' does not exist")
            return
        
        for filename in os.listdir(self.results_dir):
            if not filename.endswith('.log'):
                continue
            
            file_path = os.path.join(self.results_dir, filename)
            try:
                # Parse scenario name from filename
                scenario_name = filename.split('_')[0]
                for i in range(1, len(filename.split('_'))):
                    if filename.split('_')[i].endswith('.log'):
                        break
                    scenario_name += '_' + filename.split('_')[i]
                
                # Parse timestamp from filename
                timestamp_str = '_'.join(filename.split('_')[i:]).replace('.log', '')
                try:
                    timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                except ValueError:
                    timestamp = datetime.now()
                
                # Parse the log file
                with open(file_path, 'r') as f:
                    content = f.read()
                
                # Extract data using regex
                data = self._extract_data_from_log(content)
                data['scenario'] = scenario_name.replace('_', ' ').title()
                data['timestamp'] = timestamp
                data['file'] = filename
                
                self.results_data.append(data)
            except Exception as e:
                logger.error(f"Error parsing file {filename}: {e}")
        
        logger.info(f"Parsed {len(self.results_data)} result files")
    
    def _extract_data_from_log(self, content: str) -> Dict:
        """Extract metrics from log content."""
        data = {
            'stock_log_inconsistencies': 0,
            'payment_log_inconsistencies': 0,
            'stock_db_inconsistencies': 0, 
            'payment_db_inconsistencies': 0,
            'successful_checkouts': 0,
            'failed_checkouts': 0,
            'test_duration': 0,
            'services_killed': [],
            'kill_count': 0,
            'recovery_time': 0,
            'test_type': 'unknown'
        }
        
        # Extract test type
        test_type_match = re.search(r'Test type: (\w+)', content)
        if test_type_match:
            data['test_type'] = test_type_match.group(1)
        
        # Extract services killed
        services_match = re.search(r'Services to kill: (.+)', content)
        if services_match:
            services = services_match.group(1).strip()
            if services and services != 'none':
                data['services_killed'] = [s.strip() for s in services.split(',')]
        
        # Extract kill count
        kill_count_match = re.search(r'Kill count: (\d+)', content)
        if kill_count_match:
            data['kill_count'] = int(kill_count_match.group(1))
        
        # Extract recovery time
        recovery_time_match = re.search(r'Recovery time: (\d+)', content)
        if recovery_time_match:
            data['recovery_time'] = int(recovery_time_match.group(1))
        
        # Extract inconsistencies
        stock_log_match = re.search(r'Stock service inconsistencies in the logs: (-?\d+)', content)
        if stock_log_match:
            data['stock_log_inconsistencies'] = int(stock_log_match.group(1))
        
        payment_log_match = re.search(r'Payment service inconsistencies in the logs: (\d+)', content)
        if payment_log_match:
            data['payment_log_inconsistencies'] = int(payment_log_match.group(1))
        
        stock_db_match = re.search(r'Stock service inconsistencies in the database: (-?\d+)', content)
        if stock_db_match:
            data['stock_db_inconsistencies'] = int(stock_db_match.group(1))
        
        payment_db_match = re.search(r'Payment service inconsistencies in the database: (\d+)', content)
        if payment_db_match:
            data['payment_db_inconsistencies'] = int(payment_db_match.group(1))
        
        # Extract successful checkouts
        checkout_match = re.search(r'Successful checkouts: (\d+)', content)
        if checkout_match:
            data['successful_checkouts'] = int(checkout_match.group(1))
        
        # Extract failed checkouts - try to infer from the log
        fail_matches = re.findall(r'FAIL __OUR_LOG__', content)
        success_matches = re.findall(r'SUCCESS __OUR_LOG__', content)
        data['successful_checkouts'] = len(success_matches) if success_matches else data['successful_checkouts']
        data['failed_checkouts'] = len(fail_matches) if fail_matches else 0
        
        # Extract test duration if available
        duration_match = re.search(r'Test duration: ([\d.]+) seconds', content)
        if duration_match:
            data['test_duration'] = float(duration_match.group(1))
        
        return data
    
    def create_dataframe(self) -> pd.DataFrame:
        """Create a pandas DataFrame from the parsed results."""
        if not self.results_data:
            logger.warning("No results data available")
            return pd.DataFrame()
        
        df = pd.DataFrame(self.results_data)
        
        # Calculate success rate metrics
        if not df.empty and 'successful_checkouts' in df.columns and 'failed_checkouts' in df.columns:
            df['total_checkouts'] = df['successful_checkouts'] + df['failed_checkouts']
            df['success_rate'] = df.apply(
                lambda row: (row['successful_checkouts'] / row['total_checkouts'] * 100) 
                if row['total_checkouts'] > 0 else 0, 
                axis=1
            )
        
        return df
    
    def visualize_inconsistencies(self, output_file: str = None):
        """Visualize inconsistencies across different scenarios."""
        df = self.create_dataframe()
        if df.empty:
            return
        
        plt.figure(figsize=(12, 8))
        
        scenarios = df['scenario'].unique()
        x = range(len(scenarios))
        
        try:
            # Plot stock inconsistencies
            plt.subplot(2, 1, 1)
            plt.bar(x, df.groupby('scenario')['stock_log_inconsistencies'].mean(), 
                    width=0.4, label='Stock Log Inconsistencies')
            plt.bar([i+0.4 for i in x], df.groupby('scenario')['stock_db_inconsistencies'].mean(), 
                    width=0.4, label='Stock DB Inconsistencies')
            plt.xlabel('Scenario')
            plt.ylabel('Inconsistencies')
            plt.title('Stock Service Inconsistencies by Scenario')
            plt.xticks([i+0.2 for i in x], scenarios, rotation=45, ha='right')
            plt.legend()
            plt.grid(axis='y', linestyle='--', alpha=0.7)
            
            # Plot payment inconsistencies
            plt.subplot(2, 1, 2)
            plt.bar(x, df.groupby('scenario')['payment_log_inconsistencies'].mean(), 
                    width=0.4, label='Payment Log Inconsistencies')
            plt.bar([i+0.4 for i in x], df.groupby('scenario')['payment_db_inconsistencies'].mean(), 
                    width=0.4, label='Payment DB Inconsistencies')
            plt.xlabel('Scenario')
            plt.ylabel('Inconsistencies')
            plt.title('Payment Service Inconsistencies by Scenario')
            plt.xticks([i+0.2 for i in x], scenarios, rotation=45, ha='right')
            plt.legend()
            plt.grid(axis='y', linestyle='--', alpha=0.7)
            
            plt.tight_layout()
            
            if output_file:
                plt.savefig(output_file)
                logger.info(f"Saved inconsistencies visualization to {output_file}")
            else:
                plt.show()
        except Exception as e:
            logger.error(f"Error creating inconsistencies visualization: {e}")
    
    def visualize_checkout_success_rate(self, output_file: str = None):
        """Visualize checkout success rates across different scenarios."""
        df = self.create_dataframe()
        if df.empty:
            return
        
        try:
            plt.figure(figsize=(12, 6))
            
            scenarios = df['scenario'].unique()
            x = range(len(scenarios))
            
            # Calculate mean success rate for each scenario
            success_rates = []
            for scenario in scenarios:
                scenario_df = df[df['scenario'] == scenario]
                if 'success_rate' in scenario_df.columns:
                    success_rates.append(scenario_df['success_rate'].mean())
                else:
                    success_rates.append(0)
            
            # Plot success rate
            plt.bar(x, success_rates, width=0.6)
            plt.xlabel('Scenario')
            plt.ylabel('Success Rate (%)')
            plt.title('Checkout Success Rate by Scenario')
            plt.xticks(x, scenarios, rotation=45, ha='right')
            plt.grid(axis='y', linestyle='--', alpha=0.7)
            
            # Add value labels on bars
            for i, v in enumerate(success_rates):
                if not pd.isna(v):  # Check for NaN
                    plt.text(i, v + 1, f"{v:.1f}%", ha='center')
            
            plt.tight_layout()
            
            if output_file:
                plt.savefig(output_file)
                logger.info(f"Saved success rate visualization to {output_file}")
            else:
                plt.show()
        except Exception as e:
            logger.error(f"Error creating success rate visualization: {e}")
    
    def generate_report(self, output_file: str = None):
        """Generate a complete test report."""
        df = self.create_dataframe()
        if df.empty:
            logger.warning("No data available to generate report")
            return
        
        try:
            report = []
            report.append("# Fault Tolerance Test Report")
            report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            
            # Summary statistics
            report.append("## Summary Statistics")
            report.append(f"Total test scenarios: {len(df['scenario'].unique())}")
            report.append(f"Total tests run: {len(df)}")
            report.append(f"Average stock inconsistencies: {df['stock_db_inconsistencies'].mean():.2f}")
            report.append(f"Average payment inconsistencies: {df['payment_db_inconsistencies'].mean():.2f}")
            
            # Calculate total success rate
            total_success = df['successful_checkouts'].sum()
            total_failed = df['failed_checkouts'].sum()
            success_rate = (total_success / (total_success + total_failed)) * 100 if (total_success + total_failed) > 0 else 0
            report.append(f"Overall checkout success rate: {success_rate:.2f}%")
            report.append("")
            
            # Scenario details
            report.append("## Scenario Results")
            for scenario in df['scenario'].unique():
                scenario_df = df[df['scenario'] == scenario]
                report.append(f"### {scenario}")
                
                # Services killed
                if 'services_killed' in scenario_df and len(scenario_df['services_killed']) > 0:
                    services_killed = scenario_df['services_killed'].iloc[0]
                    if services_killed:
                        report.append(f"Services killed: {', '.join(services_killed)}")
                        report.append(f"Kill count: {scenario_df['kill_count'].iloc[0]}")
                        report.append(f"Recovery time: {scenario_df['recovery_time'].iloc[0]} seconds")
                    else:
                        report.append("No services were killed (baseline)")
                else:
                    report.append("No services were killed (baseline)")
                
                # Test results
                scenario_success = scenario_df['successful_checkouts'].mean()
                scenario_failed = scenario_df['failed_checkouts'].mean()
                scenario_rate = (scenario_success / (scenario_success + scenario_failed)) * 100 if (scenario_success + scenario_failed) > 0 else 0
                
                report.append(f"Successful checkouts: {scenario_success:.1f}")
                report.append(f"Failed checkouts: {scenario_failed:.1f}")
                report.append(f"Success rate: {scenario_rate:.2f}%")
                report.append(f"Stock inconsistencies: {scenario_df['stock_db_inconsistencies'].mean():.2f}")
                report.append(f"Payment inconsistencies: {scenario_df['payment_db_inconsistencies'].mean():.2f}")
                report.append("")
            
            # Observations and recommendations
            report.append("## Observations and Recommendations")
            
            # Find the worst performing scenario if possible
            try:
                scenario_success_rates = {}
                for scenario in df['scenario'].unique():
                    scenario_df = df[df['scenario'] == scenario]
                    success = scenario_df['successful_checkouts'].sum()
                    failed = scenario_df['failed_checkouts'].sum()
                    rate = (success / (success + failed)) * 100 if (success + failed) > 0 else 0
                    scenario_success_rates[scenario] = rate
                
                if scenario_success_rates:
                    worst_scenario = min(scenario_success_rates, key=scenario_success_rates.get)
                    report.append(f"* The scenario with the worst performance was '{worst_scenario}'.")
            except Exception as e:
                report.append(f"* Could not determine worst scenario: {e}")
            
            # Check for significant inconsistencies
            high_inconsistency = df['stock_db_inconsistencies'].max() > 10 or df['payment_db_inconsistencies'].max() > 10
            if high_inconsistency:
                report.append("* Significant inconsistencies were detected in the database state.")
                report.append("* Recommendation: Implement stronger transactional guarantees or retry mechanisms.")
            
            # Check recovery effectiveness
            recovery_scenarios = df[df['recovery_time'] > 0]
            if not recovery_scenarios.empty:
                try:
                    recovery_rates = []
                    for scenario in recovery_scenarios['scenario'].unique():
                        scenario_df = recovery_scenarios[recovery_scenarios['scenario'] == scenario]
                        success = scenario_df['successful_checkouts'].sum()
                        failed = scenario_df['failed_checkouts'].sum()
                        rate = (success / (success + failed)) * 100 if (success + failed) > 0 else 0
                        recovery_rates.append(rate)
                    
                    non_recovery_scenarios = df[df['recovery_time'] == 0]
                    non_recovery_rates = []
                    for scenario in non_recovery_scenarios['scenario'].unique():
                        scenario_df = non_recovery_scenarios[non_recovery_scenarios['scenario'] == scenario]
                        success = scenario_df['successful_checkouts'].sum()
                        failed = scenario_df['failed_checkouts'].sum()
                        rate = (success / (success + failed)) * 100 if (success + failed) > 0 else 0
                        non_recovery_rates.append(rate)
                    
                    avg_success_with_recovery = sum(recovery_rates) / len(recovery_rates) if recovery_rates else 0
                    avg_success_without_recovery = sum(non_recovery_rates) / len(non_recovery_rates) if non_recovery_rates else 0
                    
                    if avg_success_with_recovery > avg_success_without_recovery:
                        report.append("* Service recovery had a positive impact on system consistency.")
                    else:
                        report.append("* Service recovery did not significantly improve system consistency.")
                        report.append("* Recommendation: Review the recovery mechanism to ensure proper state restoration.")
                except Exception as e:
                    report.append(f"* Could not analyze recovery effectiveness: {e}")
            
            # Output the report
            report_text = "\n".join(report)
            
            if output_file:
                with open(output_file, 'w') as f:
                    f.write(report_text)
                logger.info(f"Report saved to {output_file}")
            else:
                print(report_text)
            
            return report_text
        
        except Exception as e:
            logger.error(f"Error generating report: {e}")
            return None

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Visualize fault tolerance test results')
    parser.add_argument('--results-dir', default='results', help='Directory containing test result logs')
    parser.add_argument('--output-dir', default='visualizations', help='Directory to save visualizations')
    parser.add_argument('--report', action='store_true', help='Generate a comprehensive report')
    
    return parser.parse_args()

def main():
    """Main function."""
    args = parse_arguments()
    
    # Create output directory if it doesn't exist
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    
    visualizer = ResultsVisualizer(args.results_dir)
    
    # Create visualizations
    visualizer.visualize_inconsistencies(
        output_file=os.path.join(args.output_dir, 'inconsistencies.png')
    )
    
    visualizer.visualize_checkout_success_rate(
        output_file=os.path.join(args.output_dir, 'success_rate.png')
    )
    
    # Generate report if requested
    if args.report:
        visualizer.generate_report(
            output_file=os.path.join(args.output_dir, 'fault_tolerance_report.md')
        )
    
    logger.info(f"All visualizations saved to {args.output_dir}")
    return 0

if __name__ == "__main__":
    sys.exit(main())