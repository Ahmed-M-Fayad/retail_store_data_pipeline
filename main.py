"""
MAIN ORCHESTRATOR
Purpose: Coordinate the complete data pipeline from check to SQL loading
Workflow: Check → Transform → Load
"""

import subprocess
import sys
import os
import time
import json
import yaml
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

# Script paths
CHECK_SCRIPT = "data_quality_check.py"
TRANSFORM_SCRIPT = "transform_pipeline.py"
LOAD_SCRIPT = "sql_loader.py"
CONFIG_FILE = "pipeline_config.yaml"

# Execution options
SKIP_CHECK = False          # Set to True to skip quality check
SKIP_TRANSFORM = False      # Set to True to skip transformation
SKIP_LOAD = False           # Set to True to skip SQL loading
AUTO_CONTINUE = True        # Set to False to prompt between steps

print("="*80)
print("DATA PIPELINE ORCHESTRATOR")
print("="*80)
print("\nWorkflow: CHECK → TRANSFORM → LOAD")
print("="*80)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def print_step_header(step_num, step_name, description):
    """Print formatted step header"""
    print("\n" + "="*80)
    print(f"STEP {step_num}: {step_name}")
    print("="*80)
    print(f"{description}")
    print("="*80)


def run_script(script_path, step_name):
    """Run a Python script and capture its output"""
    print(f"\n▶️  Executing: {script_path}")
    print("-" * 80)
    
    start_time = time.time()
    
    try:
        # Run the script
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=False,  # Show output in real-time
            text=True,
            check=True
        )
        
        elapsed_time = time.time() - start_time
        
        print("-" * 80)
        print(f"✓ {step_name} completed successfully")
        print(f"  Execution time: {elapsed_time:.2f} seconds")
        
        return True, elapsed_time
    
    except subprocess.CalledProcessError as e:
        elapsed_time = time.time() - start_time
        
        print("-" * 80)
        print(f"✗ {step_name} failed!")
        print(f"  Execution time: {elapsed_time:.2f} seconds")
        print(f"  Error code: {e.returncode}")
        
        return False, elapsed_time
    
    except FileNotFoundError:
        print("-" * 80)
        print(f"✗ Script not found: {script_path}")
        return False, 0


def check_script_exists(script_path):
    """Check if a script file exists"""
    if not os.path.exists(script_path):
        print(f"✗ ERROR: Script not found: {script_path}")
        return False
    return True


def prompt_continue(step_name):
    """Prompt user to continue to next step"""
    if AUTO_CONTINUE:
        return True
    
    print(f"\n{'='*80}")
    response = input(f"Continue to {step_name}? (y/n): ").strip().lower()
    return response in ['y', 'yes']


def load_config():
    """Load pipeline configuration if it exists"""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return yaml.safe_load(f)
        except:
            return None
    return None


def print_config_summary(config):
    """Print configuration summary"""
    if not config:
        return
    
    print("\n📊 Configuration Summary:")
    print(f"  Quality Score: {config['overall_quality_score']:.2f}%")
    print(f"  Check Time: {config['metadata']['check_timestamp']}")
    
    print("\n  Pipeline Steps:")
    for step, needed in config['pipeline_steps'].items():
        status = "✓ Execute" if needed else "⊗ Skip"
        print(f"    {step:30} : {status}")


# ============================================================================
# MAIN ORCHESTRATION
# ============================================================================

def main():
    """Orchestrate the complete data pipeline"""
    
    print("\n🚀 Starting Pipeline Orchestration...")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check all scripts exist
    print("\n" + "="*80)
    print("PRE-FLIGHT CHECK")
    print("="*80)
    
    scripts_ok = True
    scripts = [
        (CHECK_SCRIPT, "Data Quality Check"),
        (TRANSFORM_SCRIPT, "Transformation Pipeline"),
        (LOAD_SCRIPT, "SQL Server Loader")
    ]
    
    for script_path, script_name in scripts:
        if check_script_exists(script_path):
            print(f"  ✓ {script_name:30} : {script_path}")
        else:
            scripts_ok = False
            print(f"  ✗ {script_name:30} : {script_path} NOT FOUND")
    
    if not scripts_ok:
        print("\n✗ Pre-flight check failed!")
        print("   Please ensure all required scripts are in the current directory")
        return False
    
    print("\n✓ All scripts found")
    
    # Initialize tracking
    execution_log = {
        'start_time': datetime.now().isoformat(),
        'steps': [],
        'total_time': 0,
        'success': True
    }
    
    # ========================================================================
    # STEP 1: DATA QUALITY CHECK
    # ========================================================================
    
    if not SKIP_CHECK:
        print_step_header(1, "DATA QUALITY CHECK", 
                         "Analyzing data quality and generating configuration")
        
        success, elapsed = run_script(CHECK_SCRIPT, "Data Quality Check")
        
        execution_log['steps'].append({
            'step': 'check',
            'success': success,
            'time': elapsed
        })
        
        if not success:
            print("\n❌ Pipeline aborted due to check failure")
            execution_log['success'] = False
            return False
        
        # Load and display configuration
        config = load_config()
        print_config_summary(config)
        
        if not prompt_continue("Transformation"):
            print("\n⏸️  Pipeline paused by user")
            return False
    else:
        print("\n⊗ SKIPPING: Data Quality Check (disabled in configuration)")
        config = load_config()
    
    # ========================================================================
    # STEP 2: DATA TRANSFORMATION
    # ========================================================================
    
    if not SKIP_TRANSFORM:
        print_step_header(2, "DATA TRANSFORMATION",
                         "Cleaning and transforming data based on configuration")
        
        success, elapsed = run_script(TRANSFORM_SCRIPT, "Data Transformation")
        
        execution_log['steps'].append({
            'step': 'transform',
            'success': success,
            'time': elapsed
        })
        
        if not success:
            print("\n❌ Pipeline aborted due to transformation failure")
            execution_log['success'] = False
            return False
        
        if not prompt_continue("SQL Loading"):
            print("\n⏸️  Pipeline paused by user")
            return False
    else:
        print("\n⊗ SKIPPING: Data Transformation (disabled in configuration)")
    
    # ========================================================================
    # STEP 3: SQL SERVER LOADING
    # ========================================================================
    
    if not SKIP_LOAD:
        print_step_header(3, "SQL SERVER LOADING",
                         "Loading cleaned data into SQL Server database")
        
        success, elapsed = run_script(LOAD_SCRIPT, "SQL Server Loading")
        
        execution_log['steps'].append({
            'step': 'load',
            'success': success,
            'time': elapsed
        })
        
        if not success:
            print("\n❌ SQL loading failed")
            print("   However, cleaned data is available in cleaned_data/ directory")
            execution_log['success'] = False
            # Don't return False - partial success
    else:
        print("\n⊗ SKIPPING: SQL Server Loading (disabled in configuration)")
    
    # ========================================================================
    # FINAL SUMMARY
    # ========================================================================
    
    execution_log['end_time'] = datetime.now().isoformat()
    execution_log['total_time'] = sum(step['time'] for step in execution_log['steps'])
    
    print("\n" + "="*80)
    print("PIPELINE EXECUTION SUMMARY")
    print("="*80)
    
    print("\n⏱️  Execution Timeline:")
    for i, step_info in enumerate(execution_log['steps'], 1):
        status = "✓" if step_info['success'] else "✗"
        step_name = step_info['step'].upper()
        print(f"  {status} Step {i} ({step_name:15}): {step_info['time']:.2f}s")
    
    print(f"\n  Total execution time: {execution_log['total_time']:.2f} seconds")
    
    # Results summary
    print("\n📊 Results:")
    successful_steps = sum(1 for s in execution_log['steps'] if s['success'])
    total_steps = len(execution_log['steps'])
    
    print(f"  Steps completed: {successful_steps}/{total_steps}")
    
    if os.path.exists("cleaned_data/"):
        csv_files = [f for f in os.listdir("cleaned_data/") if f.endswith('.csv')]
        print(f"  Cleaned files: {len(csv_files)} CSV files in cleaned_data/")
    
    # Overall status
    print("\n" + "="*80)
    if execution_log['success'] and successful_steps == total_steps:
        print("✅ PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*80)
        print("\n🎉 All steps executed successfully!")
        print("   ✓ Data quality checked")
        print("   ✓ Data cleaned and transformed")
        print("   ✓ Data loaded to SQL Server")
    elif successful_steps > 0:
        print("⚠️  PIPELINE PARTIALLY COMPLETED")
        print("="*80)
        print(f"\n   {successful_steps} of {total_steps} steps completed")
        print("   Check the logs above for details")
    else:
        print("❌ PIPELINE FAILED")
        print("="*80)
        print("\n   No steps completed successfully")
        print("   Check the error messages above")
    
    print("="*80)
    print(f"\nExecution completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    return execution_log['success']


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    print("\n" + "="*80)
    print("RETAIL DATA PIPELINE - MAIN ORCHESTRATOR")
    print("="*80)
    
    # Display configuration
    print("\n⚙️  Configuration:")
    print(f"  Skip Check: {SKIP_CHECK}")
    print(f"  Skip Transform: {SKIP_TRANSFORM}")
    print(f"  Skip Load: {SKIP_LOAD}")
    print(f"  Auto Continue: {AUTO_CONTINUE}")
    
    # Prompt to start
    if not AUTO_CONTINUE:
        print("\n" + "="*80)
        response = input("Start pipeline execution? (y/n): ").strip().lower()
        if response not in ['y', 'yes']:
            print("\n❌ Pipeline execution cancelled by user")
            sys.exit(0)
    
    # Execute pipeline
    success = main()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)