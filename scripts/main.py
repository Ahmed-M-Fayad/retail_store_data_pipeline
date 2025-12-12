"""
MAIN ORCHESTRATOR
Purpose: Coordinate the complete data pipeline from check to SQL loading
Workflow: Check → Transform → Load
"""

import sys
import os
import time
from datetime import datetime
import yaml

# Ensure src module can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.extract import data_quality_check
from src.transform import transform_pipeline
from src.load import sql_loader
from src.utils.config_loader import load_config

# ============================================================================
# CONFIGURATION
# ============================================================================

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


def run_pipeline_step(step_func, step_name):
    """Run a pipeline step function and capture time"""
    print(f"\n▶️  Executing: {step_name}")
    print("-" * 80)
    
    start_time = time.time()
    
    try:
        # Run the function
        result = step_func()
        
        elapsed_time = time.time() - start_time
        
        success = result is not None and result is not False
        
        print("-" * 80)
        if success:
            print(f"✓ {step_name} completed successfully")
        else:
            print(f"✗ {step_name} reported failure or invalid result")
            
        print(f"  Execution time: {elapsed_time:.2f} seconds")
        
        return success, elapsed_time
    
    except Exception as e:
        elapsed_time = time.time() - start_time
        
        print("-" * 80)
        print(f"✗ {step_name} failed with exception!")
        print(f"  Error: {e}")
        print(f"  Execution time: {elapsed_time:.2f} seconds")
        
        import traceback
        traceback.print_exc()
        
        return False, elapsed_time


def prompt_continue(step_name):
    """Prompt user to continue to next step"""
    if AUTO_CONTINUE:
        return True
    
    print(f"\n{'='*80}")
    response = input(f"Continue to {step_name}? (y/n): ").strip().lower()
    return response in ['y', 'yes']


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
        
        success, elapsed = run_pipeline_step(data_quality_check.main, "Data Quality Check")
        
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
        
        success, elapsed = run_pipeline_step(transform_pipeline.main, "Data Transformation")
        
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
        
        success, elapsed = run_pipeline_step(sql_loader.main, "SQL Server Loading")
        
        execution_log['steps'].append({
            'step': 'load',
            'success': success,
            'time': elapsed
        })
        
        if not success:
            print("\n❌ SQL loading failed")
            execution_log['success'] = False
            # Don't return False - partial success logic as before?
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
    
    # Overall status
    print("\n" + "="*80)
    if execution_log['success'] and successful_steps == total_steps:
        print("✅ PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*80)
        print("\n🎉 All steps executed successfully!")
    elif successful_steps > 0:
        print("⚠️  PIPELINE PARTIALLY COMPLETED")
        print("="*80)
        print(f"\n   {successful_steps} of {total_steps} steps completed")
    else:
        print("❌ PIPELINE FAILED")
        print("="*80)
        print("\n   No steps completed successfully")
    
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