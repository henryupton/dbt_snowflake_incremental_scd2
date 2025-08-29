#!/bin/bash

# SCD2 Sequential Testing Script
# 
# This script performs sequential dbt runs to test SCD2 incremental logic.
# After each run, it executes a dbt test to verify the table matches the expected result.
#
# Usage:
#   ./test_scd2_sequence.sh [start_num] [end_num] [model_name]
#   ./test_scd2_sequence.sh 1 3 customers_scd2

# Note: Not using "set -e" to allow handling of dbt command failures gracefully

# Configuration
DEFAULT_START=1
DEFAULT_END=5
DEFAULT_MODEL="customers_scd2"
DBT_PROJECT_DIR="integration_tests"

# Initialize results array
RESULTS=()

# Parse arguments
START_NUM=${1:-$DEFAULT_START}
END_NUM=${2:-$DEFAULT_END}
TARGET_MODEL=${3:-$DEFAULT_MODEL}
SOURCE_MODEL=${4:-$DEFAULT_SOURCE_MODEL}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to run dbt command with error handling
run_dbt() {
    local cmd="$1"
    local iteration="$2"
    
    log_info "Running: dbt $cmd"
    
    # Change to the dbt project directory and run command
    if (cd "$DBT_PROJECT_DIR" && eval "dbt $cmd"); then
        return 0
    else
        log_error "dbt command failed for iteration $iteration"
        return 1
    fi
}

# Function to log results (in-memory only)
log_result() {
    local iteration=$1
    local input_seed=$2
    local expected_seed=$3
    local status=$4
    
    # Store results in array for summary display
    RESULTS+=("$iteration,$input_seed,$expected_seed,$status")
}

# Function to display current table state
display_table_state() {
    local iteration=$1
    local model_name=$2
    
    log_info "📊 Displaying current state of $model_name after iteration $iteration"
    echo "============================================================"
    
    # Use dbt show --inline to display the current table state
    local show_query="select * from {{ ref('$model_name') }} order by customer_id, _valid_from"
    
    if (cd "$DBT_PROJECT_DIR" && dbt show --inline "$show_query" --vars "{iteration: $iteration}"); then
        log_success "Table state displayed successfully"
    else
        log_warning "Failed to display table state - continuing anyway"
    fi
    
    echo "============================================================"
}

# Function to display comparison when test fails
display_comparison_on_failure() {
    local iteration=$1
    local actual_model=$2
    local expected_seed=$3
    
    log_error "🔍 COMPARISON VIEW - Actual vs Expected for iteration $iteration"
    echo "============================================================"
    
    # Display actual table state
    log_info "📊 ACTUAL TABLE STATE ($actual_model):"
    echo "------------------------------------------------------------"
    local actual_query="select * from {{ ref('$actual_model') }} order by customer_id, _valid_from"
    
    if (cd "$DBT_PROJECT_DIR" && dbt show --inline "$actual_query" --limit 100 --vars "{iteration: $iteration}"); then
        log_info "Actual table displayed successfully"
    else
        log_warning "Failed to display actual table state"
    fi
    
    echo "------------------------------------------------------------"

    echo "============================================================"
}

# Function to run a single iteration
run_iteration() {
    local iteration=$1
    local input_seed="customers_raw_${iteration}"
    local expected_seed="customers_scd2_result_${iteration}"
    
    log_info "=========================================="
    log_info "🔄 Starting Iteration $iteration"
    log_info "Input: $input_seed → Expected: $expected_seed"
    log_info "=========================================="
    
    # Initialize status variables
    local run_status="FAILED"
    local test_status="FAILED"
    local overall_status="FAILED"
    
    # Step 1: Run dbt (seeds + conditional model + SCD2 model)
    log_info "Step 1: Running dbt for iteration $iteration"
    if run_dbt "run --vars \"{iteration: $iteration}\"" "$iteration"; then
        run_status="PASSED"
        log_success "dbt run completed successfully"
        
        # Display current table state
        display_table_state "$iteration" "$TARGET_MODEL"
    else
        log_error "dbt run failed for iteration $iteration"
        log_result "$iteration" "$input_seed" "$expected_seed" "FAILED"
        return 1
    fi
    
    # Step 2: Run comparison test
    log_info "Step 2: Running table comparison test for iteration $iteration"
    if run_dbt "test --select test_scd2_table_matches_expected customers_scd2+ --vars \"{iteration: $iteration}\"" "$iteration"; then
        test_status="PASSED"
        log_success "✅ Table matches expected result!"
    else
        test_status="FAILED"
        log_error "❌ Table does not match expected result"
        
        # Display both actual and expected states when test fails
        display_comparison_on_failure "$iteration" "$TARGET_MODEL" "$expected_seed"
    fi
    
    # Determine overall status
    if [[ "$run_status" == "PASSED" && "$test_status" == "PASSED" ]]; then
        overall_status="PASSED"
        log_success "✅ Iteration $iteration completed successfully"
    else
        overall_status="FAILED"
        log_error "❌ Iteration $iteration failed"
    fi
    
    # Log results
    log_result "$iteration" "$input_seed" "$expected_seed" "$overall_status"
    
    # Return failure if iteration failed
    if [[ "$overall_status" == "FAILED" ]]; then
        return 1
    else
        return 0
    fi
}

# Function to print summary
print_summary() {
    log_info "=========================================="
    log_info "📊 TEST SUMMARY"
    log_info "=========================================="
    
    if [[ ${#RESULTS[@]} -gt 0 ]]; then
        # Count results from in-memory array
        local total_tests=${#RESULTS[@]}
        local passed_tests=0
        
        for result in "${RESULTS[@]}"; do
            if [[ "$result" == *",PASSED" ]]; then
                ((passed_tests++))
            fi
        done
        
        local failed_tests=$((total_tests - passed_tests))
        
        log_info "Total iterations: $total_tests"
        log_success "Passed: $passed_tests"
        
        if [[ $failed_tests -gt 0 ]]; then
            log_error "Failed: $failed_tests"
        else
            log_success "Failed: $failed_tests"
        fi
        
        echo
        log_info "Detailed results:"
        printf "%-10s %-20s %-25s %-10s\n" "Iteration" "Input" "Expected" "Status"
        printf "%-10s %-20s %-25s %-10s\n" "--------" "----" "--------" "------"
        
        for result in "${RESULTS[@]}"; do
            IFS=',' read -r iteration input expected status <<< "$result"
            printf "%-10s %-20s %-25s %-10s\n" "$iteration" "$input" "$expected" "$status"
        done
        
        # Overall status
        if [[ $failed_tests -eq 0 ]]; then
            echo
            log_success "🎉 All iterations passed!"
            return 0
        else
            echo
            log_error "⚠️  $failed_tests iteration(s) failed"
            return 1
        fi
    else
        log_error "No results found"
        return 1
    fi
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [start_num] [end_num] [target_model] [source_model]"
    echo
    echo "Arguments:"
    echo "  start_num     Starting iteration number (default: $DEFAULT_START)"
    echo "  end_num       Ending iteration number (default: $DEFAULT_END)"
    echo "  target_model  Target SCD2 model name (default: $DEFAULT_MODEL)"
    echo "  source_model  Source model with conditional logic (default: $DEFAULT_SOURCE_MODEL)"
    echo
    echo "Examples:"
    echo "  $0                           # Test iterations 1-3 with default model"
    echo "  $0 1 5                       # Test iterations 1-5"
    echo "  $0 1 3 my_scd2_model         # Test iterations 1-3 with custom model"
    echo
    echo "The script expects:"
    echo "  - Seeds named like: customers_raw_1.csv, customers_raw_2.csv, etc."
    echo "  - Expected seeds: expected_customers_1.csv, expected_customers_2.csv, etc."
    echo "  - A conditional source model that selects seeds based on {{ var('iteration') }}"
    echo
}

# Main execution
main() {
    # Show usage if help requested
    if [[ "$1" == "-h" || "$1" == "--help" ]]; then
        show_usage
        exit 0
    fi
    
    # Validate arguments
    if ! [[ "$START_NUM" =~ ^[0-9]+$ ]] || ! [[ "$END_NUM" =~ ^[0-9]+$ ]]; then
        log_error "Start and end numbers must be integers"
        show_usage
        exit 1
    fi
    
    if [[ $START_NUM -gt $END_NUM ]]; then
        log_error "Start number must be less than or equal to end number"
        exit 1
    fi
    
    # Check if dbt project directory exists
    if [[ ! -d "$DBT_PROJECT_DIR" ]]; then
        log_error "dbt project directory '$DBT_PROJECT_DIR' not found"
        exit 1
    fi
    
    # Setup
    log_info "🚀 Starting SCD2 Sequential Testing"
    log_info "Iterations: $START_NUM to $END_NUM"
    log_info "Target model: $TARGET_MODEL"
    log_info "Source model: $SOURCE_MODEL"
    log_info "dbt project: $DBT_PROJECT_DIR"
    
    # Run seeds first
#    log_info "Running seeds..."
#    if run_dbt "seed  --full-refresh" "setup"; then
#        log_success "Seeds loaded successfully"
#    else
#        log_warning "Failed to load seeds - continuing anyway"
#    fi
    
    # Clean up target model first
    log_info "Cleaning target model with full refresh..."
    if run_dbt "run --select $TARGET_MODEL --full-refresh --vars \"{iteration: 1}\"" "cleanup"; then
        log_success "Target model cleaned successfully"
    else
        log_warning "Failed to clean target model - continuing anyway"
    fi
    
    # Run iterations
    local failed_iterations=0
    for i in $(seq $START_NUM $END_NUM); do
        if ! run_iteration "$i"; then
            ((failed_iterations++))
        fi
        echo # Add spacing between iterations
    done
    
    # Print summary
    print_summary
    exit_code=$?
    
    # Final message
    if [[ $exit_code -eq 0 ]]; then
        log_success "🎉 All SCD2 tests completed successfully!"
    else
        log_error "⚠️  SCD2 testing completed with failures"
    fi
    
    exit $exit_code
}

# Run main function
main "$@"