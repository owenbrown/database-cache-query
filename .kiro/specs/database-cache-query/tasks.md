# Implementation Plan

- [x] 1. Set up project structure and configuration
  - Create config.py with CACHED_DATA_FOLDER configuration
  - Set up directory structure for the project modules
  - Create __init__.py files for proper Python package structure
  - _Requirements: 5.7_

- [x] 2. Implement JSON parsing utilities
  - [x] 2.1 Create json_parser.py with column name parsing
    - Implement parse_column_name() to detect dot notation (e.g., "vendor.address.city")
    - Write unit tests for various column name formats
    - _Requirements: 6.1_
  
  - [x] 2.2 Implement JSON value extraction
    - Create extract_json_value() function to parse JSON strings and extract nested values
    - Handle both string JSON and dictionary inputs
    - Write comprehensive tests for nested JSON extraction and error cases
    - _Requirements: 6.2, 6.3, 6.4_

- [x] 3. Implement cache management functionality
  - [x] 3.1 Create basic cache file operations
    - Implement get_table_cache_path() to generate file paths from table names
    - Create directory creation logic for cache folder
    - Write tests for path generation and directory creation
    - _Requirements: 5.4, 5.7_
  
  - [x] 3.2 Implement cache data retrieval
    - Create get_cached_data() function to read Parquet files and identify incomplete IDs
    - Implement logic to check if all requested columns exist for each ID
    - Write tests for cache hit/miss scenarios and incomplete data detection
    - _Requirements: 2.1, 2.2, 2.3_
  
  - [x] 3.3 Implement cache data storage
    - Create store_data() function to write/update Parquet files using Polars
    - Implement insert-or-replace logic for incomplete rows
    - Handle schema evolution when new columns are added
    - Write tests for data storage and schema updates
    - _Requirements: 5.1, 5.2, 5.8_

- [x] 4. Implement batch processing with progress tracking
  - [x] 4.1 Create batch size calculation
    - Implement calculate_batch_size() with formula: max(100, ceil(missing_count / 100))
    - Ensure maximum of 100 batches and minimum batch size of 100 items
    - Write tests for various dataset sizes (100, 1000, 10000, 1M+ items)
    - _Requirements: 4.1, 4.4_
  
  - [x] 4.2 Implement batch fetching with progress
    - Create fetch_missing_data() function with tqdm progress tracking
    - Implement sequential batch processing to avoid overwhelming database
    - Add error handling for individual batch failures
    - Write tests for batch processing and progress tracking
    - _Requirements: 3.1, 3.2, 3.3, 4.2, 4.3_

- [x] 5. Implement main orchestration function
  - [x] 5.1 Create main get_data function
    - Implement the main API function that orchestrates cache lookup, fetching, and storage
    - Integrate all modules (cache_manager, batch_processor, json_parser)
    - Handle JSON column parsing and data transformation
    - _Requirements: 1.1, 1.2_
  
  - [x] 5.2 Add comprehensive error handling
    - Implement DataNotFoundError for missing IDs
    - Implement ColumnNotFoundError for unavailable columns
    - Add JSONParseError for JSON parsing failures
    - Write tests for all error scenarios
    - _Requirements: 1.3, 7.3, 7.4_
  
  - [x] 5.3 Add input validation
    - Validate ID list format and types
    - Validate column names and table_name format
    - Validate fetcher function signature
    - Write tests for input validation edge cases
    - _Requirements: 1.1, 1.3, 1.5, 1.6_

- [ ] 6. Create comprehensive test suite
  - [ ] 6.1 Write integration tests
    - Test complete end-to-end workflow with cache miss scenario
    - Test partial cache hit scenario with incomplete data
    - Test JSON column parsing in full workflow
    - _Requirements: 1.1, 2.4, 6.1_
  
  - [ ] 6.2 Write performance tests
    - Test with large datasets (100K+ IDs) to verify batch processing
    - Measure cache lookup performance with large Parquet files
    - Test memory usage during batch processing
    - _Requirements: 4.4, 5.2_
  
  - [ ] 6.3 Create mock fetcher functions for testing
    - Create test fetchers that simulate database responses
    - Include test cases for missing IDs and columns
    - Add test cases with JSON data for dot notation testing
    - _Requirements: 1.2, 7.3, 7.4_

- [ ] 7. Add file expansion and optimization
  - [ ] 7.1 Implement Parquet file size monitoring
    - Add logic to check file sizes and trigger expansion when needed
    - Implement expand_cache_files() function for splitting large files
    - Write tests for file expansion scenarios
    - _Requirements: 5.3_
  
  - [ ] 7.2 Optimize Polars operations
    - Reread code and update, as needed, to use most modern polars API and recommended usage 
    - If optimization requires increasing code length or complexity, note the optimization as code comment but to do not implement.
    - _Requirements: 5.2, 5.6_

- [x] 8. Create example usage and documentation
  - [x] 8.1 Write example usage script
    - Create example showing basic usage with mock data
    - Demonstrate JSON column parsing with real examples
    - Show batch processing with progress tracking
    - _Requirements: 1.1, 3.1, 6.1_
  
  - [x] 8.2 Add docstrings and type hints
    - Add comprehensive docstrings to all functions
    - Add proper type hints throughout the codebase
    - Create module-level documentation
    - _Requirements: All requirements for maintainability_