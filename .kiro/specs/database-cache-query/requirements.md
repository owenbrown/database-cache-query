# Requirements Document

## Introduction

This feature implements a smart database caching system that efficiently retrieves data by maintaining a local Parquet cache using Polars and only querying the database for missing data. The system provides progress feedback through tqdm and handles large datasets by batching queries. The cache uses immutable data that never requires invalidation.

## Requirements

### Requirement 1

**User Story:** As a data analyst, I want to request specific data by providing a list of IDs, columns, and a data fetcher function, so that I can retrieve only the data I need without manual cache management.

#### Acceptance Criteria

1. WHEN the user calls get_data(ids: List[int], columns: List[str], table_name: str, fetcher: Callable) THEN the system SHALL accept these inputs and validate their format
2. WHEN the fetcher function is called THEN it SHALL receive a list of integer IDs and table_name string and return a list of dictionaries with column names as keys
3. WHEN table_name is provided THEN it SHALL be in format "schema_name.table_name"
4. WHEN the input contains invalid IDs or column names THEN the system SHALL return appropriate error messages
5. WHEN the user specifies columns THEN the system SHALL only retrieve and cache those specific columns
6. WHEN IDs are provided THEN they SHALL always be integers

### Requirement 2

**User Story:** As a developer, I want the system to automatically check the cache for existing data, so that I can avoid unnecessary database queries and improve performance.

#### Acceptance Criteria

1. WHEN a data request is made THEN the system SHALL first check the local cache for existing data
2. WHEN data exists in the cache THEN the system SHALL use cached data instead of querying the database
3. WHEN only partial data exists in the cache THEN the system SHALL identify which IDs and columns are missing and need to be queried
4. WHEN different column combinations are requested for overlapping IDs THEN the system SHALL intelligently merge cache entries to avoid duplicate storage

### Requirement 3

**User Story:** As a user working with large datasets, I want to see progress feedback during data retrieval, so that I can monitor the operation and estimate completion time.

#### Acceptance Criteria

1. WHEN the system queries the database THEN it SHALL display progress using tqdm
2. WHEN processing batches THEN the system SHALL update progress for each completed batch
3. WHEN the operation completes THEN the system SHALL show final completion status

### Requirement 4

**User Story:** As a system administrator, I want the system to handle large queries efficiently through batching, so that database performance is not impacted by oversized queries.

#### Acceptance Criteria

1. WHEN querying for missing data THEN the system SHALL break queries into batches with at most 100 batches total and minimum batch size of 100 items
2. WHEN processing batches THEN the system SHALL execute them sequentially to avoid overwhelming the database
3. WHEN a batch fails THEN the system SHALL handle the error gracefully and continue with remaining batches
4. WHEN calculating batch size THEN the system SHALL use the formula: max(100, ceil(missing_ids_count / 100)) to determine batch size

### Requirement 5

**User Story:** As a data engineer, I want retrieved data to be automatically cached, so that future requests for the same data are faster.

#### Acceptance Criteria

1. WHEN new data is retrieved from the database THEN the system SHALL store it in the local cache
2. WHEN storing data THEN the system SHALL use Parquet format with Polars for optimal performance with 1-10 million row datasets
3. WHEN managing cache files THEN the system SHALL start with a single Parquet file and expand to multiple files following Parquet best practices for file sizing
4. WHEN the cache directory doesn't exist THEN the system SHALL create it automatically
5. WHEN accessing cached data THEN the system SHALL efficiently query by ID and column combinations using Polars
7. WHEN the system starts THEN it SHALL use the cache directory defined in config.py as CACHED_DATA_FOLDER = pathlib.Path.home() / "data" / "cached_data"
8. WHEN the program restarts THEN the system SHALL persist and reuse previously cached data without any invalidation

### Requirement 6

**User Story:** As a developer working with JSON data, I want to access nested JSON fields using dot notation, so that I can retrieve specific values from JSON columns without manual parsing.

#### Acceptance Criteria

1. WHEN a column name contains dot notation like "first.second" THEN the system SHALL parse "first" as a JSON column and extract the "second" key
2. WHEN a JSON column is a string THEN the system SHALL parse it as JSON before extracting the nested value
3. WHEN a JSON column is already a dictionary THEN the system SHALL directly extract the nested value
4. WHEN a nested JSON key doesn't exist THEN the system SHALL handle the error appropriately

### Requirement 7

**User Story:** As an application developer, I want the system to return the complete requested dataset, so that I can use the data immediately without additional processing.

#### Acceptance Criteria

1. WHEN all data retrieval is complete THEN the system SHALL return the complete dataset for the requested IDs and columns
2. WHEN returning data THEN the system SHALL maintain the original data types and structure, unless the data type was a json str. Then, return the parsed json as a dict, list, or object
3. WHEN an ID is not found in either cache or database THEN the system SHALL raise an exception
4. WHEN a requested column is not available in the database THEN the system SHALL raise an exception