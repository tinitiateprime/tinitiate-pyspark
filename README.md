![PySpark Tinitiate Image](tinitiate-pyspark.png)

# PySpark Training
> Venkata Bhattaram & Jay Kumsi &copy; TINITIATE.COM

- **PySpark Core Language**

  - ## [DQL Clauses](dql_clauses/README.md)

    - [SELECT](dql_clauses/01_select.ipynb)
      - Choose output columns
      - Reorder columns
      - Return only needed fields

    - [WHERE](dql_clauses/02_where.ipynb)
      - Row-level filtering
      - Salary cutoffs and comparisons
      - Boolean expressions

    - [GROUP BY](dql_clauses/03_group_by.ipynb)
      - Group records
      - Calculate counts, averages, and totals

    - [HAVING](dql_clauses/04_having.ipynb)
      - Filter aggregate results
      - Keep grouped summaries that match conditions

    - [ORDER BY](dql_clauses/05_order_by.ipynb)
      - Sort query results
      - Use ascending, descending, and multi-column sorting

  - ## [Joins](joins/README.md)

    - [Inner join](joins/01_inner_join.ipynb)
      - Keep only rows that match on both sides

    - [Left outer join](joins/02_left_outer_join.ipynb)
      - Keep every row from the left side
      - Fill unmatched right-side columns with null

    - [Right outer join](joins/03_right_outer_join.ipynb)
      - Keep every row from the right side
      - Compare with left join table order

    - [Full outer join](joins/04_full_outer_join.ipynb)
      - Keep matched and unmatched rows from both tables
      - Use for reconciliation checks

    - [Multiple table joins](joins/05_multiple_tables.ipynb)
      - Connect several related tables
      - Join employees, bridge tables, and projects

    - [Non-equi joins](joins/06_non_equi_join.ipynb)
      - Join with range or comparison logic
      - Match salary values to salary grades

  - ## [Set Operations](set_operations/README.md)

    - [UNION](set_operations/01_union.ipynb)
      - Combine two result sets
      - Remove duplicate rows

    - [UNION ALL](set_operations/02_union_all.ipynb)
      - Combine two result sets
      - Keep duplicate rows

    - [INTERSECT](set_operations/03_intersect.ipynb)
      - Return only rows that appear in both result sets

    - [EXCEPT](set_operations/04_except.ipynb)
      - Return rows from the first result set that do not appear in the second

  - ## [Operators](operators/README.md)

    - [Equality and inequality](operators/01_equality_inequality.ipynb)
      - Compare equal and not-equal values

    - [IN and NOT IN](operators/02_in_not_in.ipynb)
      - Keep or exclude rows based on a list of values

    - [LIKE and NOT LIKE](operators/03_like_not_like.ipynb)
      - Filter text using wildcard pattern matching

    - [BETWEEN](operators/04_between.ipynb)
      - Filter values inside an inclusive range

    - [Comparisons](operators/05_comparisons.ipynb)
      - Use greater-than, less-than, and related comparisons

    - [EXISTS and NOT EXISTS](operators/06_exists_not_exists.ipynb)
      - Check for related rows
      - Use Spark SQL subqueries, semi joins, and anti joins

  - ## [String Functions](string_functions/README.md)

    - [Substring and concatenation](string_functions/01_substring_concatenation.ipynb)
      - Extract part of a string
      - Combine values into readable labels

    - [LOWER and UPPER](string_functions/02_lower_upper.ipynb)
      - Convert text to lower or upper case
      - Standardize values for display and comparison

    - [TRIM, LTRIM, and RTRIM](string_functions/03_trim_ltrim_rtrim.ipynb)
      - Remove unwanted spaces
      - Clean text before joins and filters

    - [CHARINDEX](string_functions/04_charindex.ipynb)
      - Find the position of one string inside another

    - [LEFT and RIGHT](string_functions/05_left_right.ipynb)
      - Extract characters from the beginning or end of text

    - [REVERSE, REPLACE, and LENGTH](string_functions/06_reverse_replace_length.ipynb)
      - Reverse strings
      - Replace text
      - Measure text length

  - ## [Aggregate Functions](aggregate_functions/README.md)

    - [COUNT](aggregate_functions/01_count.ipynb)
      - Count all rows
      - Count non-null values in a column

    - [SUM and AVG](aggregate_functions/02_sum_avg.ipynb)
      - Calculate totals
      - Calculate averages for numeric columns

    - [MAX and MIN](aggregate_functions/03_max_min.ipynb)
      - Find highest and lowest values

    - [Aggregate by group](aggregate_functions/04_by_group.ipynb)
      - Apply aggregate functions by category
      - Build salary totals and counts by department

  - ## [Analytical Functions](analytical_functions/README.md)

    - [ROW_NUMBER, RANK, and DENSE_RANK](analytical_functions/01_row_number_rank_dense_rank.ipynb)
      - Assign row positions
      - Understand how ties affect ranking output

    - [NTILE](analytical_functions/02_ntile.ipynb)
      - Divide ordered rows into numbered buckets

    - [LAG and LEAD](analytical_functions/03_lag_lead.ipynb)
      - Compare current rows with previous or next rows

    - [FIRST_VALUE and LAST_VALUE](analytical_functions/04_first_last_value.ipynb)
      - Return first or last values inside a window frame

  - ## [Performance Tuning](performance_tuning.md)

    - [Join performance benchmark](02_join_performance_small_vs_large_files.ipynb)
      - Load many small files and measure runtime
      - Load fewer larger files and compare runtime
      - Measure the difference across CSV, JSON, and Parquet

    - File layout
      - Small files vs larger files
      - How too many source files slow Spark loading
      - File listing, metadata overhead, and scan planning
      - Why fewer larger files usually process faster

    - Small-file problem
      - Source systems may send many tiny files
      - Spark creates more scan tasks for many files
      - Job startup and task scheduling become slower
      - Processing time increases even when data volume is the same

    - Merge small files
      - Read small files received from source
      - Merge or compact them into fewer larger files
      - Use [compact_small_files.py](scripts/compact_small_files.py)
      - Process the compacted output instead of the raw tiny files
      - Compare before and after runtimes to show performance improvement

    - File format behavior
      - CSV parsing overhead
      - JSON semi-structured data overhead
      - Parquet columnar reads and compression

    - Join strategy
      - Broadcast joins
      - Shuffle joins
      - Data skew

    - Update performance
      - Compare small updates with large updates
      - Understand full rewrites for plain files
      - Review table formats for frequent updates

    - Optimization techniques
      - Compaction
      - Merging source small files before processing
      - `repartition()` vs `coalesce()`
      - Partitioning by columns
      - Caching reused DataFrames

    - Spark execution settings
      - `spark.sql.adaptive.enabled`
      - `spark.sql.shuffle.partitions`
      - `spark.sql.files.maxPartitionBytes`

    - Spark UI review
      - Jobs, stages, and tasks
      - Shuffle read and write
      - Task duration and spill

  - ## [Supporting Files](misc/README.md)

    - [Local PySpark setup](misc/pyspark-local-setup.md)
    - [Python notes](misc/python.md)
    - [Tutorial outline](misc/pyspark_tutorials.md)
    - [Helper scripts](scripts)
    - [Combined DQL notebook](01_dql_select_where_group_having_order.ipynb)
