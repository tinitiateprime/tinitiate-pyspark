# Aggregate Functions

Aggregate functions turn many rows into summary values. They answer questions like how many rows exist, what the total is, what the average is, and which values are highest or lowest.

Aggregates can be calculated across an entire DataFrame or within groups. When combined with `GROUP BY`, they become the foundation of reporting and analytics.

## Aggregate Concepts

| Concept | Meaning |
|---|---|
| Aggregate function | A function that summarizes multiple rows into one value. |
| Ungrouped aggregate | A summary across the whole DataFrame. |
| Grouped aggregate | A summary calculated once per group, such as per department. |
| Null handling | Some aggregate functions ignore nulls, so missing data can affect interpretation. |

| Topic | Notebook | Description |
|---|---|---|
| COUNT | [01_count.ipynb](01_count.ipynb) | Counts all rows or counts non-null values in a specific column. |
| SUM and AVG | [02_sum_avg.ipynb](02_sum_avg.ipynb) | Calculates totals and averages for numeric columns. |
| MAX and MIN | [03_max_min.ipynb](03_max_min.ipynb) | Finds the highest and lowest values in numeric, date, or text columns. |
| Aggregate by group | [04_by_group.ipynb](04_by_group.ipynb) | Applies aggregate functions per category, such as salary totals by department. |

## Practical Notes

Use aliases for aggregate columns. Names like `avg_salary` or `employee_count` make summary output much easier to understand than generated names.

For production logic, think carefully about nulls and data quality before interpreting aggregate results.

[Back to main README](../../README.md#curriculum)
