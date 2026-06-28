# Analytical Functions

Analytical functions, also called window functions, calculate values across related rows without collapsing the result into one row per group. They are useful when you need row-level detail and summary-style context at the same time.

Unlike `GROUP BY`, window functions keep the original rows. They add new calculated columns such as rank, previous value, next value, first value, or last value.

## Window Concepts

| Concept | Meaning |
|---|---|
| Partition | The group of rows a window function can see, such as one department. |
| Order | The row sequence inside each partition, such as salary descending. |
| Frame | The subset of ordered rows used for a calculation. |
| Ranking | Assigning ordered positions to rows. |
| Offset | Looking backward or forward from the current row. |

| Topic | Notebook | Description |
|---|---|---|
| ROW_NUMBER, RANK, and DENSE_RANK | [01_row_number_rank_dense_rank.ipynb](01_row_number_rank_dense_rank.ipynb) | Assigns row positions and explains how ties affect ranking output. |
| NTILE | [02_ntile.ipynb](02_ntile.ipynb) | Divides ordered rows into numbered buckets, such as salary quartiles. |
| LAG and LEAD | [03_lag_lead.ipynb](03_lag_lead.ipynb) | Looks at previous or next row values for comparisons. |
| FIRST_VALUE and LAST_VALUE | [04_first_last_value.ipynb](04_first_last_value.ipynb) | Returns first or last values inside a window frame. |

## Practical Notes

Window functions are powerful, but they depend on clear partitioning and ordering. If the order is not deterministic, ranking and offset results can be confusing.

Use `partitionBy` when calculations should restart for each group. Use `orderBy` when row sequence matters.

[Back to main README](../../README.md#curriculum)
