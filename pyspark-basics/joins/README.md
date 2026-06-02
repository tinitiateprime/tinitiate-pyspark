# Joins

Joins combine rows from two or more DataFrames by matching related columns. They are one of the most important SQL and PySpark skills because real data is usually split across multiple tables.

In these examples, employees are connected to departments, projects, and salary grades. The notebooks show both PySpark DataFrame joins and Spark SQL joins so you can compare the two styles.

Use joins when one table has the facts you want to analyze and another table has descriptive information that gives those facts meaning.

## Join Concepts

| Concept | Meaning |
|---|---|
| Join key | The column or expression used to match rows, such as `deptno` or `empno`. |
| Left table | The first DataFrame in the join expression. |
| Right table | The second DataFrame in the join expression. |
| Matched row | A row where the join condition finds a related row on the other side. |
| Unmatched row | A row where the join condition does not find a related row. |
| Null result columns | Empty values added when an outer join keeps an unmatched row. |

| Topic | Notebook | Description |
|---|---|---|
| Inner join | [01_inner_join.ipynb](01_inner_join.ipynb) | Keeps only rows that match on both sides, such as employees with a matching department. |
| Left outer join | [02_left_outer_join.ipynb](02_left_outer_join.ipynb) | Keeps every row from the left side and fills unmatched right-side columns with null. |
| Right outer join | [03_right_outer_join.ipynb](03_right_outer_join.ipynb) | Keeps every row from the right side; often rewritten as a left join by swapping table order. |
| Full outer join | [04_full_outer_join.ipynb](04_full_outer_join.ipynb) | Keeps matched and unmatched rows from both tables, useful for reconciliation checks. |
| Multiple table joins | [05_multiple_tables.ipynb](05_multiple_tables.ipynb) | Connects several related tables, such as employees, bridge tables, and projects. |
| Non-equi joins | [06_non_equi_join.ipynb](06_non_equi_join.ipynb) | Uses range or comparison logic instead of equality, such as matching salary to a salary grade. |

## Practical Notes

Start with the join question in plain English before writing code. For example: "Show every employee with their department name" usually means an inner or left join between `emp` and `dept`.

For large datasets, joins can be expensive because Spark may need to shuffle rows across the cluster. Filtering early, selecting only needed columns, and broadcasting small lookup tables can make joins easier to read and faster to run.

[Back to main index](../README.md)
