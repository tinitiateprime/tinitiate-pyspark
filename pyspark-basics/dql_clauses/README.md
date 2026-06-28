# DQL Clauses

Data Query Language (DQL) is the part of SQL used to read and analyze data. These notebooks explain the most common DQL clauses and show each one in both PySpark DataFrame syntax and Spark SQL syntax.

The clauses build on each other:

1. `SELECT` chooses the columns and expressions to show.
2. `WHERE` filters individual rows before grouping.
3. `GROUP BY` combines rows into summary groups.
4. `HAVING` filters grouped summary results.
5. `ORDER BY` sorts the final output.

Use these notebooks as the starting path for learning how SQL query logic maps to PySpark code. Each notebook includes the same sample tables, so you can compare the DataFrame API and Spark SQL side by side.

| Topic | Notebook | Description |
|---|---|---|
| SELECT | [01_select.ipynb](01_select.ipynb) | Explains how to choose output columns, reorder columns, and return only the fields needed for the result. |
| WHERE | [02_where.ipynb](02_where.ipynb) | Explains row-level filtering with conditions such as salary cutoffs, comparisons, and boolean expressions. |
| GROUP BY | [03_group_by.ipynb](03_group_by.ipynb) | Explains how to group records and calculate summaries such as counts, averages, and totals. |
| HAVING | [04_having.ipynb](04_having.ipynb) | Explains how to filter aggregate results after `GROUP BY`, such as keeping only departments above a salary total. |
| ORDER BY | [05_order_by.ipynb](05_order_by.ipynb) | Explains how to sort final query results using ascending, descending, and multiple-column order rules. |

## Query Processing Order

SQL is usually written in this order:

```sql
SELECT ...
FROM ...
WHERE ...
GROUP BY ...
HAVING ...
ORDER BY ...
```

Spark does not simply execute queries from top to bottom. It analyzes the full query, builds a logical plan, optimizes it, and then creates a physical execution plan. For learning, it helps to remember the logical meaning:

| Clause | Logical Role |
|---|---|
| `FROM` | Start with a table or DataFrame. |
| `WHERE` | Remove rows that do not match the row-level condition. |
| `GROUP BY` | Put remaining rows into groups. |
| `HAVING` | Remove groups that do not match the aggregate condition. |
| `SELECT` | Choose the columns and calculated expressions to return. |
| `ORDER BY` | Sort the final result. |

## DataFrame API vs Spark SQL

Each notebook shows two styles:

| Style | Example | When to Use |
|---|---|---|
| DataFrame API | `emp.select("empno", "ename")` | Useful in Python pipelines, reusable functions, and programmatic transformations. |
| Spark SQL | `SELECT empno, ename FROM emp` | Useful when writing SQL training examples or translating existing SQL logic to Spark. |

Both styles run through Spark's optimizer. In most lessons, focus first on understanding the query meaning, then compare how the same logic appears in each syntax.

[Back to main README](../../README.md#curriculum)
