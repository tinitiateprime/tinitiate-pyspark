# Set Operations

Set operations combine or compare complete result sets. Unlike joins, which match columns from different tables side by side, set operations stack or compare rows from two result sets that have the same shape.

Use set operations when you are working with two lists of similar records and you want to combine them, find overlap, or find differences.

## Compatibility Rule

Both result sets should have:

| Requirement | Meaning |
|---|---|
| Same number of columns | Each side must return the same column count. |
| Same column order | The first column on one side lines up with the first column on the other side. |
| Compatible data types | Matching columns should have compatible types. |
| Clear duplicate handling | Decide whether duplicates should be kept or removed. |

| Topic | Notebook | Description |
|---|---|---|
| UNION | [01_union.ipynb](01_union.ipynb) | Combines two result sets and removes duplicate rows. |
| UNION ALL | [02_union_all.ipynb](02_union_all.ipynb) | Combines two result sets and keeps duplicate rows. |
| INTERSECT | [03_intersect.ipynb](03_intersect.ipynb) | Returns only rows that appear in both result sets. |
| EXCEPT | [04_except.ipynb](04_except.ipynb) | Returns rows from the first result set that do not appear in the second result set. |

## Practical Notes

Use `UNION ALL` when duplicates matter or when you know the inputs do not overlap. Use `UNION` when the final answer should contain unique rows only.

Use `INTERSECT` and `EXCEPT` for comparison work, such as finding records that exist in one extract but not another.

[Back to main README](../../README.md#curriculum)
