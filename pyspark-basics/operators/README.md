# Operators

Operators are the building blocks of filtering logic. They help Spark decide which rows satisfy a condition and should stay in the result.

Most operators appear inside `WHERE`, `filter`, `join`, or `case when` logic. Learning them well makes SQL and DataFrame transformations much easier to read.

## Operator Ideas

| Idea | Meaning |
|---|---|
| Comparison | Check how one value relates to another value. |
| Membership | Check whether a value appears in a list. |
| Pattern matching | Check whether text follows a pattern. |
| Range matching | Check whether a value is inside a start and end boundary. |
| Existence | Check whether related rows exist in another DataFrame. |

| Topic | Notebook | Description |
|---|---|---|
| Equality and inequality | [01_equality_inequality.ipynb](01_equality_inequality.ipynb) | Compares values with equal and not-equal logic. |
| IN and NOT IN | [02_in_not_in.ipynb](02_in_not_in.ipynb) | Keeps or excludes rows based on membership in a list of values. |
| LIKE and NOT LIKE | [03_like_not_like.ipynb](03_like_not_like.ipynb) | Filters text using wildcard pattern matching. |
| BETWEEN | [04_between.ipynb](04_between.ipynb) | Filters values inside an inclusive range. |
| Comparisons | [05_comparisons.ipynb](05_comparisons.ipynb) | Uses greater-than, less-than, and related numeric or date comparisons. |
| EXISTS and NOT EXISTS | [06_exists_not_exists.ipynb](06_exists_not_exists.ipynb) | Checks for related rows using Spark SQL subqueries or semi and anti joins. |

## Practical Notes

Always think about nulls when writing conditions. A comparison with null often does not behave like a normal true or false check because null means unknown.

For DataFrame code, wrap columns with `F.col(...)` when expressions become more complex. It keeps the logic explicit and easier to maintain.

[Back to main README](../../README.md#curriculum)
