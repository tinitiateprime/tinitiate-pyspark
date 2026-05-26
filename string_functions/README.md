# String Functions

String functions clean, standardize, search, and reshape text values. They are used heavily in data preparation because source systems often store names, codes, categories, and descriptions as text.

These notebooks show common SQL-style string operations in PySpark. The examples use employee and department data, plus small demo DataFrames where needed.

## Common String Tasks

| Task | Example |
|---|---|
| Standardize casing | Convert values to upper or lower case before comparing. |
| Remove extra spaces | Trim leading and trailing spaces from imported text. |
| Extract codes | Take the left, right, or middle part of a string. |
| Build labels | Concatenate multiple columns into one readable value. |
| Search text | Find whether a word or character appears inside a value. |
| Replace text | Clean or normalize repeated text patterns. |

| Topic | Notebook | Description |
|---|---|---|
| Substring and concatenation | [01_substring_concatenation.ipynb](01_substring_concatenation.ipynb) | Extracts part of a string and combines multiple values into readable labels. |
| LOWER and UPPER | [02_lower_upper.ipynb](02_lower_upper.ipynb) | Converts text to lower or upper case for display and comparison. |
| TRIM, LTRIM, and RTRIM | [03_trim_ltrim_rtrim.ipynb](03_trim_ltrim_rtrim.ipynb) | Removes unwanted spaces that can break joins, filters, and comparisons. |
| CHARINDEX | [04_charindex.ipynb](04_charindex.ipynb) | Finds the position of one string inside another using Spark equivalents. |
| LEFT and RIGHT | [05_left_right.ipynb](05_left_right.ipynb) | Extracts characters from the beginning or end of a string. |
| REVERSE, REPLACE, and LENGTH | [06_reverse_replace_length.ipynb](06_reverse_replace_length.ipynb) | Reverses strings, replaces text, and measures text length for data quality checks. |

## Practical Notes

String cleanup is often best done before joins and grouping. Small differences such as extra spaces or inconsistent casing can make two values fail to match.

When replacing or extracting text, create clear output column names so the transformed value is easy to inspect.

[Back to main index](../README.md)
