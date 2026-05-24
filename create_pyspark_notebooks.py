import json
from pathlib import Path


CSV_SETUP = r'''from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("pyspark-sql-tutorial")
    .getOrCreate()
)

candidate_dirs = [
    Path.cwd() / "data",
    Path.cwd().parent / "data",
    Path.cwd(),
    Path.cwd().parent,
]

DATA_DIR = next((path for path in candidate_dirs if (path / "emp.csv").exists()), None)
if DATA_DIR is None:
    raise FileNotFoundError("Could not find emp.csv. Put the CSV files in ./data or the current folder.")

print(f"Reading CSV files from: {DATA_DIR}")

emp = spark.read.option("header", True).option("inferSchema", True).csv(str(DATA_DIR / "emp.csv"))
dept = spark.read.option("header", True).option("inferSchema", True).csv(str(DATA_DIR / "dept.csv"))
salgrade = spark.read.option("header", True).option("inferSchema", True).csv(str(DATA_DIR / "salgrade.csv"))
projects = spark.read.option("header", True).option("inferSchema", True).csv(str(DATA_DIR / "projects.csv"))
emp_projects = spark.read.option("header", True).option("inferSchema", True).csv(str(DATA_DIR / "emp_projects.csv"))

for name, df in {
    "emp": emp,
    "dept": dept,
    "salgrade": salgrade,
    "projects": projects,
    "emp_projects": emp_projects,
}.items():
    df.createOrReplaceTempView(name)
'''


PARQUET_SETUP = r'''from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("pyspark-parquet-sql-tutorial")
    .getOrCreate()
)

candidate_dirs = [
    Path.cwd() / "data",
    Path.cwd().parent / "data",
    Path.cwd(),
    Path.cwd().parent,
]

DATA_DIR = next((path for path in candidate_dirs if (path / "dept.parquet").exists()), None)
if DATA_DIR is None:
    raise FileNotFoundError("Could not find dept.parquet. Put the Parquet files in ./data or the current folder.")

emp_paths = sorted(DATA_DIR.glob("emp_part_*.parquet"))
if not emp_paths:
    raise FileNotFoundError("Could not find emp_part_*.parquet files in the Parquet data folder.")

print(f"Reading Parquet files from: {DATA_DIR}")

emp = spark.read.parquet(*[str(path) for path in emp_paths])
dept = spark.read.parquet(str(DATA_DIR / "dept.parquet"))
salgrade = spark.read.parquet(str(DATA_DIR / "salgrade.parquet"))
projects = spark.read.parquet(str(DATA_DIR / "projects.parquet"))
emp_projects = spark.read.parquet(str(DATA_DIR / "emp_projects.parquet"))

for name, df in {
    "emp": emp,
    "dept": dept,
    "salgrade": salgrade,
    "projects": projects,
    "emp_projects": emp_projects,
}.items():
    df.createOrReplaceTempView(name)
'''


SECTION_NOTES = {
    "## SELECT": """In plain English: `SELECT` means "show me these columns." It does not remove rows by itself; it only controls which columns appear in the final output.

Key points:

* `emp.select(...)` is the PySpark DataFrame version.
* `SELECT ... FROM emp` is the SQL version.
* Both examples return employee columns from the same `emp` data.""",
    "## WHERE": """In plain English: `WHERE` means "keep only the rows that pass this test." A condition is an expression that is either true or false for each row.

Key points:

* Rows with salary below the cutoff are removed from the result.
* The original DataFrame is not changed; Spark creates a new filtered result.
* `where` and `filter` mean the same thing in PySpark.""",
    "## GROUP BY": """In plain English: `GROUP BY` makes buckets of rows. After rows are grouped, aggregate functions such as `count`, `avg`, and `sum` calculate one answer per bucket.

Key points:

* Grouping by `deptno` creates one result row per department number.
* Every selected column must either be part of the group or be calculated with an aggregate function.
* This is how we answer questions like "how many employees are in each department?\"""",
    "## HAVING": """In plain English: `HAVING` is like `WHERE`, but it filters after grouping has already happened.

Use this memory trick:

* `WHERE` filters individual rows before aggregation.
* `HAVING` filters grouped summary rows after aggregation.

In this example, Spark first totals salary by department and then keeps only departments whose total salary is high enough.""",
    "## ORDER BY": """In plain English: `ORDER BY` sorts the final answer so humans can read it more easily.

Key points:

* `desc()` means descending order, so larger salaries appear first.
* Sorting usually happens near the end of a query.
* If two rows have the same salary, the second sort column can make the order predictable.""",
    "## Inner Join": """In plain English: an inner join keeps only matching rows from both tables.

Think of it as asking: "Which employees have a department row that matches their `deptno`?"

If a row has no match on the other side, it does not appear in an inner join result.""",
    "## Left Outer Join": """In plain English: a left join keeps every row from the left table, even when there is no match in the right table.

Key points:

* The left table in this example is `dept`.
* Every department is kept.
* Employee columns become null when a department has no matching employees.""",
    "## Right Outer Join": """In plain English: a right join keeps every row from the right table.

Most teams use left joins more often because they are easier to read. A right join can usually be rewritten by swapping table order and using a left join.""",
    "## Full Outer Join": """In plain English: a full outer join keeps all rows from both sides.

Use it when you want to find everything, including matches and non-matches. Unmatched columns from the other table become null.""",
    "## Joining Multiple Tables": """Real questions often need more than two tables. Here, `emp_projects` is a bridge table: it connects employees to projects.

The joins work step by step:

* `emp` joins to `emp_projects` by `empno`.
* `emp_projects` joins to `projects` by `projectno`.
* The final result can show employee names and project names together.""",
    "## Non-Equi Join": """A normal join often uses equality, such as `deptno = deptno`. A non-equi join uses another comparison.

In this example, salary grade is found by checking whether employee salary falls between a low salary and high salary range.""",
    "## Prepare Example Sets": """Set operations need two result sets with the same shape. That means the same number of columns in the same order, with compatible data types.

This setup creates two small employee lists so the set operation results are easy to understand.""",
    "## UNION": """`UNION` stacks two result sets and removes duplicate rows.

Use it when you want one combined list and you do not want repeated rows in the final answer.""",
    "## UNION ALL": """`UNION ALL` stacks two result sets and keeps duplicates.

Use it when duplicates are meaningful, or when you want faster behavior and you know duplicates do not matter.""",
    "## INTERSECT": """`INTERSECT` returns only rows that appear in both result sets.

In plain English: "show me the overlap between these two lists.\"""",
    "## EXCEPT": """`EXCEPT` subtracts the second result set from the first result set.

In plain English: "show me rows from list A that are not in list B.\"""",
    "## Equality and Inequality": """Operators are used inside filter conditions.

Equality asks "is this value the same?" Inequality asks "is this value different?" In SQL, not equal is commonly written as `<>`; in PySpark Python expressions it is written as `!=`.""",
    "## IN and NOT IN": """`IN` is a shortcut for multiple equality checks.

For example, `deptno IN (10, 30)` means department number is 10 or 30. `NOT IN` keeps rows outside that list.""",
    "## LIKE and NOT LIKE": """`LIKE` searches text using patterns.

The `%` wildcard means "any number of characters." For example, `a%` means values that start with `a`.""",
    "## BETWEEN": """`BETWEEN` checks whether a value is inside a range.

Important detail: `BETWEEN` includes both endpoints. Salary between 2000 and 3000 includes salary equal to 2000 and salary equal to 3000.""",
    "## Comparison Operators": """Comparison operators are the building blocks of filters.

Plain-English meaning:

* `>` means greater than.
* `>=` means greater than or equal to.
* `<` means less than.
* `<=` means less than or equal to.""",
    "## EXISTS and NOT EXISTS": """`EXISTS` asks whether a matching row exists in another table.

In PySpark, left semi joins and left anti joins are common ways to express this:

* `left_semi` means keep rows that have a match.
* `left_anti` means keep rows that do not have a match.""",
    "## Substring and Concatenation": """String functions help clean and reshape text.

`substring` cuts out part of a string. `concat` or `concat_ws` joins strings together to make a readable label.""",
    "## LOWER and UPPER": """Case functions are useful when values may be typed differently.

For example, `Sales`, `SALES`, and `sales` look different to a computer. Converting text to one case makes comparison easier.""",
    "## TRIM, LTRIM, and RTRIM": """Extra spaces are hard to see, but they can break comparisons.

`trim` removes spaces from both sides. `ltrim` removes spaces from the left. `rtrim` removes spaces from the right.""",
    "## CHARINDEX Equivalent": """Some databases use `CHARINDEX` to find the position of text. Spark uses `instr` or `locate`.

If the result is 0, Spark did not find the search text.""",
    "## LEFT and RIGHT": """`LEFT` and `RIGHT` return characters from the beginning or end of a string.

These are useful for extracting prefixes, suffixes, codes, or short display labels.""",
    "## REVERSE, REPLACE, and LENGTH": """These functions are often used for data cleanup and checking text quality.

`replace` changes one piece of text to another. `length` counts characters, which helps find unusually short or long values.""",
    "## COUNT": """`COUNT` answers "how many?"

Important difference:

* `count(*)` counts all rows.
* `count(column)` counts only rows where that column is not null.""",
    "## SUM and AVG": """`SUM` adds values. `AVG` calculates the average.

When a column can contain nulls, think carefully. Null means missing or unknown, so you may need `coalesce` to replace null with 0 before adding.""",
    "## MAX and MIN": """`MAX` finds the largest value. `MIN` finds the smallest value.

These work on numbers, dates, and strings, but the meaning depends on the data type.""",
    "## Aggregates by Group": """Grouped aggregates answer business questions by category.

Examples:

* salary totals by department
* employee counts by job
* maximum salary by salary grade""",
    "## Window Setup": """Analytical functions use windows. A window tells Spark which nearby or related rows each row can compare itself to.

Two important window ideas:

* `partitionBy` splits rows into groups, like departments.
* `orderBy` decides the row order inside each group.""",
    "## ROW_NUMBER, RANK, and DENSE_RANK": """These functions assign positions to rows.

Use this difference:

* `row_number` always gives unique numbers.
* `rank` gives the same rank to ties but leaves gaps.
* `dense_rank` gives the same rank to ties and does not leave gaps.""",
    "## NTILE": """`NTILE` divides ordered rows into a chosen number of buckets.

For example, `NTILE(4)` can split employees into four salary groups from highest to lowest.""",
    "## LAG and LEAD": """`LAG` looks backward. `LEAD` looks forward.

These are useful for comparing a row to the previous or next row, such as comparing this employee's salary to the previous hire's salary.""",
    "## FIRST_VALUE and LAST_VALUE": """`FIRST_VALUE` and `LAST_VALUE` pull values from the first or last row in a window.

The window frame matters. For `LAST_VALUE`, we explicitly tell Spark to look all the way to the end of the partition."""
}


def md(source):
    text = source.strip()
    heading = text.splitlines()[0] if text else ""
    extra = SECTION_NOTES.get(heading)
    if extra:
        text = f"{text}\n\n{extra}"
    return {"cell_type": "markdown", "metadata": {}, "source": text + "\n"}


def code(source):
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source.strip() + "\n",
    }


def notebook(title, intro, cells):
    all_cells = [
        md(f"# {title}\n\n{intro}"),
        md("## CSV Data Source\n\nCSV stores data as plain text rows. The loader creates the table names used by the examples: `emp`, `dept`, `salgrade`, `projects`, and `emp_projects`."),
        code(CSV_SETUP),
        code("emp.show(5)\ndept.show()\nsalgrade.show()\nprojects.show()\nemp_projects.show(5)"),
        md("## Parquet Data Source\n\nParquet stores data in a compressed columnar format. The same table names are created, so the DQL examples work the same way after loading Parquet."),
        code(PARQUET_SETUP),
        code("emp.show(5)\ndept.show()\nsalgrade.show()\nprojects.show()\nemp_projects.show(5)"),
    ]
    all_cells.extend(cells)
    return {
        "cells": all_cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "name": "python",
                "pygments_lexer": "ipython3",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


notebooks = {
    "01_dql_select.ipynb": notebook(
        "DQL: SELECT",
        "`SELECT` chooses the columns that appear in the result. It answers the question: which fields should be shown?",
        [
            md("## SELECT\n\nUse `select` to choose columns. In PySpark you can use the DataFrame API or Spark SQL."),
            code('emp.select("empno", "ename", "job", "sal", "deptno").show(10)\n\nspark.sql("""\nSELECT empno, ename, job, sal, deptno\nFROM emp\nLIMIT 10\n""").show()'),
        ],
    ),
    "02_dql_where.ipynb": notebook(
        "DQL: WHERE",
        "`WHERE` filters rows. It answers the question: which records should stay in the result?",
        [
            md("## WHERE\n\nUse `where` or `filter` to keep only rows that match a condition."),
            code('emp.where(F.col("sal") >= 3000).select("empno", "ename", "job", "sal").show()\n\nspark.sql("""\nSELECT empno, ename, job, sal\nFROM emp\nWHERE sal >= 3000\n""").show()'),
        ],
    ),
    "03_dql_group_by.ipynb": notebook(
        "DQL: GROUP BY",
        "`GROUP BY` puts rows into groups and calculates summaries for each group.",
        [
            md("## GROUP BY\n\nUse `groupBy` to collect rows into groups, then calculate summaries for each group."),
            code('emp.groupBy("deptno").agg(\n    F.count("*").alias("employee_count"),\n    F.round(F.avg("sal"), 2).alias("avg_salary"),\n    F.sum("sal").alias("total_salary")\n).orderBy("deptno").show()\n\nspark.sql("""\nSELECT deptno, COUNT(*) AS employee_count, ROUND(AVG(sal), 2) AS avg_salary, SUM(sal) AS total_salary\nFROM emp\nGROUP BY deptno\nORDER BY deptno\n""").show()'),
        ],
    ),
    "04_dql_having.ipynb": notebook(
        "DQL: HAVING",
        "`HAVING` filters grouped results after aggregation has already happened.",
        [
            md("## HAVING\n\n`HAVING` filters grouped results. In the DataFrame API, aggregate first and then filter the summary rows."),
            code('dept_summary = emp.groupBy("deptno").agg(\n    F.count("*").alias("employee_count"),\n    F.sum("sal").alias("total_salary")\n)\n\ndept_summary.where(F.col("total_salary") > 15000).orderBy("total_salary", ascending=False).show()\n\nspark.sql("""\nSELECT deptno, COUNT(*) AS employee_count, SUM(sal) AS total_salary\nFROM emp\nGROUP BY deptno\nHAVING SUM(sal) > 15000\nORDER BY total_salary DESC\n""").show()'),
        ],
    ),
    "05_dql_order_by.ipynb": notebook(
        "DQL: ORDER BY",
        "`ORDER BY` sorts the final result so values appear in a predictable order.",
        [
            md("## ORDER BY\n\nUse `orderBy` to sort the final result. Sorting is usually one of the last steps in a query."),
            code('emp.select("empno", "ename", "job", "sal").orderBy(F.col("sal").desc(), F.col("ename")).show(10)\n\nspark.sql("""\nSELECT empno, ename, job, sal\nFROM emp\nORDER BY sal DESC, ename\nLIMIT 10\n""").show()'),
        ],
    ),
    "06_dql_joins.ipynb": notebook(
        "DQL: Joins",
        "Joins combine rows from two or more tables. These examples join employees to departments, projects, and salary grades.",
        [
            md("## Inner Join\n\nAn inner join keeps only rows that match in both DataFrames."),
            code('emp.join(dept, on="deptno", how="inner").select("empno", "ename", "job", "dname", "loc").show(10)\n\nspark.sql("""\nSELECT e.empno, e.ename, e.job, d.dname, d.loc\nFROM emp e\nINNER JOIN dept d ON e.deptno = d.deptno\nORDER BY e.empno\nLIMIT 10\n""").show()'),
            md("## Left Outer Join\n\nA left join keeps all rows from the left DataFrame and fills unmatched right-side columns with null."),
            code('dept.join(emp, on="deptno", how="left").select("deptno", "dname", "empno", "ename").orderBy("deptno", "empno").show(20)\n\nspark.sql("""\nSELECT d.deptno, d.dname, e.empno, e.ename\nFROM dept d\nLEFT OUTER JOIN emp e ON d.deptno = e.deptno\nORDER BY d.deptno, e.empno\n""").show(20)'),
            md("## Right Outer Join\n\nA right join keeps all rows from the right DataFrame. It is often equivalent to swapping table order and using a left join."),
            code('emp.join(dept, on="deptno", how="right").select("deptno", "dname", "empno", "ename").orderBy("deptno", "empno").show(20)\n\nspark.sql("""\nSELECT d.deptno, d.dname, e.empno, e.ename\nFROM emp e\nRIGHT OUTER JOIN dept d ON e.deptno = d.deptno\nORDER BY d.deptno, e.empno\n""").show(20)'),
            md("## Full Outer Join\n\nA full outer join keeps all rows from both sides. Unmatched columns become null."),
            code('emp.join(dept, on="deptno", how="full").select("deptno", "dname", "empno", "ename").orderBy("deptno", "empno").show(20)\n\nspark.sql("""\nSELECT COALESCE(e.deptno, d.deptno) AS deptno, d.dname, e.empno, e.ename\nFROM emp e\nFULL OUTER JOIN dept d ON e.deptno = d.deptno\nORDER BY deptno, e.empno\n""").show(20)'),
            md("## Joining Multiple Tables\n\nThis example connects employees to projects through `emp_projects`."),
            code('emp.join(emp_projects, "empno", "inner").join(projects, "projectno", "inner").select(\n    "empno", "ename", "project_name", "budget", "start_date", "end_date"\n).orderBy("empno").show(15)\n\nspark.sql("""\nSELECT e.empno, e.ename, p.project_name, p.budget, ep.start_date, ep.end_date\nFROM emp e\nJOIN emp_projects ep ON e.empno = ep.empno\nJOIN projects p ON ep.projectno = p.projectno\nORDER BY e.empno\nLIMIT 15\n""").show()'),
            md("## Non-Equi Join\n\nNot every join uses equality. Salary grades are matched by checking whether salary falls between `losal` and `hisal`."),
            code('emp.join(salgrade, (emp.sal >= salgrade.losal) & (emp.sal <= salgrade.hisal), "inner").select(\n    "empno", "ename", "sal", "grade"\n).orderBy("empno").show(15)\n\nspark.sql("""\nSELECT e.empno, e.ename, e.sal, s.grade\nFROM emp e\nJOIN salgrade s ON e.sal BETWEEN s.losal AND s.hisal\nORDER BY e.empno\nLIMIT 15\n""").show()'),
        ],
    ),
    "07_dql_set_operations.ipynb": notebook(
        "DQL: Set Operations",
        "Set operations combine the results of two compatible queries. Compatible means the same number of columns and compatible data types.",
        [
            md("## Prepare Example Sets\n\nThese small DataFrames make set operation behavior easy to see."),
            code('sales_people = emp.where(F.col("job") == "salesman").select("empno", "ename")\nhigh_salary_people = emp.where(F.col("sal") >= 3000).select("empno", "ename")\n\nsales_people.show()\nhigh_salary_people.show()'),
            md("## UNION\n\nSQL `UNION` removes duplicates. In the DataFrame API, use `unionByName(...).distinct()`."),
            code('''sales_people.unionByName(high_salary_people).distinct().orderBy("empno").show()

spark.sql("""
SELECT empno, ename FROM emp WHERE job = 'salesman'
UNION
SELECT empno, ename FROM emp WHERE sal >= 3000
ORDER BY empno
""").show()'''),
            md("## UNION ALL\n\n`UNION ALL` keeps duplicate rows."),
            code('''sales_people.unionByName(high_salary_people).orderBy("empno").show()

spark.sql("""
SELECT empno, ename FROM emp WHERE job = 'salesman'
UNION ALL
SELECT empno, ename FROM emp WHERE sal >= 3000
ORDER BY empno
""").show()'''),
            md("## INTERSECT\n\n`INTERSECT` returns rows found in both result sets."),
            code('''sales_people.intersect(high_salary_people).orderBy("empno").show()

spark.sql("""
SELECT empno, ename FROM emp WHERE job = 'salesman'
INTERSECT
SELECT empno, ename FROM emp WHERE sal >= 3000
ORDER BY empno
""").show()'''),
            md("## EXCEPT\n\n`EXCEPT` returns rows from the first query that are not in the second query."),
            code('''sales_people.subtract(high_salary_people).orderBy("empno").show()

spark.sql("""
SELECT empno, ename FROM emp WHERE job = 'salesman'
EXCEPT
SELECT empno, ename FROM emp WHERE sal >= 3000
ORDER BY empno
""").show()'''),
        ],
    ),
    "08_dql_basic_operators.ipynb": notebook(
        "DQL: Basic Operators",
        "Operators build filter conditions. These examples show common SQL operators and their PySpark equivalents.",
        [
            md("## Equality and Inequality\n\nUse `==` for equality in the DataFrame API. Use `!=` for not equal."),
            code('''emp.where(F.col("job") == "manager").select("empno", "ename", "job").show()
emp.where(F.col("job") != "manager").select("empno", "ename", "job").show(10)

spark.sql("""
SELECT empno, ename, job FROM emp WHERE job = 'manager'
""").show()

spark.sql("""
SELECT empno, ename, job FROM emp WHERE job <> 'manager' LIMIT 10
""").show()'''),
            md("## IN and NOT IN\n\nUse `isin` for membership checks."),
            code('emp.where(F.col("deptno").isin(10, 30)).select("empno", "ename", "deptno").show()\nemp.where(~F.col("deptno").isin(10, 30)).select("empno", "ename", "deptno").show(10)\n\nspark.sql("""\nSELECT empno, ename, deptno FROM emp WHERE deptno IN (10, 30)\n""").show()\n\nspark.sql("""\nSELECT empno, ename, deptno FROM emp WHERE deptno NOT IN (10, 30) LIMIT 10\n""").show()'),
            md("## LIKE and NOT LIKE\n\nUse `like` for wildcard matching. `%` means any number of characters."),
            code('''emp.where(F.col("ename").like("a%")).select("empno", "ename").show()
emp.where(~F.col("ename").like("%_1%")).select("empno", "ename").show(10)

spark.sql("""
SELECT empno, ename FROM emp WHERE ename LIKE 'a%'
""").show()

spark.sql("""
SELECT empno, ename FROM emp WHERE ename NOT LIKE '%_1%' LIMIT 10
""").show()'''),
            md("## BETWEEN\n\n`between` includes both endpoints."),
            code('emp.where(F.col("sal").between(2000, 3000)).select("empno", "ename", "sal").orderBy("sal").show()\n\nspark.sql("""\nSELECT empno, ename, sal\nFROM emp\nWHERE sal BETWEEN 2000 AND 3000\nORDER BY sal\n""").show()'),
            md("## Comparison Operators\n\nUse greater than, greater than or equal, less than, and less than or equal for numeric and date comparisons."),
            code('emp.select("empno", "ename", "sal").where(F.col("sal") > 3000).show()\nemp.select("empno", "ename", "sal").where(F.col("sal") >= 3000).show()\nemp.select("empno", "ename", "sal").where(F.col("sal") < 1200).show()\nemp.select("empno", "ename", "sal").where(F.col("sal") <= 1200).show()\n\nspark.sql("""\nSELECT empno, ename, sal FROM emp WHERE sal > 3000\n""").show()'),
            md("## EXISTS and NOT EXISTS\n\nSpark SQL supports correlated subqueries with `EXISTS`. In the DataFrame API, semi and anti joins are common equivalents."),
            code('emp.join(emp_projects.select("empno").distinct(), on="empno", how="left_semi").select("empno", "ename").show(10)\nemp.join(emp_projects.select("empno").distinct(), on="empno", how="left_anti").select("empno", "ename").show(10)\n\nspark.sql("""\nSELECT e.empno, e.ename\nFROM emp e\nWHERE EXISTS (\n    SELECT 1 FROM emp_projects ep WHERE ep.empno = e.empno\n)\nORDER BY e.empno\nLIMIT 10\n""").show()\n\nspark.sql("""\nSELECT e.empno, e.ename\nFROM emp e\nWHERE NOT EXISTS (\n    SELECT 1 FROM emp_projects ep WHERE ep.empno = e.empno\n)\nORDER BY e.empno\nLIMIT 10\n""").show()'),
        ],
    ),
    "09_dql_string_functions.ipynb": notebook(
        "DQL: String Functions",
        "String functions clean, transform, and search text columns. Examples use employee names, jobs, departments, and project names.",
        [
            md("## Substring and Concatenation\n\nSpark uses `substring` for part of a string and `concat` or `concat_ws` for joining strings."),
            code('''emp.select(
    "ename",
    F.substring("ename", 1, 5).alias("first_5_chars"),
    F.concat_ws(" - ", "ename", "job").alias("employee_label")
).show(10)

spark.sql("""
SELECT ename,
       SUBSTRING(ename, 1, 5) AS first_5_chars,
       CONCAT(ename, ' - ', job) AS employee_label
FROM emp
LIMIT 10
""").show(truncate=False)'''),
            md("## LOWER and UPPER\n\nUse these functions to normalize text casing."),
            code('emp.select("ename", F.lower("ename").alias("lower_name"), F.upper("job").alias("upper_job")).show(10)\n\nspark.sql("""\nSELECT ename, LOWER(ename) AS lower_name, UPPER(job) AS upper_job\nFROM emp\nLIMIT 10\n""").show()'),
            md("## TRIM, LTRIM, and RTRIM\n\nTrim functions remove unwanted spaces. The sample data is already clean, so this creates a small demo DataFrame."),
            code('messy = spark.createDataFrame([("  accounting  ",), ("  sales",), ("research  ",)], ["raw_text"])\n\nmessy.select(\n    "raw_text",\n    F.trim("raw_text").alias("trimmed"),\n    F.ltrim("raw_text").alias("left_trimmed"),\n    F.rtrim("raw_text").alias("right_trimmed")\n).show(truncate=False)\n\nmessy.createOrReplaceTempView("messy")\nspark.sql("""\nSELECT raw_text, TRIM(raw_text) AS trimmed, LTRIM(raw_text) AS left_trimmed, RTRIM(raw_text) AS right_trimmed\nFROM messy\n""").show(truncate=False)'),
            md("## CHARINDEX Equivalent\n\nIn Spark, use `instr` or `locate` to find the position of text inside a string."),
            code('''dept.select("dname", F.instr("dname", "a").alias("position_of_a")).show()

spark.sql("""
SELECT dname, INSTR(dname, 'a') AS position_of_a
FROM dept
""").show()'''),
            md("## LEFT and RIGHT\n\nSpark SQL has `left` and `right`. In the DataFrame API, `substring` is portable and explicit."),
            code('projects.select(\n    "project_name",\n    F.substring("project_name", 1, 3).alias("left_3"),\n    F.expr("right(project_name, 3)").alias("right_3")\n).show()\n\nspark.sql("""\nSELECT project_name, LEFT(project_name, 3) AS left_3, RIGHT(project_name, 3) AS right_3\nFROM projects\n""").show()'),
            md("## REVERSE, REPLACE, and LENGTH\n\nThese functions are useful for formatting and data quality checks."),
            code('''emp.select(
    "ename",
    F.reverse("ename").alias("reversed_name"),
    F.regexp_replace("ename", "_", "-").alias("dash_name"),
    F.length("ename").alias("name_length")
).show(10)

spark.sql("""
SELECT ename,
       REVERSE(ename) AS reversed_name,
       REPLACE(ename, '_', '-') AS dash_name,
       LENGTH(ename) AS name_length
FROM emp
LIMIT 10
""").show()'''),
        ],
    ),
    "10_dql_aggregate_functions.ipynb": notebook(
        "DQL: Aggregate Functions",
        "Aggregate functions summarize many rows into one value or one value per group.",
        [
            md("## COUNT\n\n`count(*)` counts rows. `count(column)` counts non-null values in that column."),
            code('emp.agg(\n    F.count("*").alias("row_count"),\n    F.count("commission").alias("employees_with_commission")\n).show()\n\nspark.sql("""\nSELECT COUNT(*) AS row_count, COUNT(commission) AS employees_with_commission\nFROM emp\n""").show()'),
            md("## SUM and AVG\n\nUse `sum` for totals and `avg` for averages. `round` makes decimal output easier to read."),
            code('emp.agg(\n    F.sum("sal").alias("total_salary"),\n    F.round(F.avg("sal"), 2).alias("avg_salary"),\n    F.sum(F.coalesce("commission", F.lit(0))).alias("total_commission")\n).show()\n\nspark.sql("""\nSELECT SUM(sal) AS total_salary,\n       ROUND(AVG(sal), 2) AS avg_salary,\n       SUM(COALESCE(commission, 0)) AS total_commission\nFROM emp\n""").show()'),
            md("## MAX and MIN\n\nUse `max` and `min` to find extremes."),
            code('emp.agg(\n    F.max("sal").alias("highest_salary"),\n    F.min("sal").alias("lowest_salary"),\n    F.min("hiredate").alias("first_hire_date"),\n    F.max("hiredate").alias("last_hire_date")\n).show()\n\nspark.sql("""\nSELECT MAX(sal) AS highest_salary,\n       MIN(sal) AS lowest_salary,\n       MIN(hiredate) AS first_hire_date,\n       MAX(hiredate) AS last_hire_date\nFROM emp\n""").show()'),
            md("## Aggregates by Group\n\nAggregates are often grouped by department, job, project, or salary grade."),
            code('emp.join(dept, "deptno").groupBy("deptno", "dname").agg(\n    F.count("*").alias("employee_count"),\n    F.round(F.avg("sal"), 2).alias("avg_salary"),\n    F.max("sal").alias("max_salary"),\n    F.min("sal").alias("min_salary")\n).orderBy("deptno").show()\n\nspark.sql("""\nSELECT d.deptno, d.dname,\n       COUNT(*) AS employee_count,\n       ROUND(AVG(e.sal), 2) AS avg_salary,\n       MAX(e.sal) AS max_salary,\n       MIN(e.sal) AS min_salary\nFROM emp e\nJOIN dept d ON e.deptno = d.deptno\nGROUP BY d.deptno, d.dname\nORDER BY d.deptno\n""").show()'),
        ],
    ),
    "11_dql_analytical_functions.ipynb": notebook(
        "DQL: Analytical Functions",
        "Analytical functions calculate values across related rows while keeping row-level detail. They are powered by window specifications.",
        [
            md("## Window Setup\n\nA window defines the rows each analytical function can see. This example partitions by department and orders by salary."),
            code('dept_salary_window = Window.partitionBy("deptno").orderBy(F.col("sal").desc())\nall_salary_window = Window.orderBy(F.col("sal").desc())'),
            md("## ROW_NUMBER, RANK, and DENSE_RANK\n\n`row_number` gives every row a unique sequence. `rank` leaves gaps after ties. `dense_rank` does not leave gaps."),
            code('emp.select(\n    "deptno", "empno", "ename", "job", "sal",\n    F.row_number().over(dept_salary_window).alias("row_number_in_dept"),\n    F.rank().over(dept_salary_window).alias("rank_in_dept"),\n    F.dense_rank().over(dept_salary_window).alias("dense_rank_in_dept")\n).orderBy("deptno", "row_number_in_dept").show(30)\n\nspark.sql("""\nSELECT deptno, empno, ename, job, sal,\n       ROW_NUMBER() OVER (PARTITION BY deptno ORDER BY sal DESC) AS row_number_in_dept,\n       RANK() OVER (PARTITION BY deptno ORDER BY sal DESC) AS rank_in_dept,\n       DENSE_RANK() OVER (PARTITION BY deptno ORDER BY sal DESC) AS dense_rank_in_dept\nFROM emp\nORDER BY deptno, row_number_in_dept\n""").show(30)'),
            md("## NTILE\n\n`ntile(n)` splits ordered rows into `n` buckets. This example places employees into four salary bands."),
            code('emp.select(\n    "empno", "ename", "sal",\n    F.ntile(4).over(all_salary_window).alias("salary_quartile")\n).orderBy("salary_quartile", F.col("sal").desc()).show(30)\n\nspark.sql("""\nSELECT empno, ename, sal,\n       NTILE(4) OVER (ORDER BY sal DESC) AS salary_quartile\nFROM emp\nORDER BY salary_quartile, sal DESC\n""").show(30)'),
            md("## LAG and LEAD\n\n`lag` looks at a previous row. `lead` looks at a following row. They are useful for comparisons."),
            code('hire_window = Window.orderBy("hiredate")\n\nemp.select(\n    "empno", "ename", "hiredate", "sal",\n    F.lag("sal").over(hire_window).alias("previous_salary"),\n    F.lead("sal").over(hire_window).alias("next_salary")\n).orderBy("hiredate").show(20)\n\nspark.sql("""\nSELECT empno, ename, hiredate, sal,\n       LAG(sal) OVER (ORDER BY hiredate) AS previous_salary,\n       LEAD(sal) OVER (ORDER BY hiredate) AS next_salary\nFROM emp\nORDER BY hiredate\nLIMIT 20\n""").show()'),
            md("## FIRST_VALUE and LAST_VALUE\n\nThese functions return values from the first or last row in the window frame. For `last_value`, define the frame through the end of the partition."),
            code('dept_full_window = (\n    Window.partitionBy("deptno")\n    .orderBy(F.col("sal").desc())\n    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)\n)\n\nemp.select(\n    "deptno", "empno", "ename", "sal",\n    F.expr("first_value(ename)").over(dept_full_window).alias("highest_paid_in_dept"),\n    F.expr("last_value(ename)").over(dept_full_window).alias("lowest_paid_in_dept")\n).orderBy("deptno", F.col("sal").desc()).show(30)\n\nspark.sql("""\nSELECT deptno, empno, ename, sal,\n       FIRST_VALUE(ename) OVER (\n           PARTITION BY deptno ORDER BY sal DESC\n           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\n       ) AS highest_paid_in_dept,\n       LAST_VALUE(ename) OVER (\n           PARTITION BY deptno ORDER BY sal DESC\n           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\n       ) AS lowest_paid_in_dept\nFROM emp\nORDER BY deptno, sal DESC\n""").show(30)'),
        ],
    ),
}


def topic_cell_pairs(nb):
    pairs = []
    cells = nb["cells"][7:]
    for index in range(0, len(cells), 2):
        if index + 1 < len(cells):
            pairs.append((cells[index], cells[index + 1]))
    return pairs


def section_heading(cell):
    first_line = cell["source"].splitlines()[0]
    return first_line.replace("## ", "").strip()


def make_lesson(filename, title, intro, cells):
    return filename, notebook(title, intro, cells)


def add_lesson(output, folder, counter, suffix, title, intro, cells):
    filename = f"{folder}/{counter:02d}_{suffix}.ipynb"
    output[filename] = notebook(title, intro, cells)
    return counter + 1


def build_split_notebooks(source_notebooks):
    output = {}

    first_lessons = [
        ("01_dql_select.ipynb", "select"),
        ("02_dql_where.ipynb", "where"),
        ("03_dql_group_by.ipynb", "group_by"),
        ("04_dql_having.ipynb", "having"),
        ("05_dql_order_by.ipynb", "order_by"),
    ]
    for counter, (source_name, suffix) in enumerate(first_lessons, start=1):
        source = source_notebooks[source_name]
        output[f"dql_clauses/{counter:02d}_{suffix}.ipynb"] = source

    split_plan = [
        (
            "06_dql_joins.ipynb",
            "joins",
            {
                "Inner Join": ("inner_join", "DQL Join: Inner Join", "An inner join returns rows that match in both tables."),
                "Left Outer Join": ("left_outer_join", "DQL Join: Left Outer Join", "A left outer join keeps all rows from the left table and adds matching rows from the right table."),
                "Right Outer Join": ("right_outer_join", "DQL Join: Right Outer Join", "A right outer join keeps all rows from the right table and adds matching rows from the left table."),
                "Full Outer Join": ("full_outer_join", "DQL Join: Full Outer Join", "A full outer join keeps matched and unmatched rows from both tables."),
                "Joining Multiple Tables": ("multiple_tables", "DQL Join: Multiple Tables", "Multiple-table joins connect more than two tables to answer a broader question."),
                "Non-Equi Join": ("non_equi_join", "DQL Join: Non-Equi Join", "A non-equi join uses comparisons other than equality, such as a salary range."),
            },
            {},
        ),
        (
            "07_dql_set_operations.ipynb",
            "set_operations",
            {
                "UNION": ("union", "DQL Set Operation: UNION", "`UNION` combines two result sets and removes duplicate rows."),
                "UNION ALL": ("union_all", "DQL Set Operation: UNION ALL", "`UNION ALL` combines two result sets and keeps duplicate rows."),
                "INTERSECT": ("intersect", "DQL Set Operation: INTERSECT", "`INTERSECT` returns rows that exist in both result sets."),
                "EXCEPT": ("except", "DQL Set Operation: EXCEPT", "`EXCEPT` returns rows from the first result set that are not in the second result set."),
            },
            {"Prepare Example Sets": ["UNION", "UNION ALL", "INTERSECT", "EXCEPT"]},
        ),
        (
            "08_dql_basic_operators.ipynb",
            "operators",
            {
                "Equality and Inequality": ("equality_inequality", "DQL Operator: Equality and Inequality", "Equality and inequality operators compare one value to another."),
                "IN and NOT IN": ("in_not_in", "DQL Operator: IN and NOT IN", "`IN` checks membership in a list, and `NOT IN` excludes values in a list."),
                "LIKE and NOT LIKE": ("like_not_like", "DQL Operator: LIKE and NOT LIKE", "`LIKE` and `NOT LIKE` match text patterns."),
                "BETWEEN": ("between", "DQL Operator: BETWEEN", "`BETWEEN` checks whether a value falls inside an inclusive range."),
                "Comparison Operators": ("comparisons", "DQL Operator: Comparison Operators", "Comparison operators filter numeric, date, and text values using greater-than and less-than logic."),
                "EXISTS and NOT EXISTS": ("exists_not_exists", "DQL Operator: EXISTS and NOT EXISTS", "`EXISTS` checks for related rows in another table."),
            },
            {},
        ),
        (
            "09_dql_string_functions.ipynb",
            "string_functions",
            {
                "Substring and Concatenation": ("substring_concatenation", "DQL String Functions: Substring and Concatenation", "Substring extracts part of a string, while concatenation joins strings together."),
                "LOWER and UPPER": ("lower_upper", "DQL String Functions: LOWER and UPPER", "`LOWER` and `UPPER` standardize text casing."),
                "TRIM, LTRIM, and RTRIM": ("trim_ltrim_rtrim", "DQL String Functions: TRIM, LTRIM, and RTRIM", "Trim functions remove unwanted spaces from text."),
                "CHARINDEX Equivalent": ("charindex", "DQL String Function: CHARINDEX Equivalent", "Spark uses functions like `instr` to find text position inside a string."),
                "LEFT and RIGHT": ("left_right", "DQL String Functions: LEFT and RIGHT", "`LEFT` and `RIGHT` return characters from the beginning or end of a string."),
                "REVERSE, REPLACE, and LENGTH": ("reverse_replace_length", "DQL String Functions: REVERSE, REPLACE, and LENGTH", "These functions reshape text and help check text quality."),
            },
            {},
        ),
        (
            "10_dql_aggregate_functions.ipynb",
            "aggregate_functions",
            {
                "COUNT": ("count", "DQL Aggregate Function: COUNT", "`COUNT` returns how many rows or non-null values are present."),
                "SUM and AVG": ("sum_avg", "DQL Aggregate Functions: SUM and AVG", "`SUM` adds values, and `AVG` calculates the average."),
                "MAX and MIN": ("max_min", "DQL Aggregate Functions: MAX and MIN", "`MAX` returns the largest value, and `MIN` returns the smallest value."),
                "Aggregates by Group": ("by_group", "DQL Aggregate Functions: Grouped Aggregates", "Grouped aggregates calculate summary values for each category."),
            },
            {},
        ),
        (
            "11_dql_analytical_functions.ipynb",
            "analytical_functions",
            {
                "ROW_NUMBER, RANK, and DENSE_RANK": ("row_number_rank_dense_rank", "DQL Analytical Functions: ROW_NUMBER, RANK, and DENSE_RANK", "Ranking functions assign positions to rows inside a window."),
                "NTILE": ("ntile", "DQL Analytical Function: NTILE", "`NTILE` divides ordered rows into a fixed number of buckets."),
                "LAG and LEAD": ("lag_lead", "DQL Analytical Functions: LAG and LEAD", "`LAG` looks at a previous row, and `LEAD` looks at a following row."),
                "FIRST_VALUE and LAST_VALUE": ("first_last_value", "DQL Analytical Functions: FIRST_VALUE and LAST_VALUE", "These functions return values from the first or last row in a window frame."),
            },
            {
                "Window Setup": [
                    "ROW_NUMBER, RANK, and DENSE_RANK",
                    "NTILE",
                    "LAG and LEAD",
                    "FIRST_VALUE and LAST_VALUE",
                ]
            },
        ),
    ]

    for source_name, folder, sections, dependencies in split_plan:
        counter = 1
        pairs_by_heading = {section_heading(md_cell): (md_cell, code_cell) for md_cell, code_cell in topic_cell_pairs(source_notebooks[source_name])}
        for heading, (suffix, title, intro) in sections.items():
            cells = []
            for dependency_heading, target_headings in dependencies.items():
                if heading in target_headings:
                    cells.extend(pairs_by_heading[dependency_heading])
            cells.extend(pairs_by_heading[heading])
            counter = add_lesson(output, folder, counter, suffix, title, intro, cells)

    return output


split_notebooks = build_split_notebooks(notebooks)

for filename, nb in split_notebooks.items():
    path = Path(filename)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(nb, indent=2), encoding="utf-8")
    print(f"Wrote {filename}")
