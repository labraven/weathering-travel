# This is how we might compare operators

| PostgreSQL Operator | Function                  | Example                                 |
|---------------------|---------------------------|-----------------------------------------|
| `=`                 | Equal to                  | `WHERE school = 'Baker Middle`          |
| `<>` or `!=`        | Not equal to              | `WHERE school != 'Baker Middle`         |
| `>`                 | Greater than              | `WHERE age > 18`                        |
| `<`                 | Less than                 | `WHERE age < 18`                        |
| `>=`                | Greater than or equal to  | `WHERE age >= 18`                       |
| `<=`                | Less than or equal to     | `WHERE age <= 18`                       |
| `BETWEEN`           | Between a range           | `WHERE age BETWEEN 18 AND 21`           |
| `IN`                | Check for multiple values | `WHERE last_name IN ('Jones', 'Smith')` |
| `LIKE`              | Match a pattern           | `WHERE name LIKE 'J%'`                  |
| `ILIKE`             | Case-insensitive `LIKE`   | `WHERE name ILIKE 'j%'`                 |
| `NOT`               | Negates a condition       | `WHERE NOT age = 18`                    |

## `LIKE` and `ILIKE` wildcards
- `%` is a wildcard character that matches any number of characters
- `_` is a wildcard character that matches a single character
- `LIKE` is case-sensitive and is ANSI
- `ILIKE` is case-insensitive and is PostgreSQL-specific