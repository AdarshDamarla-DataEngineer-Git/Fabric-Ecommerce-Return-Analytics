-- Calendar table for the Power BI semantic model.
-- One row per day from 2020-01-01 to 2035-12-31.
-- Safe to re-run: the table is created once and filled only while it is empty.

IF OBJECT_ID('Returns_Gold.Dim_Date', 'U') IS NULL
CREATE TABLE Returns_Gold.Dim_Date
(
    [Date]         DATE,
    [Year]         INT,
    [Month_Number] INT,
    [Month]        VARCHAR(3),
    [Year_Month]   VARCHAR(7)
);

IF NOT EXISTS (SELECT 1 FROM Returns_Gold.Dim_Date)
INSERT INTO Returns_Gold.Dim_Date ([Date], [Year], [Month_Number], [Month], [Year_Month])
SELECT d,
       YEAR(d),
       MONTH(d),
       LEFT(DATENAME(MONTH, d), 3),
       CONCAT(YEAR(d), '-', RIGHT(CONCAT('0', MONTH(d)), 2))
FROM (
    SELECT DATEADD(DAY, n.n, CAST('2020-01-01' AS DATE)) AS d
    FROM (
        SELECT a.v + 10 * b.v + 100 * c.v + 1000 * e.v AS n
        FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) a(v)
        CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) b(v)
        CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) c(v)
        CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) e(v)
    ) n
    WHERE n.n <= 5843
) x;