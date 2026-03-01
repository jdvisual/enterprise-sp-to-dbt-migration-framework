# Stored Procedure to dbt Conversion Framework
## HEDIS Gap Engine Refactor

Author: Jon Defoe  
GitHub: https://github.com/jdvisual

---

# Executive Summary

Legacy stored procedures are imperative and stateful.
dbt is declarative and DAG-driven.

This document demonstrates how chained-update healthcare stored procedures
can be refactored into deterministic dbt Core models using set-based logic.

---

# 1. Legacy Stored Procedure (Chained Updates)

```sql
CREATE PROC dbo.usp_build_member_measure_status (@meas_year int)
AS
BEGIN

  SELECT m.member_id,
         'CDC_A1C' as measure_id,
         @meas_year as meas_year,
         1 as denom_flag,
         0 as numer_flag,
         0 as excl_flag
  INTO #mm
  FROM dbo.member m
  JOIN dbo.enrollment e ON e.member_id = m.member_id
  WHERE e.coverage_year = @meas_year
    AND e.continuous_enrollment_flag = 1
    AND m.age BETWEEN 18 AND 75;

  UPDATE mm
    SET numer_flag = 1
  FROM #mm mm
  JOIN dbo.claims c ON c.member_id = mm.member_id
  WHERE c.cpt_code IN ('83036','83037')
    AND YEAR(c.svc_date) = @meas_year;

  UPDATE mm
    SET excl_flag = 1
  FROM #mm mm
  JOIN dbo.claims c ON c.member_id = mm.member_id
  WHERE c.hospice_flag = 1
    AND YEAR(c.svc_date) = @meas_year;

  UPDATE mm
    SET gap_flag =
      CASE
        WHEN excl_flag = 1 THEN 0
        WHEN numer_flag = 1 THEN 0
        ELSE 1
      END;

END
```

---

# Key Insight

Chained updates mutate state.
dbt models compute final state deterministically.

Converting stored procedures to dbt is not translation.
It is architectural refactoring.
