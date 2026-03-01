 # Stored Procedure to dbt Migration Framework  
## Refactoring a HEDIS Gap Engine from Imperative to Declarative Architecture  

Author: Jon Defoe  
GitHub: https://github.com/jdvisual  

---

# Executive Summary

Legacy stored procedures are imperative and stateful.  
dbt is declarative and DAG-driven.

This document demonstrates how a chained-update healthcare stored procedure
can be refactored into deterministic dbt Core models using set-based logic,
explicit grain definition, and dependency-driven execution.

Converting stored procedures to dbt is not translation.  
It is architectural refactoring.

---

# Architectural Mismatch: Imperative vs Declarative

| Stored Procedures | dbt Core |
|-------------------|----------|
| Imperative control flow | Declarative model definitions |
| Sequential mutation | Deterministic SELECT statements |
| Temp tables | Layered models |
| Order-dependent logic | Dependency-driven DAG |
| Embedded orchestration | External orchestration |

Stored procedures encode execution order.  
dbt encodes data dependencies.

---

# 1. Legacy Stored Procedure (Chained Updates)

The following simplified example represents a common HEDIS gap engine pattern
using temp tables and sequential updates.

```sql
CREATE PROC dbo.usp_build_member_measure_status (@meas_year int)
AS
BEGIN

  SELECT
      m.member_id,
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

## Observations

- Multiple sequential UPDATE statements mutate state
- Execution order matters
- Temp tables act as intermediate state containers
- Logic is embedded inside procedural control flow

This pattern does not translate directly to dbt.

---

# 2. Refactor Strategy

## Step 1 – Define Canonical Grain

**member_id + measure_id + measurement_year**

This becomes the atomic unit of computation.

## Step 2 – Extract Event Sets

Replace UPDATE statements with deterministic sets:

- Denominator population
- Numerator evidence
- Exclusion evidence

## Step 3 – Compute Final State

Replace mutation logic with a single SELECT expression using CASE.

## Step 4 – Materialize Incrementally

Use dbt incremental model with composite unique key.

---

# 3. dbt Model Refactor (Declarative DAG)

Each dbt model represents a deterministic transformation node.  
Execution order is inferred through `ref()` dependencies rather than explicitly coded control flow.

---

## 3.1 Denominator Population

`int_hedis_population__cdc_a1c.sql`

```sql
select
    m.member_id,
    'CDC_A1C' as measure_id,
    {{ var('meas_year') }} as meas_year
from {{ ref('stg_member') }} m
join {{ ref('stg_enrollment') }} e
  on e.member_id = m.member_id
where e.coverage_year = {{ var('meas_year') }}
  and e.continuous_enrollment_flag = 1
  and m.age between 18 and 75
```

---

## 3.2 Numerator Evidence Set

`int_hedis_numerator_events__cdc_a1c.sql`

```sql
select distinct
    c.member_id,
    'CDC_A1C' as measure_id,
    {{ var('meas_year') }} as meas_year,
    max(c.svc_date) over (partition by c.member_id) as last_evidence_dt
from {{ ref('stg_claims') }} c
where c.cpt_code in ('83036','83037')
  and year(c.svc_date) = {{ var('meas_year') }}
```

---

## 3.3 Exclusion Evidence Set

`int_hedis_exclusion_events__cdc_a1c.sql`

```sql
select distinct
    c.member_id,
    'CDC_A1C' as measure_id,
    {{ var('meas_year') }} as meas_year
from {{ ref('stg_claims') }} c
where c.hospice_flag = 1
  and year(c.svc_date) = {{ var('meas_year') }}
```

---

## 3.4 Final Fact Model (Incremental Merge)

`fct_member_measure_status.sql`

```sql
{{ config(
    materialized='incremental',
    unique_key=['member_id','measure_id','meas_year'],
    incremental_strategy='merge'
) }}

select
    denom.member_id,
    denom.measure_id,
    denom.meas_year,

    case when numer.member_id is not null then 1 else 0 end as numer_flag,
    case when excl.member_id is not null then 1 else 0 end as excl_flag,

    case
        when excl.member_id is not null then 0
        when numer.member_id is not null then 0
        else 1
    end as gap_flag

from {{ ref('int_hedis_population__cdc_a1c') }} denom
left join {{ ref('int_hedis_numerator_events__cdc_a1c') }} numer
  on numer.member_id = denom.member_id
 and numer.meas_year  = denom.meas_year
left join {{ ref('int_hedis_exclusion_events__cdc_a1c') }} excl
  on excl.member_id = denom.member_id
 and excl.meas_year  = denom.meas_year
```

---

# 4. Handling Non-DAG Logic

Certain stored procedures contain patterns that do not map directly to dbt models:

- Iterative loops over measures
- Dynamic SQL generation
- Control table watermark management
- Multi-pass mutation of the same dataset

Refactor approach:

- Replace loops with set-based transformations
- Parameterize measure logic using macros
- Move orchestration to scheduler (Airflow, Control-M, dbt Cloud, etc.)
- Keep dbt layer deterministic and idempotent

---

# 5. Key Insight

Chained updates mutate state.  
dbt models compute final state deterministically from event sets.

The architectural shift is from:

> Stateful mutation  

to  

> Deterministic transformation

This enables:

- Parallel DAG execution
- Testable intermediate layers
- Improved observability
- Scalable measure expansion
- Safer incremental processing

---

# Conclusion

Migrating stored procedures to dbt is not about rewriting SQL.

It is about:

- Redefining grain
- Eliminating order-dependent mutation
- Separating orchestration from transformation
- Designing for determinism and scalability

This pattern has been successfully applied to healthcare HEDIS gap engines,
enabling scalable, testable, and parallelized measure computation.
