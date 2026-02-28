# Enterprise Stored Procedure to dbt Migration Framework

A practical framework for converting legacy, chained-update stored procedures into deterministic dbt Core models with declarative DAG dependencies (Snowflake-first).

## Whatâ€™s inside
- **Whitepaper draft**: dbt conversion example (HEDIS gap engine style)
- **Case study**: Chained updates refactored into event sets + computed flags
- **Patterns**: Incremental merge, dedupe, set-based refactors, handling non-DAG logic

## Why this exists
Stored procedures are imperative and stateful; dbt is declarative and DAG-driven. This repo documents repeatable refactoring patterns that preserve correctness and improve maintainability.

## Case study: HEDIS Gap Engine
See: case_studies/hedis_gap_engine/

## Whitepaper
See: whitepaper/

> Note: Examples are sanitized and contain no proprietary code or client data.
