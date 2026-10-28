-- 01_Create_Schemas.sql
-- Creates the three schemas used by the project. Run this first, once.
-- CREATE SCHEMA has no IF NOT EXISTS form here, so if a schema already exists
-- you will get an "already exists" error for that line. That is safe to ignore.

CREATE SCHEMA Returns_Control;   -- watermark + run metadata
CREATE SCHEMA Returns_Staging;   -- *_stg tables and the Gold merge procedure
CREATE SCHEMA Returns_Gold;      -- star schema (fact + dimensions)
