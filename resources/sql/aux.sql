--Create & use a new schema
CREATE SCHEMA IF NOT EXISTS world_bank;
SET search_path TO world_bank;

--Rename a table
ALTER TABLE wdicountry RENAME TO country;

--CTAS & Drop
CREATE TABLE world_bank.raw_data AS (select * from public.aa_wdi_raw);
DROP TABLE public.aa_wdi_raw;

CREATE TABLE world_bank.piv_data AS (select * from public.aa_wdi_transformed);
DROP TABLE public.aa_wdi_transformed;
