--Years in the dataset: 1960-2022
select min(year), max(year) from public.aa_wdi_transformed where "Country Code" = 'AFE';

--Let's see what poverty related data do we have by country (some countries doesn't have much data)
--Also, this table has mix of countries and areas or classifications like "Latin America & Caribbean" or "Upper middle income" ??
select
	"Country Name"
	,"Country Code"
	--,"Indicator Code"
	--,"Indicator Name"
	--,year
	,count(value) as num
from
	world_bank.piv_data
where "Indicator Name" ilike '%poverty%'
group by
	"Country Name"
	,"Country Code"
	--,"Indicator Code"
	--,"Indicator Name"
	--,year
--limit 100
--keeping the countries with more data
having count(value) > 200;

-- 'world_bank.piv_data' has 24,902,388 records!
select distinct
	"Country Name"
	, d."Country Code"
	--,"Indicator Code"
	--,"Indicator Name"
	--,year
	, c."Region"
	, c."Income Group"
from
	world_bank.piv_data d JOIN world_bank.country c ON d."Country Code" = c."Country Code";

-- Which latin american countries have more data about poverty ?
select
	"Country Name"
	, d."Country Code"
	--,"Indicator Code"
	--,"Indicator Name"
	--,year
	--, c."Region"
	--, c."Income Group"
	, count(d."value") as poverty_values_num
from
	world_bank.country c JOIN world_bank.piv_data d ON c."Country Code" = d."Country Code"
where
	c."Region" ilike '%latin%'
	AND d."Indicator Name" ilike '%poverty%'
group by
	"Country Name"
	, d."Country Code"
order by poverty_values_num desc


-- Which poverty indicators have more data for different countries and from 1980 onboard ?
-- From the data we can see that one indicator could be 'SI.POV.DDAY' (% of population living with less than 2.15 a day)
select
	"Country Name","Country Code",  "Indicator Code", "Indicator Name"
	--, year
	, count(value) as num
from
	world_bank.piv_data
where
	"Indicator Name" ilike '%poverty%'
	--AND "Indicator Name" ilike '%headcount%'
	AND ("Country Code" = 'ARG' or "Country Code" = 'CHL' or "Country Code" = 'URY')
	AND (year::integer > 1980)
	AND "Indicator Code" = 'SI.POV.DDAY'
group by "Country Name","Country Code",  "Indicator Code","Indicator Name"
having count(value) > 0
order by num desc


-- Lets create a new table based on the 'SI.POV.DDAY' indicator for the whole world
-- This is the table I will later use in the BI tool
-- 16,695
select count(*)
from
	world_bank.piv_data d JOIN world_bank.country c ON d."Country Code" = c."Country Code"
where
	"Indicator Code" = 'SI.POV.DDAY'

CREATE TABLE world_bank.extreme_poverty as (
select
	d.*
	, c."Region"
	, c."Income Group"
	, c."Other groups"
	--, c."System of National Accounts"
	, c."Latest population census"
	--, c."Latest household survey"
	--, c."Latest agricultural census"
	--, c."Latest industrial data"
	--, c."Latest trade data"
from
	world_bank.piv_data d JOIN world_bank.country c ON d."Country Code" = c."Country Code"
where
	"Indicator Code" = 'SI.POV.DDAY')

