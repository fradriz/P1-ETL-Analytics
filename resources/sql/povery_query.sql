select
	"Country Name","Country Code",  "Indicator Code", "Indicator Name", year, count(value) as num
from aa_wdi_transformed
where
	--"Indicator Name" ilike '%poverty%'
	--AND "Indicator Name" ilike '%headcount%'
	AND "Country Code" = 'ARG'
	AND (year::integer > 1980)
	AND "Indicator Code" = 'SI.POV.DDAY'
group by "Country Name","Country Code",  "Indicator Code","Indicator Name", year
having count(value) > 0
order by year
