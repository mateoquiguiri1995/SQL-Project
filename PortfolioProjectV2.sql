--select *
--from PortfolioProject..CovidDeaths
--order by 3,4

--select *
--from PortfolioProject..CovidVaccinations
--order by 3,4

-- Seleccionamos los datos que usaremos

select location, date,total_cases, new_cases,total_deaths,population
from PortfolioProject..CovidDeaths
where continent is null
order by 1,2

-- A partir de aqui el analisis se centrara en Ecuador
-- Columna calculada, porcentaje de muertes
-- Podemos ver la probabilidad de morir si una persona se contagia de Covid en Ecuador

select location, date,total_cases,total_deaths,(cast(total_deaths as float)/cast(total_cases AS float))*100 AS muertespct
from PortfolioProject..CovidDeaths
where location like '%ecuador%'
order by 1,2

-- Columna calculada, porcentaje de contagiados
-- Se puede ver el porcentaje de la poblacion que contrajo COVID
select location, date,total_cases,population,(cast(total_cases as float)/cast(population AS float))*100 AS contagpct
from PortfolioProject..CovidDeaths
where location like '%ecuador%'
order by 1,2

-- Paises con la tasa de infeccion mas alta

select location, population,MAX(total_cases) as MaximaCantidadCasos,
	MAX(cast(total_cases as float)/cast(population AS float))*100 as contagpct
from PortfolioProject..CovidDeaths
--where location like '%ecuador%'
group by location, population
order by contagpct DESC

-- Paises con la tasa de muertes mas alta

select location,MAX(cast(total_deaths as float)) as MaximaCantidadMuertes,
	MAX(cast(total_deaths as float)/cast(population AS float))*100 as muertespct
from PortfolioProject..CovidDeaths
--where location like '%ecuador%'
where continent is not null
group by location
order by muertespct DESC

-- Ahora agregamos los resultados por continente y los ordenamos

select location,MAX(cast(total_deaths as float)) as MaximaCantidadMuertes,
	MAX(cast(total_deaths as float)/cast(population AS float))*100 as muertespct
from PortfolioProject..CovidDeaths
--where location like '%ecuador%'
where continent is null
group by location
order by MaximaCantidadMuertes DESC

-- A continuacio los continentes con la mayor cantidad de casos

select continent,MAX(cast(total_cases as float)) as MaximaCantidadMuertes
from PortfolioProject..CovidDeaths
--where location like '%ecuador%'
where continent is not null
group by continent
order by MaximaCantidadMuertes DESC

-- A continacion se puede ver los paises de America con la mayor cantidad de
-- contagiados
select location,MAX(cast(total_cases AS FLOAT)) AS NumeroContagiados
from PortfolioProject..CovidDeaths
where continent like '%america%'
group by location
order by NumeroContagiados DESC

-- GLOBAL NUMBERS por dia
-- Podemos ver la evolucion diaria de los principales indicadores del COVID, entre estos
-- Casos totales, Muertes totales y la Mortalidad a nivel mundial.

select date, sum(cast(new_cases as float)) as CasosTotales, sum(cast(new_deaths AS float)) as MuertesTotales,
(sum(cast (new_cases as float))/sum(cast (new_deaths as float)))*100 as Mortalidad 
from PortfolioProject..CovidDeaths
where continent is not null
group by date
order by 1,2

-- Ahora los indicadores para el ____
select sum(cast(new_cases as float)) as CasosTotales, sum(cast(new_deaths AS float)) as MuertesTotales,
(sum(cast (new_cases as float))/sum(cast (new_deaths as float)))*100 as Mortalidad 
from PortfolioProject..CovidDeaths
where continent is not null

-- TABLAS RELACIONALES
-- A continuacion usamos las funciones Join para unir las tablas deaths y vaccinatios
-- mediante una columna en comun, en este caso, date y population. De esta manera podemos ver d
-- informacion acerca de los contagios y la vacunacion.
select dea.continent, dea.location,dea.date, dea.population, vac.new_vaccinations,
sum(cast(vac.new_vaccinations as float) ) OVER (Partition by 
dea.location order by  dea.location,dea.date) as rolling_personas_vacunadas
--(personas_vacunadas/population)*100
from PortfolioProject..CovidDeaths as dea
join PortfolioProject..CovidVaccinations as vac
	on dea.location=vac.location
	and dea.date=vac.date
where dea.continent is not null
order by 2,3

-- USo de CTE 
-- Para crear columnas calculadas a partir de columnas con calculos precedentes es necesario la creacion de tablas temporales
-- A continuacion en la columna pct_pob_vacunada se presenta la evolucion del porcentaje de vacunacion de la poblacion de acuerdo a la fecha

with ppbvsvac (continent, location, date, population,new_vaccinations, rolling_personas_vacunadas) 
as
(select dea.continent, dea.location,dea.date, dea.population, vac.new_vaccinations,
sum(cast(vac.new_vaccinations as float) ) OVER (Partition by 
dea.location order by  dea.location,dea.date) as rolling_personas_vacunadas
--(personas_vacunadas/population)*100
from PortfolioProject..CovidDeaths as dea
join PortfolioProject..CovidVaccinations as vac
	on dea.location=vac.location
	and dea.date=vac.date
where dea.continent is not null
--order by 2,3
)
select *, (rolling_personas_vacunadas/population)*100 as pct_pob_vacunada
from ppbvsvac

-- A la siguiente tabla nos centraremos unicamente en Ecuador, para observar como ha evolucionado el porcentaje de vacunacion de acuerdo a la fecha
with ppbvsvac (continent, location, date, population,new_vaccinations, rolling_personas_vacunadas) 
as
(select dea.continent, dea.location,dea.date, dea.population, vac.new_vaccinations,
sum(cast(vac.new_vaccinations as float) ) OVER (Partition by 
dea.location order by  dea.location,dea.date) as rolling_personas_vacunadas
--(personas_vacunadas/population)*100
from PortfolioProject..CovidDeaths as dea
join PortfolioProject..CovidVaccinations as vac
	on dea.location=vac.location
	and dea.date=vac.date
where dea.location like '%Ecuad%'

--order by 2,3
)
select *, (rolling_personas_vacunadas)*100 as pct_pob_vacunada
from ppbvsvac
-- A continuacion veremos cuantas vacunas se inyectaron en el dia que mas vacunas se pusieron
select MAX(cast(vac.new_vaccinations as float)) max_vacunas_diarias, max(cast (vac.new_vaccinations as float))/max(dea.population) pct_poblacion

from PortfolioProject..CovidDeaths as dea
join PortfolioProject..CovidVaccinations as vac
	on dea.location=vac.location
	and dea.date=vac.date
where dea.location like '%Ecuad%'

-- O podriamos ordernar la tabla penultima tabla de acuerdo a las numero de vacunas diarias para identificar cual fue el dia
-- con el mayor numero de vacunas

with ppbvsvac (continent, location, date, population,new_vaccinations, rolling_personas_vacunadas) 
as
(select dea.continent, dea.location,dea.date, dea.population, cast(vac.new_vaccinations as float),
sum(cast(vac.new_vaccinations as float) ) OVER (Partition by 
dea.location order by  dea.location,dea.date) as rolling_personas_vacunadas
--(personas_vacunadas/population)*100
from PortfolioProject..CovidDeaths as dea
join PortfolioProject..CovidVaccinations as vac
	on dea.location=vac.location
	and dea.date=vac.date
where dea.location like '%Ecuad%'
--order by 2,3
)
select *, (rolling_personas_vacunadas/population)*100 as pct_pob_vacunada
from ppbvsvac
order by 5 DESC
-- Como se puede ver el dia en el cual se inoculo a la mayor cantidad de personas fue el 15 de julio del 2021

-- Otra manera de realizar el calculo anterior es mediante el uso de tablas temporales
-- TEMP TABLE
DROP table if exists #pct_pob_vac_v
create Table #pct_pob_vac_v
(
	 continent nvarchar(255),
	 location nvarchar(255),
	 date datetime,
	 population numeric,
	 new_vaccination numeric,
	 rolling_personas_vacunadas numeric
)
insert into #pct_pob_vac_v

select dea.continent, dea.location,dea.date, dea.population, cast(vac.new_vaccinations as float),
sum(cast(vac.new_vaccinations as float) ) OVER (Partition by 
dea.location order by  dea.location,dea.date) as rolling_personas_vacunadas
--(personas_vacunadas/population)*100
from PortfolioProject..CovidDeaths as dea
join PortfolioProject..CovidVaccinations as vac
	on dea.location=vac.location
	and dea.date=vac.date
--where dea.location like '%Ecuad%'
--order by 2,3

select *, (rolling_personas_vacunadas/population)*100 as pct_pob_vacunada
from #pct_pob_vac_v


-- CREAMOS VIEW PARA SU USO POSTERIOR

create view pct_personas_vacunadas as 
select dea.continent, dea.location,dea.date, dea.population, cast(vac.new_vaccinations as float) as new_vaccinations,
sum(cast(vac.new_vaccinations as float) ) OVER (Partition by 
dea.location order by  dea.location,dea.date) as rolling_personas_vacunadas
--(personas_vacunadas/population)*100
from PortfolioProject..CovidDeaths as dea
join PortfolioProject..CovidVaccinations as vac
	on dea.location=vac.location
	and dea.date=vac.date
where dea.continent is not NULL
--where dea.location like '%Ecuad%'
--order by 2,3
select *
from pct_personas_vacunadas