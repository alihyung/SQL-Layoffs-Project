-- Data Cleaning

select *
from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null values or blank values
-- 4. Remove any Columns or Rows

create table layoffs_staging
like layoffs; 
-- new empty table

insert layoffs_staging
select *
from layoffs;
-- inserted everything from layoffs

select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;
-- assigned row numbers

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select * 
from duplicate_cte
where row_num > 1; -- look for those row that have value 2 in a row_num column

select *
from layoffs_staging
where company = 'Casper'; -- checking if it has duplicates

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
delete 
from duplicate_cte
where row_num > 1; -- didn't work, can't delete



CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
-- creating new table

select *
from layoffs_staging2
where row_num > 1;
 
insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;
-- created new table with row numbers


delete 
from layoffs_staging2
where row_num > 1; -- deleted duplicates

select *
from layoffs_staging2;

-- Standardizing Data

select company,  trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company); -- removed spaces from names of companies

select distinct country
from layoffs_staging2
order by 1;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%'; -- fixed different Crypto names

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%'; -- fixed United States.

select `date`
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');  -- changed date format

alter table layoffs_staging2
modify column `date` date; -- changed date column type to date

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging2
set industry = null
where industry = ''; -- changed all empty values to null

select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company = "Bally's Interactive";

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null; -- set the same industry for the same compamies

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null; -- deleted nulls rows

select *
from layoffs_staging2
where total_laid_off is nulls
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num; -- removed row_num column

select *
from layoffs_staging2;