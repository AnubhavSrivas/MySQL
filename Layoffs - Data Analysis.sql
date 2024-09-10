-- DATA CLEANING --
--------------------------------------
SELECT * 
FROM layoffs;

-- 1. REMOVE DUPLICATES.
-- 2. STANDARDIZE THE DATA.
-- 3. HANDLING NULL VALUES OR BLANK VALUES.
-- 4. REMOVE ANY COLUMNS/ROWS THAT WE DON'T NEED.


# To not make changes in the raw data we need to create another table with the same data as the original.

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs_staging;
# At this point we don't have any data in the table, so insert the data from the layoffs table.

INSERT layoffs_staging 
SELECT *
FROM layoffs;


-- 1. REMOVING DUPLICATES.

# Checking duplicate rows from the table.

SELECT *,
ROW_NUMBER() 
OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging;

# Creating another table having row_num column from the above query.

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

# Inserting the data in the new table.

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() 
OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1 ;

# Deleting every duplicate rows.
DELETE
FROM layoffs_staging2
WHERE row_num > 1 ;


-- 2. STANDARDIZING
 
 # Checking  what needs to be standardized. 
 
 SELECT * 
 FROM layoffs_staging2;

# There were some whitespaces in the company column, so removing them. 
SELECT company,(TRIM(company)) AS TC
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

# Same as country, we are checking every other column using distinct, and see if we find anything.
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

# There were rows where industry was named crypto, cryptocurrency and such, so taking care those rows.
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'CRYPTO%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country  = 'United States'
WHERE country LIKE 'United States%';

# This is a way to remove the dots(.) at the end the data.
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER  BY 1;

SELECT `date`
FROM layoffs_staging2;
# Here date column was considered a string, so changing it into date data type.

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. HANDLING NULL VALUES OR BLANK VALUES

# Checking rows where both total_laid_off and _percentage_laid_off are null.
SELECT *  
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# Updating the table where there are blank values into null values.
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

# Populating null values in the industry where possible(e.g. if a row has 'airbnb' from travel industry then filling other null industry values from different 'airbnb' as travel)
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT * 
FROM layoffs_staging2
WHERE company = 'AIRBNB';

-- 4. REMOVING ANY COLUMNS/ROWS THAT WE DON'T NEED
SELECT *  
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;





-- EXPLORATORY DATA ANALYSIS --
--------------------------------------

# Checking max layoff.
SELECT MAX(total_laid_off)
FROM layoffs_staging2;

# Companies that have percentage_laid_off = 1 means that 100 percent of the company has been laid off.
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
# funds_raised_millions showes how big the company was before it went under.

# Companies with the biggest single layoffs.
SELECT company, total_laid_off
FROM layoffs_staging2
ORDER BY 2 DESC
LIMIT 5;

# Period of layoffs available in the dataset.
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

# Sum of total layoffs by company.
SELECT company, SUM(total_laid_off) AS STL
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

# Sum of total layoffs by stage of a company.
SELECT stage, SUM(total_laid_off) AS STL
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

# Sum of total layoffs by location.
SELECT location, Sum(total_laid_off) AS STL
FROM layoffs_staging2
GROUP BY location
ORDER BY 2 DESC;

# Sum of total layoffs by country.
SELECT country, Sum(total_laid_off) AS STL
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

# Sum of total layoffs by year.
SELECT Year(`date`), Sum(total_laid_off) AS STL
FROM layoffs_staging2
GROUP BY Year(`date`)
ORDER BY 2 DESC;

# Sum of total layoffs by industry.
SELECT industry, Sum(total_laid_off) AS STL
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

# Sum of total layoffs per month.
SELECT SUBSTRING(`date`, 1, 7) AS `Month`, SUM(total_laid_off) AS STL
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC;

# Layoffs of companies per year.
SELECT company, YEAR(`date`), SUM(total_laid_off) AS STL
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

# Ranking companies by sum of total layoffs per year.
WITH company_yr (company, YEARS, STL) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
),Company_yr_rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY YEARS ORDER BY STL DESC) AS RANKING
FROM company_yr
WHERE YEARS IS NOT NULL
)
SELECT *
FROM Company_yr_rank
WHERE RANKING <= 5
ORDER BY YEARS ASC, STL DESC;

# Rolling total of layoffs per month.
WITH date_cte AS
(
SELECT SUBSTRING(`date`, 1, 7) as Dates, SUM(total_laid_off) AS STL
FROM layoffs_staging2
GROUP BY Dates
ORDER BY Dates ASC
)
SELECT Dates, SUM(STL) OVER(ORDER BY Dates ASC) AS RTL
FROM date_cte
WHERE Dates IS NOT NULL
ORDER BY Dates ASC;