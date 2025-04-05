-- Find no of businesses in each category 
WITH CTE AS (
SELECT
    business_id,
    trim(A.value) as category 
FROM tbl_yelp_businesses,
lateral split_to_table(categories, ',') as A 
)

SELECT
    category,
    COUNT(*) as no_of_businesses
FROM cte
GROUP BY 1

-- Find top 10 users who have reviewed the most businesses in the restaurant category
SELECT 
    r.user_id,
    COUNT(DISTINCT r.business_id)
FROM TBL_YELP_REVIEWS r 
INNER JOIN TBL_YELP_BUSINESSES b
ON r.business_id = b.business_id
WHERE b.categories ILIKE '%restaurant%'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10

-- Find the most popular categories of busninesses (based on no of reviews)
WITH CTE AS (
SELECT
    business_id,
    trim(A.value) as category 
FROM tbl_yelp_businesses,
lateral split_to_table(categories, ',') as A 
)
SELECT
    category,
    COUNT(*) as no_of_reviews
FROM cte
INNER JOIN tbl_yelp_reviews r 
ON cte.BUSINESS_ID = r.business_id
GROUP BY 1
ORDER BY 2 DESC

-- Find the top 3 most recent reviews for each business 
WITH CTE AS (
SELECT 
    r.*,
    b.name,
    ROW_NUMBER() OVER(PARTITION BY r.business_id ORDER BY review_date DESC) as rn 
FROM TBL_YELP_REVIEWS r 
INNER JOIN TBL_YELP_BUSINESSES b
ON r.business_id = b.business_id
)
SELECT * FROM cte 
where rn <= 3

-- Find the month with highest no of reviews 
SELECT
    TO_CHAR(review_date, 'Mon') AS review_month,
    COUNT(*) AS no_of_reviews
FROM TBL_YELP_REVIEWS
GROUP BY 1
ORDER BY 2 DESC;

-- Find the percentage of 5-star reviews for each business
SELECT 
    b.business_id,
    b.name,
    COUNT(*) AS total_reviews,
    COUNT(CASE WHEN r.review_stars = 5 THEN 1 ELSE NULL END ) AS star_5_reviews,
    ROUND(star_5_reviews * 100/total_reviews, 2) AS percent_5_star
FROM TBL_YELP_REVIEWS r 
INNER JOIN TBL_YELP_BUSINESSES b
ON r.business_id = b.business_id
GROUP BY 1, 2

-- Find top 5 most reviewed businesses in each city
WITH CTE AS (
SELECT 
    b.city,
    b.business_id,
    b.name,
    COUNT(*) AS total_reviews,
FROM TBL_YELP_REVIEWS r 
INNER JOIN TBL_YELP_BUSINESSES b
ON r.business_id = b.business_id
GROUP BY 1, 2, 3
)

SELECT * FROM cte
QUALIFY ROW_NUMBER() OVER(PARTITION BY city ORDER BY total_reviews DESC) <= 5

-- Find the average rating of businesses that have atleast 100 reviews 
SELECT 
    b.business_id,
    b.name,
    COUNT(*) AS total_reviews,
    ROUND(AVG(review_stars), 2) AS avg_rating
FROM TBL_YELP_REVIEWS r 
INNER JOIN TBL_YELP_BUSINESSES b
ON r.business_id = b.business_id
GROUP BY 1, 2
HAVING COUNT(*) >= 100

-- List the top 10 users who have written the most reviews along with the businesses they have reviewed 
WITH CTE AS (
SELECT 
    r.user_id,
    COUNT(*) AS total_reviews,
FROM TBL_YELP_REVIEWS r 
INNER JOIN TBL_YELP_BUSINESSES b
ON r.business_id = b.business_id
GROUP BY 1
ORDER BY 2 DESC 
LIMIT 10
)

SELECT
    user_id,
    business_id
FROM tbl_yelp_reviews
WHERE user_id IN (SELECT user_id FROM cte)
GROUP BY 1, 2
ORDER BY user_id

-- Find top 10 businesses with highest positive sentiment analysis

SELECT
    b.business_id,
    b.name,
    COUNT(*) AS total_reviews,
FROM TBL_YELP_REVIEWS r 
INNER JOIN TBL_YELP_BUSINESSES b
ON r.business_id = b.business_id
WHERE sentiments = 'Positive'
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 10