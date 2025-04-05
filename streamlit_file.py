# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session


# Write directly to the app
st.title("Yelp Data Analysis Dashboard")

# Get the current credentials
session = get_active_session()

# Function to run SQL and return pandas DataFrame
def run_query(query):
    return session.sql(query).to_pandas()

# 1. Number of businesses per category
st.subheader("1. Number of Businesses per Category")
query1 = """
    WITH CTE AS (
        SELECT business_id, TRIM(A.value) AS category
        FROM TBL_YELP_BUSINESSES,
        LATERAL SPLIT_TO_TABLE(categories, ',') AS A
    )
    SELECT category, COUNT(*) AS no_of_businesses
    FROM CTE
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 20
"""
df1 = run_query(query1)
st.bar_chart(df1.set_index("CATEGORY"))

# 2. Top 10 users who reviewed the most restaurants
st.subheader("2. Top 10 Users Who Reviewed Most Restaurants")
query2 = """
    SELECT r.user_id, COUNT(DISTINCT r.business_id) AS reviews
    FROM TBL_YELP_REVIEWS r
    JOIN TBL_YELP_BUSINESSES b ON r.business_id = b.business_id
    WHERE b.categories ILIKE '%restaurant%'
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10
"""
df2 = run_query(query2)
st.bar_chart(df2.set_index("USER_ID"))

# 3. Most popular categories by number of reviews
st.subheader("3. Most Popular Categories by Number of Reviews")
query3 = """
    WITH CTE AS (
        SELECT business_id, TRIM(A.value) AS category
        FROM TBL_YELP_BUSINESSES,
        LATERAL SPLIT_TO_TABLE(categories, ',') AS A
    )
    SELECT category, COUNT(*) AS no_of_reviews
    FROM CTE
    JOIN TBL_YELP_REVIEWS r ON CTE.business_id = r.business_id
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 20
"""
df3 = run_query(query3)
st.bar_chart(df3.set_index("CATEGORY"))

# 4. Most recent 3 reviews per business
st.subheader("4. Top 3 Most Recent Reviews per Business")
query4 = """
    WITH CTE AS (
        SELECT r.*, b.name,
               ROW_NUMBER() OVER(PARTITION BY r.business_id ORDER BY review_date DESC) AS rn
        FROM TBL_YELP_REVIEWS r
        JOIN TBL_YELP_BUSINESSES b ON r.business_id = b.business_id
    )
    SELECT * FROM CTE WHERE rn <= 3
    LIMIT 10000
"""
df4 = run_query(query4)
st.dataframe(df4)

# 5. Month with most reviews
st.subheader("5. Month with Most Reviews")
query5 = """
    SELECT TO_CHAR(review_date, 'Mon') AS review_month,
           COUNT(*) AS no_of_reviews
    FROM TBL_YELP_REVIEWS
    GROUP BY 1
    ORDER BY 2 DESC
"""
df5 = run_query(query5)
st.bar_chart(df5.set_index("REVIEW_MONTH"))

# 6. Percentage of 5-star reviews per business
st.subheader("6. Percentage of 5-Star Reviews per Business")
query6 = """
    SELECT b.business_id, b.name,
           COUNT(*) AS total_reviews,
           COUNT(CASE WHEN r.review_stars = 5 THEN 1 END) AS star_5_reviews,
           ROUND(COUNT(CASE WHEN r.review_stars = 5 THEN 1 END) * 100.0 / COUNT(*), 2) AS percent_5_star
    FROM TBL_YELP_REVIEWS r
    JOIN TBL_YELP_BUSINESSES b ON r.business_id = b.business_id
    GROUP BY 1, 2
    ORDER BY percent_5_star DESC
    LIMIT 20
"""
df6 = run_query(query6)
st.dataframe(df6)

# 7. Top 5 reviewed businesses per city
st.subheader("7. Top 5 Reviewed Businesses per City")
query7 = """
    WITH CTE AS (
        SELECT b.city, b.business_id, b.name, COUNT(*) AS total_reviews
        FROM TBL_YELP_REVIEWS r
        JOIN TBL_YELP_BUSINESSES b ON r.business_id = b.business_id
        GROUP BY 1, 2, 3
    )
    SELECT * FROM CTE
    QUALIFY ROW_NUMBER() OVER(PARTITION BY city ORDER BY total_reviews DESC) <= 5
"""
df7 = run_query(query7)
st.dataframe(df7)

# 8. Top 10 users and their reviewed businesses
st.subheader("8. Top 10 Users and Their Reviewed Businesses")
query9 = """
    WITH CTE AS (
        SELECT r.user_id, COUNT(*) AS total_reviews
        FROM TBL_YELP_REVIEWS r
        JOIN TBL_YELP_BUSINESSES b ON r.business_id = b.business_id
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10
    )
    SELECT user_id, business_id
    FROM TBL_YELP_REVIEWS
    WHERE user_id IN (SELECT user_id FROM CTE)
    GROUP BY 1, 2
"""
df9 = run_query(query9)
st.dataframe(df9)

# 9. Avg rating for businesses with 100+ reviews
st.subheader("9. Average Rating for Businesses with 100+ Reviews")
query8 = """
    SELECT b.business_id, b.name, COUNT(*) AS total_reviews,
           ROUND(AVG(review_stars), 2) AS avg_rating
    FROM TBL_YELP_REVIEWS r
    JOIN TBL_YELP_BUSINESSES b ON r.business_id = b.business_id
    GROUP BY 1, 2
    HAVING COUNT(*) >= 100
    ORDER BY avg_rating DESC
"""
df8 = run_query(query8)
st.dataframe(df8)



# 10. Top 10 businesses with most positive sentiment reviews
st.subheader("10. Top 10 Businesses with Positive Sentiment")
query10 = """
    SELECT b.business_id, b.name, COUNT(*) AS total_reviews
    FROM TBL_YELP_REVIEWS r
    JOIN TBL_YELP_BUSINESSES b ON r.business_id = b.business_id
    WHERE sentiments = 'Positive'
    GROUP BY 1, 2
    ORDER BY 3 DESC
    LIMIT 10
"""
df10 = run_query(query10)
df10 = df10.set_index("NAME")
st.bar_chart(df10["TOTAL_REVIEWS"])