-- Creating database and tables
CREATE OR REPLACE DATABASE YELP

CREATE OR REPLACE TABLE YELP.PUBLIC.yelp_reviews (review_text variant)
CREATE OR REPLACE TABLE YELP.PUBLIC.yelp_businesses (business_text variant)

-- Creating storage integration and stage
CREATE STORAGE INTEGRATION my_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::650251726989:role/snowflake-s3-connection'
  STORAGE_ALLOWED_LOCATIONS = ('s3://yelp-reviews-project/');

DESC integration my_s3_integration

CREATE STAGE s3_stage
  URL = 's3://yelp-reviews-project/'
  STORAGE_INTEGRATION = my_s3_integration
  FILE_FORMAT = (TYPE = JSON);

-- Loading data into the tables 
COPY INTO yelp_businesses
FROM @s3_stage/yelp_academic_dataset_business.json;

SELECT * FROM yelp_businesses LIMIT 10;

COPY INTO yelp_reviews
FROM @s3_stage
FILE_FORMAT = (TYPE = JSON)
PATTERN = 'split_file_.*\.json';

SELECT * FROM YELP_REVIEWS LIMIT 10;

-- Creating UDF function for sentiment analysis 
CREATE OR REPLACE FUNCTION analyze_sentiment(text STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('textblob') 
HANDLER = 'sentiment_analyzer'
AS $$
from textblob import TextBlob
def sentiment_analyzer(text):
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return 'Positive'
    elif analysis.sentiment.polarity == 0:
        return 'Neutral'
    else:
        return 'Negative'
$$;

CREATE OR REPLACE TABLE tbl_yelp_reviews AS
SELECT
    review_text:business_id::string as business_id,
    review_text:date::date as review_date,
    review_text:user_id::string as user_id,
    review_text:stars::number as review_stars,
    review_text:text::string as review_text,
    analyze_sentiment(review_text) as sentiments
FROM yelp_reviews;

SELECT * FROM tbl_yelp_reviews LIMIT 10;

CREATE OR REPLACE TABLE tbl_yelp_businesses AS
SELECT
    business_text:business_id::string as business_id,
    business_text:name::string as name,
    business_text:city::string as city,
    business_text:state::string as state,
    business_text:review_count::string as review_count,
    business_text:stars::number as stars,
    business_text:categories::string as categories
FROM yelp_businesses;

SELECT * FROM TBL_YELP_BUSINESSES LIMIT 10;



