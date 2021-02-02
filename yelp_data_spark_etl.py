from pyspark.sql.types import *
from pyspark.sql.functions import *

access_key = <'ACCESS KEY'>
secret_key = <'SECRET KEY'>
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name_curated = "yelp-curated"
mount_name_curated = "yelp-curated"
aws_bucket_name_raw = "yelp-raw"
mount_name_raw = "yelp-raw"
try:
  dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name_curated), "/mnt/%s" % mount_name_curated)
  dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name_raw), "/mnt/%s" % mount_name_raw)
except  Exception  as e:
  print('Already Mounted')
  
  
'''
Define the schema for the reviews, business and users data before reading in the dataframes.
Rename the column names to impose consistent naming convention.
'''

reviews_schema = StructType([
  StructField("business_id", StringType(), True),
  StructField("cool", LongType(), True),
  StructField("date", TimestampType(), True),
  StructField("funny", LongType(), True),
  StructField("review_id", StringType(), True),
  StructField("stars", DoubleType(), True),
  StructField("text", StringType(), True),
  StructField("useful", LongType(), True),
  StructField("user_id", StringType(), True)
])

df_reviews = spark.read.format('json')\
                       .option("mode", "DROPMALFORMED")\
                       .schema(reviews_schema)\
                       .load('/mnt/yelp-raw/reviews/*.json')

df_reviews = df_reviews.selectExpr(
                                    "business_id",
                                    "cool as review_cool",
                                    "date as review_date",
                                    "funny as review_funny",
                                    "review_id",
                                    "stars as review_stars",
                                    "text as review_text",
                                    "useful as review_useful",
                                    "user_id"
                                  )

business_schema = StructType([
  StructField("business_id", StringType(), True),
  StructField("name", StringType(), True),
  StructField("city", StringType(), True),
  StructField("state", StringType(), True),
  StructField("categories", StringType(), True),
  StructField("postal_code", StringType(), True),
  StructField("review_count", IntegerType(), True),
  StructField("stars", DoubleType(), True)
])

df_business = spark.read.format('csv')\
                       .option("mode", "DROPMALFORMED")\
                       .option('header',True)\
                       .schema(business_schema)\
                       .load('/mnt/yelp-raw/*.csv')

df_business = df_business.selectExpr(
                                    "business_id",
                                    "name as business_name",
                                    "city as business_city",
                                    "state as business_state",
                                    "categories as business_categories",
                                    "postal_code as business_postal_code",
                                    "review_count as business_review_count",
                                    "stars as business_stars"
                                  )

users_schema = StructType([
  StructField("average_stars", DoubleType(), True),
  StructField("compliment_cool", LongType(), True),
  StructField("compliment_cute", LongType(), True),
  StructField("compliment_funny", LongType(), True),
  StructField("compliment_hot", LongType(), True),
  StructField("compliment_list", LongType(), True),
  StructField("compliment_more", LongType(), True),
  StructField("compliment_note", LongType(), True),
  StructField("compliment_photos", LongType(), True),
  StructField("compliment_plain", LongType(), True),
  StructField("compliment_profile", LongType(), True),
  StructField("compliment_writer", LongType(), True),
  StructField("cool", LongType(), True),
  StructField("elite", StringType(), True),
  StructField("fans", LongType(), True),
  StructField("friends", StringType(), True),
  StructField("funny", LongType(), True),
  StructField("name", StringType(), True),
  StructField("review_count", LongType(), True),
  StructField("useful", LongType(), True),
  StructField("user_id", StringType(), True),
  StructField("yelping_since", TimestampType(), True)
])

df_users = spark.read.format('json')\
                       .option("mode", "DROPMALFORMED")\
                       .schema(users_schema)\
                       .load('/mnt/yelp-raw/users/*.json')

df_users = df_users.selectExpr(
                                "average_stars as user_average_stars",
                                "compliment_cool as user_compliment_cool",
                                "compliment_cute as user_compliment_cute",
                                "compliment_funny as user_compliment_funny",
                                "compliment_hot as user_compliment_hot",
                                "compliment_list as user_compliment_list",
                                "compliment_more as user_compliment_more",
                                "compliment_note as user_compliment_note",
                                "compliment_photos as user_compliment_photos",
                                "compliment_plain as user_compliment_plain",
                                "compliment_profile as user_compliment_profile",
                                "compliment_writer as user_compliment_writer",
                                "cool as user_cool",
                                "elite as user_elite",
                                "fans as user_fans",
                                "friends as user_friends",
                                "funny as user_funny",
                                "name as user_name",
                                "review_count as user_review_count",
                                "useful as user_useful",
                                "user_id as user_id",
                                "yelping_since as user_yelping_since"
                              )

'''
Join the reviews dataframe, the business dataframe and the user dataframe to form the review fact table dataframe.
Cache the table for faster future computation
'''

df_review_bus = df_reviews.join(df_business,["business_id"],"left_outer")
df_reviews_staged = df_review_bus.join(df_users,["user_id"],"left_outer")

df_reviews_staged.repartition(100)\
                 .write\
                 .format('parquet')\
                 .save('/mnt/yelp-curated/reviews_fact_table')

df_reviews_staged.cache()

'''
VALIDATION CHECK 1:
Validation check on the staged review dataframe by counting the records and ensure a non-zero results.
'''

if df_reviews_staged.count()==0:
  print('Staged review table is empty!')
  raise


'''
Extract dimention df from the staged df.
'''

df_dim_bus = df_reviews_staged.select(
                                      'business_id',
                                      'business_name',
                                      'business_city',
                                      'business_categories',
                                      'business_stars'
                                     ).distinct()

df_dim_user = df_reviews_staged.select('user_id',
                                       'user_name',
                                       'user_review_count',
                                       'user_average_stars',
                                       'user_yelping_since'
                                      ).distinct()

df_dim_review = df_reviews_staged.select('review_id',
                                         'review_text'
                                        ).distinct()

df_dim_review_time = df_reviews_staged.select("review_date",
                                              hour("review_date").alias('hour'),
                                              dayofmonth("review_date").alias('day'),
                                              weekofyear("review_date").alias('week'),
                                              month("review_date").alias('month'),
                                              year("review_date").alias('year'),
                                              dayofweek("review_date").alias('dayofweek')
                                             ).distinct()

'''
VALIDATION CHECK 2:
Validation check on the dimention dataframes by counting the records and ensure a non-zero results.
'''

count_dim_bus = df_dim_bus.count()
count_dim_user = df_dim_user.count()
count_dim_review = df_dim_review.count()
count_dim_review_time= df_dim_review_time.count()

print(f'count_dim_bus: {count_dim_bus}')
print(f'count_dim_user: {count_dim_user}')
print(f'count_dim_review: {count_dim_review}')
print(f'count_dim_review_time: {count_dim_review_time}')

if count_dim_bus == 0 or count_dim_user == 0 or count_dim_review == 0 or count_dim_review_time == 0:
  print('Dim table has 0 records, Validation failed.')
  raise 
  
'''
Load the dimention table data into the s3 buckets
'''

df_dim_bus.repartition(100)\
          .write\
          .format('parquet')\
          .save('/mnt/yelp-curated/business_dimention_table')

df_dim_user.repartition(100)\
           .write\
           .format('parquet')\
           .save('/mnt/yelp-curated/user_dimention_table')

df_dim_review.repartition(100)\
             .write\
             .format('parquet')\
             .save('/mnt/yelp-curated/review_dimention_table')

df_dim_review_time.repartition(100)\
                  .write\
                  .format('parquet')\
                  .save('/mnt/yelp-curated/time_dimention_table')
