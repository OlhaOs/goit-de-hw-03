from ctypes.wintypes import PUINT

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyGoitSparkSandbox").getOrCreate()

# 1
users_df = spark.read.csv('./users.csv', header=True)
purchases_df = spark.read.csv('./purchases.csv', header=True)
products_df = spark.read.csv('./products.csv', header=True)

users_df.show(10)
purchases_df.show(10)
products_df.show(10)

#2
print("-----------2-----------")
users_df = users_df.dropna()
purchases_df = purchases_df.dropna()
products_df = products_df.dropna()

users_df.show(10)
purchases_df.show(10)
products_df.show(10)

#3
users_df.createTempView("users_view")
purchases_df.createTempView("purchases_view")
products_df.createTempView("products_view")



products_cdf = spark.sql("SELECT P.*, P.price * COALESCE((Select sum(quantity) " \
"from purchases_view PU where product_id = P.product_id),0) as total ""FROM products_view P ")

products_cdf.createTempView("products_total")

total_cat_df = spark.sql("SELECT category, round(sum(total),2) as total ""FROM products_total ""group by category")

print("-----------3-----------")
total_cat_df.show()
total_cat_df.createTempView("total_cat")

#4
total_cat_age = spark.sql("Select category, round(sum(total),2) as total from "
          "(SELECT P.category, (quantity * price) as total "
          "FROM purchases_view PU "
          "join products_view P on PU.product_id = P.product_id " 
          "where user_id in (Select user_id from users_view where age between 18 and 25)) s "
          "group by category ")

print("-----------4-----------")
total_cat_age.show()
total_cat_age.createTempView("total_cat_age")

#5
total_cat_age_per = spark.sql("SELECT TC.category, round(((TCA.total * 100) / TC.total),2) as percentage "
"FROM total_cat TC "
"join total_cat_age TCA on TC.category = TCA.category ")

print("-----------5-----------")
total_cat_age_per.show()
total_cat_age_per.createTempView("total_cat_age_per")

#6

print("-----------6-----------")
spark.sql("SELECT * ""FROM total_cat_age_per TC "
          "order by percentage DESC LIMIT 3 "
          ).show()