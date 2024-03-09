from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ProductCategoryPairs").getOrCreate()

"""

Creating our own DataFrame:
    products_df contains columns: product_id, product_name
    categories_df contains columns: category_id, category_name
    relations_df contains columns: product_id, category_id

"""

# Test data
products_data = [("product1", "Product 1"), ("product2", "Product 2"), ("product3", "Product 3"), ("product4", "Product 4"), ("product5", "Product 5")]
categories_data = [("category1", "Category 1"), ("category2", "Category 2")]
relations_data = [("product1", "category1"), ("product2", "category2"), ("product4", "category1"), ("product5", "category2")]


products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
relations_df = spark.createDataFrame(relations_data, ["product_id", "category_id"])

product_category_pairs = products_df.join(relations_df, "product_id", "left").join(categories_df, "category_id", "left") \
                                       .select("product_name", "category_name").orderBy("product_name")


print("Pairs 'Product name - Category name':")
product_category_pairs.show()

spark.stop()
