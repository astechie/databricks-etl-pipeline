from pyspark.sql import functions as F

def run_pipeline(spark):
    # -----------------------------
    # Define Tables
    # -----------------------------
    silver_table = "slv.sales"
    gold_tables = ["gld.dim_product", "gld.dim_customer", "gld.dim_campaign", "gld.fact_sales"]
    bronze_table = "brz.sales"

    # -----------------------------
    # Step 1: Truncate Silver Table
    # -----------------------------
    print("Truncating Silver table...")
    df_silver_existing = spark.table(silver_table)
    empty_df = df_silver_existing.limit(0)

    empty_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(silver_table)

    print("Silver table truncated successfully!")

    # -----------------------------
    # Step 2: Truncate Gold Tables
    # -----------------------------
    print("Truncating Gold tables...")
    for table in gold_tables:
        df = spark.table(table)
        empty_df = df.limit(0)
        empty_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table)
        print(f"{table} truncated successfully!")

    # -----------------------------
    # Step 3: Load Bronze → Silver
    # -----------------------------
    print("Loading Bronze → Silver...")
    df_bronze = spark.table(bronze_table)

    df_silver = df_bronze.withColumn(
        "email",
        F.regexp_extract(F.col("`Email Name`"), r"\(([^)]+)\)", 1)
    ).withColumn(
        "last_name",
        F.trim(F.split(F.regexp_extract(F.col("`Email Name`"), r"\):\s*(.*)", 1), ",")[0])
    ).withColumn(
        "first_name",
        F.trim(F.split(F.regexp_extract(F.col("`Email Name`"), r"\):\s*(.*)", 1), ",")[1])
    ).withColumnRenamed("Unit Cost", "Unit_Cost") \
     .withColumnRenamed("Unit Price", "Unit_Price") \
     .withColumnRenamed("Email Name", "Email_Name")

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(silver_table)

    print("Silver table loaded successfully!")

    # -----------------------------
    # Step 4: Normalize Silver → Gold
    # -----------------------------
    print("Loading Silver → Gold...")

    # Dimension: Product
    dim_product = df_silver.select(
        "ProductID", "Product", "Category", "Segment", "ManufacturerID", "Manufacturer"
    ).dropDuplicates()

    dim_product.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("gld.dim_product")

    # Dimension: Customer (without Email_Name)
    dim_customer = df_silver.select(
        "CustomerID", "email", "first_name", "last_name",
        "City", "State", "Region", "ZipCode", "Country"
    ).dropDuplicates()

    dim_customer.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("gld.dim_customer")

    # Dimension: Campaign
    dim_campaign = df_silver.select("CampaignID").dropDuplicates()

    dim_campaign.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("gld.dim_campaign")

    # Fact Table: Sales
    fact_sales = df_silver.select(
        "Date", "CustomerID", "ProductID", "CampaignID",
        "Units", "Unit_Cost", "Unit_Price"
    )

    fact_sales.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("gld.fact_sales")

    print("Gold tables loaded successfully!")
    print("ETL pipeline completed!")


# Run the pipeline
run_pipeline(spark)
