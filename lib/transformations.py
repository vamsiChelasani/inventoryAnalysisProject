def transformAndSaveDF(spark,combined_cleaned_df):
    combined_cleaned_df.createOrReplaceTempView('total_inventory')

    #### MONTHLY TOTAL VALUE COMPANY WISE
    monthly_values_companywise_df = spark.sql('''
    SELECT
       company_name,
       inventory_month,
       date_format(inventory_month,'MMMM') as month,
       ROUND(sum(opening_value),2) as opening_value,
       ROUND(sum(receipt_value),2) as receipt_value,
       ROUND(sum(issue_value),2) as issue_value,
       ROUND(sum(closing_value),2) as closing_value
    FROM total_inventory
    GROUP BY company_name, inventory_month
    ORDER BY inventory_month ASC, closing_value DESC
    ''')

    monthly_values_companywise_df.coalesce(1).write \
    .mode('overwrite') \
    .option('header',True) \
    .format('csv') \
    .option('path','final_data/monthly_values_companywise/') \
    .save()

    #### MONTHLY TOTAL VALUES vs MONTH-on-MONTH GROWTH RATE

    monthly_values_vs_growth_rate_df = spark.sql('''
    with MonthlySumValue as(
        SELECT
            inventory_month,
            ROUND(SUM(opening_value),2) AS total_opening_value,
            ROUND(SUM(receipt_value),2) AS total_receipt_value,
            ROUND(SUM(issue_value),2) AS total_issue_value,
            ROUND(SUM(closing_value),2) AS total_closing_value
        FROM total_inventory
        GROUP BY inventory_month
    )
    SELECT
        inventory_month,
        date_format(inventory_month, 'MMMM') AS month,
        total_opening_value,
        ROUND(((total_opening_value - LAG(total_opening_value) OVER( ORDER BY inventory_month)) / LAG(total_opening_value) OVER( ORDER BY inventory_month)) * 100,2) AS MoM_growth_percent_ov,
        total_receipt_value,
        ROUND(((total_receipt_value - LAG(total_receipt_value) OVER( ORDER BY inventory_month)) / LAG(total_receipt_value) OVER( ORDER BY inventory_month)) * 100,2) AS MoM_growth_percent_rv,
        total_issue_value,
        ROUND(((total_issue_value - LAG(total_issue_value) OVER( ORDER BY inventory_month)) / LAG(total_issue_value) OVER( ORDER BY inventory_month)) * 100,2) AS MoM_growth_percent_iv,
        total_closing_value,
        ROUND(((total_closing_value - LAG(total_closing_value) OVER( ORDER BY inventory_month)) / LAG(total_closing_value) OVER( ORDER BY inventory_month)) * 100,2) AS MoM_growth_percent_cv
    FROM MonthlySumValue
    ORDER BY inventory_month
    ''')

    monthly_values_vs_growth_rate_df.coalesce(1).write \
    .mode('overwrite') \
    .option('header',True) \
    .format('csv') \
    .option('path','final_data/monthly_values_vs_growth_rate/') \
    .save()

    #### MONTHLY TOTAL VALUE vs SIMPLE_MOVING_AVERAGE (3 MONTHS)

    monthly_values_vs_moving_avg_df = spark.sql('''
    WITH SummedValues AS (
        SELECT
            inventory_month,
            date_format(inventory_month, 'MMMM') AS month,
            ROUND(SUM(opening_value), 2) AS total_opening_value,
            ROUND(SUM(receipt_value), 2) AS total_receipt_value,
            ROUND(SUM(issue_value), 2) AS total_issue_value,
            ROUND(SUM(closing_value), 2) AS total_closing_value
        FROM total_inventory
        GROUP BY inventory_month
    )

    SELECT
        inventory_month,
        month,
        total_opening_value,
        ROUND(AVG(total_opening_value) OVER (ORDER BY inventory_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS avg_opening_value,
        total_receipt_value,
        ROUND(AVG(total_receipt_value) OVER (ORDER BY inventory_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS avg_receipt_value,
        total_issue_value,
        ROUND(AVG(total_issue_value) OVER (ORDER BY inventory_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS avg_issue_value,
        total_closing_value,
        ROUND(AVG(total_closing_value) OVER (ORDER BY inventory_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS avg_closing_value
    FROM SummedValues
    ORDER BY inventory_month
    ''')
    
    monthly_values_vs_moving_avg_df.coalesce(1).write \
    .mode('overwrite') \
    .option('header',True) \
    .format('csv') \
    .option('path','final_data/monthly_values_vs_moving_avg/') \
    .save()

    #### 3-Month Moving Average vs MONTHLY TOTAL VALUE COMPANY WISE

    monthly_company_total_vs_moving_avg_df = spark.sql('''
    WITH MonthlyAggregates AS (
        SELECT
            company_name,
            inventory_month,
            ROUND(sum(opening_value),2) as total_opening_value,
            ROUND(sum(receipt_value),2) as total_receipt_value,
            ROUND(sum(issue_value),2) as total_issue_value,
            ROUND(sum(closing_value),2) as total_closing_value
        FROM total_inventory
        GROUP BY company_name, inventory_month
    )

    SELECT
        company_name,
        inventory_month,
        date_format(inventory_month, 'MMMM') AS month,
        total_opening_value,
        ROUND(AVG(total_opening_value) OVER (PARTITION BY company_name ORDER BY inventory_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS avg_opening_value,
        total_receipt_value,
        ROUND(AVG(total_receipt_value) OVER (PARTITION BY company_name ORDER BY inventory_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS avg_receipt_value,
        total_issue_value,
        ROUND(AVG(total_issue_value) OVER (PARTITION BY company_name ORDER BY inventory_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS avg_issue_value,
        total_closing_value,
        ROUND(AVG(total_closing_value) OVER (PARTITION BY company_name ORDER BY inventory_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS avg_closing_value
    FROM MonthlyAggregates
    ORDER BY inventory_month ASC
    ''')

    monthly_company_total_vs_moving_avg_df.coalesce(1).write \
    .mode('overwrite') \
    .option('header',True) \
    .format('csv') \
    .option('path','final_data/monthly_company_total_vs_moving_avg/') \
    .save()

    #### MONTHLY INVENTORY TURN OVER RATIO

    monthly_inventory_turnover_ratio_df = spark.sql('''
    SELECT
        inventory_month,
        date_format(inventory_month,'MMMM') AS month,
        CASE
            WHEN (sum(opening_value) + sum(closing_value)) = 0 THEN NULL
            ELSE ROUND(sum(issue_value) / ((sum(opening_value) + sum(closing_value))/2),2)
        END AS inventory_turnover_ratio
    FROM total_inventory
    GROUP BY inventory_month
    ORDER BY inventory_month
    ''')

    monthly_inventory_turnover_ratio_df.coalesce(1).write \
    .mode('overwrite') \
    .option('header',True) \
    .format('csv') \
    .option('path','final_data/monthly_inventory_turnover_ratio/') \
    .save()

    #### COMPANY WISE INVENTORY TURNOVER RATIO MONTHLY

    company_inventory_turnover_ratio_df = spark.sql('''
    SELECT
        company_name,
        inventory_month,
        date_format(inventory_month,'MMMM') AS month,
        ROUND(sum(opening_value),2) as total_opening_value,
        ROUND(sum(issue_value),2) as total_issue_value,
        ROUND(sum(closing_value),2) as total_closing_value,
        CASE
            WHEN (sum(opening_value) + sum(closing_value)) = 0 THEN NULL
            ELSE ROUND(sum(issue_value) / ((sum(opening_value) + sum(closing_value))/2),2)
        END AS inventory_turnover_ratio
    FROM total_inventory
    GROUP BY company_name , inventory_month
    ORDER BY company_name , inventory_month
    ''')

    company_inventory_turnover_ratio_df.coalesce(1).write \
    .mode('overwrite') \
    .option('header',True) \
    .format('csv') \
    .option('path','final_data/company_inventory_turnover_ratio/') \
    .save()

    #### COMAPANY WISE 3-MONTH MOVING AVERAGE INVENTORY TURNOVER RATIO

    inventory_turnover_ratio_vs_avg_ratio_df = spark.sql('''
    WITH MonthlyAggregates AS (
        SELECT
            company_name,
            inventory_month,
            SUM(opening_value) AS total_opening_value,
            SUM(closing_value) AS total_closing_value,
            SUM(issue_value) AS total_issue_value,
            CASE 
                WHEN (SUM(opening_value) + SUM(closing_value)) = 0 THEN 0
                ELSE SUM(issue_value) / ((SUM(opening_value) + SUM(closing_value)) / 2.0)
            END AS inventory_turnover_ratio
        FROM total_inventory
        GROUP BY company_name, inventory_month
    ),

    InventoryTurnover AS (
        SELECT
            company_name,
            inventory_month,
            inventory_turnover_ratio,
            total_opening_value,
            total_closing_value,
            total_issue_value,
            SUM(total_opening_value) OVER (PARTITION BY company_name ORDER BY inventory_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_opening_value,
            SUM(total_closing_value) OVER (PARTITION BY company_name ORDER BY inventory_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_closing_value,
            SUM(total_issue_value) OVER (PARTITION BY company_name ORDER BY inventory_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_issue_value
        FROM MonthlyAggregates
    )

    SELECT
        company_name,
        inventory_month,
        ROUND(inventory_turnover_ratio,2) as inventory_turnover_ratio,
        CASE 
            WHEN (avg_opening_value + avg_closing_value) = 0 THEN 0
            ELSE ROUND(avg_issue_value / ((avg_opening_value + avg_closing_value) / 2.0),2)
        END AS avg_inventory_turnover_ratio
    FROM InventoryTurnover
    ORDER BY company_name, inventory_month
    ''')

    inventory_turnover_ratio_vs_avg_ratio_df.coalesce(1).write \
    .mode('overwrite') \
    .option('header',True) \
    .format('csv') \
    .option('path','final_data/inventory_turnover_ratio_vs_avg_ratio/') \
    .save()

    #### DAYS SALES OF INVENTORY(DSI) TO FIND FAST/SLOW MOVING GOODS

    days_sales_inventory_df = spark.sql('''
    WITH InventoryTurnover AS (
        SELECT
            item,
            company_name,
            AVG((opening_quantity + closing_quantity) / 2.0) AS average_inventory,
            SUM(issue_quantity) AS total_issues,
            COUNT(CASE WHEN opening_quantity + closing_quantity = 0 THEN 1 ELSE NULL END) AS zero_inventory_count
        FROM total_inventory
        GROUP BY company_name, item
    ),

    DSICalculation AS (
        SELECT
            item,
            company_name,
            average_inventory,
            total_issues,
            zero_inventory_count,
            CASE
                WHEN total_issues = 0 THEN NULL
                ELSE ROUND((average_inventory / total_issues) * 365, 2)
            END AS days_sales_of_inventory
        FROM InventoryTurnover
    )

    SELECT
        item,
        company_name,
        days_sales_of_inventory,
        zero_inventory_count
    FROM DSICalculation
    WHERE days_sales_of_inventory IS NOT NULL
    ORDER BY company_name, days_sales_of_inventory DESC
    ''')

    days_sales_inventory_df.coalesce(1).write \
    .mode('overwrite') \
    .option('header',True) \
    .format('csv') \
    .option('path','final_data/days_sales_inventory/') \
    .save()