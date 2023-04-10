#CREATE TABLES

orders_table_create = ('''
                        CREATE TABLE IF NOT EXISTS orders (
                        order_id INTEGER NOT NULL PRIMARY KEY,
                        customer_id INTEGER NOT NULL,
                        order_date DATE NOT NULL,
                        product_id INTEGER NOT NULL,
                        unit_price INTEGER NOT NULL,
                        quantity INTEGER NOT NULL,
                        total_price INTEGER NOT NULL
                        );''')


reviews_table_create = ('''
                        CREATE TABLE IF NOT EXISTS reviews(
                            review INTEGER NOT NULL PRIMARY KEY,
                            product_id INTEGER NOT NULL
                        );
                        ''')


shipment_table_create = ('''
                        CREATE TABLE IF NOT EXISTS shipment_deliveries(
                        shipment_id INTEGER NOT NULL PRIMARY KEY,
                        order_id INTEGER NOT NULL,
                        shipment_date DATE NULL,
                        delivery_date DATE NULL
                        );''')

agg_public_holiday_table_create = (
                        '''
                        CREATE TABLE idrialug9071_analytics.agg_public_holiday(
                        ingestion_date DATE not null primary key,
                        tt_order_hol_jan integer not null,
                        tt_order_hol_feb integer not null,
                        tt_order_hol_mar integer not null,
                        tt_order_hol_apr integer not null,
                        tt_order_hol_may integer not null,
                        tt_order_hol_jun integer not null,
                        tt_order_hol_jul integer not null,
                        tt_order_hol_aug integer not null,
                        tt_order_hol_sep integer not null,
                        tt_order_hol_oct integer not null,
                        tt_order_hol_nov integer not null,
                        tt_order_hol_dec integer not null
                        );'''
                        )

agg_shipments_table_create = ('''
                        CREATE TABLE idrialug9071_analytics.agg_shipments(
                            ingestion_date DATE not null primary key,
                            tt_late_shipments integer not null,
                            tt_undelivered_shipments integer not null
                            );
                        ''')

best_performing_product_table_create = ('''
                                                  CREATE TABLE idrialug9071_analytics.best_performing_product(
                            ingestion_date date not null primary key,
                            product_name varchar not null,
                            most_ordered_day date not null,
                            is_public_holiday bool not null,
                            tt_review_points integer not null,
                            pct_one_star_review float not null,
                            pct_two_star_review float not null,
                            pct_three_star_review float not null,
                            pct_four_star_review float not null,
                            pct_five_star_review float not null,
                            pct_early_shipments float not null,
                            pct_late_shipments float not null
                            );
                        ''')


#DROP TABLES

agg_public_holiday_table_drop = ('''
                                    DROP TABLE IF EXISTS idrialug9071_analytics.agg_public_holiday;
                                    ''')


agg_shipments_table_drop = ('''
                                    DROP TABLE IF EXISTS idrialug9071_analytics.agg_shipments;
                                ''')

best_performing_product_table_drop = ('''
                                    DROP TABLE IF EXISTS idrialug9071_analytics.best_performing_product;
                                ''')


#INSERT TABLES

orders_table_insert = ('''
                        INSERT INTO orders(order_id, customer_id, order_date, product_id, unit_price, quantity, total_price) 
                        VALUES %s
                        ON CONFLICT (order_id) DO UPDATE 
                        SET 
                        customer_id = excluded.customer_id,
                        order_date = excluded.order_date,
                        product_id = excluded.product_id,
                        unit_price = excluded.unit_price,
                        quantity = excluded.quantity,
                        total_price = excluded.total_price;
                        ''')


reviews_table_insert = ('''
                        INSERT INTO reviews(review, product_id) 
                        VALUES %s;
                        ''')



shipment_table_insert = ('''
                        INSERT INTO shipment_deliveries(shipment_id, order_id, shipment_date, delivery_date) 
                        VALUES %s
                        ON CONFLICT (shipment_id) DO UPDATE 
                        SET 
                        order_id = excluded.order_id,
                        shipment_date = excluded.shipment_date,
                        delivery_date = excluded.delivery_date;
                        ''')

agg_public_holiday_table_insert = ('''
                        INSERT INTO idrialug9071_analytics.agg_public_holiday (
                            ingestion_date,
                            tt_order_hol_jan,
                            tt_order_hol_feb,
                            tt_order_hol_mar,
                            tt_order_hol_apr,
                            tt_order_hol_may,
                            tt_order_hol_jun,
                            tt_order_hol_jul,
                            tt_order_hol_aug,
                            tt_order_hol_sep,
                            tt_order_hol_oct,
                            tt_order_hol_nov,
                            tt_order_hol_dec
                            )
                            SELECT 
                                now() as ingestion_date, 
                                COUNT(case when EXTRACT(MONTH FROM order_date) = 1 AND day_of_the_week_num <= 5 AND working_day = False THEN order_date END) as tt_order_hol_jan,
                                COUNT(case when EXTRACT(MONTH FROM order_date) = 2 AND day_of_the_week_num <= 5 AND working_day = False THEN order_date END) as tt_order_hol_feb,
                                COUNT(case when EXTRACT(MONTH FROM order_date) = 3 AND day_of_the_week_num <= 5 AND working_day = False THEN order_date END) as tt_order_hol_mar,
                                COUNT(case when EXTRACT(MONTH FROM order_date) = 4 AND day_of_the_week_num <= 5 AND working_day = False THEN order_date END) as tt_order_hol_apr,
                                COUNT(case when EXTRACT(MONTH FROM order_date) = 5 AND day_of_the_week_num <= 5 AND working_day = False THEN order_date END) as tt_order_hol_may,
                                COUNT(case when EXTRACT(MONTH FROM order_date) = 6 AND day_of_the_week_num <= 5 AND working_day = False THEN order_date END) as tt_order_hol_jun,
                                COUNT(case when EXTRACT(MONTH FROM order_date) = 7 AND day_of_the_week_num <= 5 AND working_day = False THEN order_date END) as tt_order_hol_jul,
                                COUNT(case when EXTRACT(MONTH FROM order_date) = 8 AND day_of_the_week_num <= 5 AND working_day = False THEN order_date END) as tt_order_hol_aug,
                                COUNT(case when EXTRACT(MONTH FROM order_date) = 9 AND day_of_the_week_num <= 5 AND working_day = False THEN order_date END) as tt_order_hol_sep,
                                COUNT(case when EXTRACT(MONTH FROM order_date) = 10 AND day_of_the_week_num <= 5 AND working_day = False THEN order_date END) as tt_order_hol_oct,
                                COUNT(case when EXTRACT(MONTH FROM order_date) = 11 AND day_of_the_week_num <= 5 AND working_day = False THEN order_date END) as tt_order_hol_nov,
                                COUNT(case when EXTRACT(MONTH FROM order_date) = 12 AND day_of_the_week_num <= 5 AND working_day = False THEN order_date END) as tt_order_hol_dec
                            FROM (
                                SELECT order_date, day_of_the_week_num, working_day
                                FROM idrialug9071_staging.orders 
                                JOIN if_common.dim_dates 
                                    ON orders.order_date = dim_dates.calendar_dt
                                WHERE EXTRACT(YEAR FROM order_date) = 2022
                            ) subquery
                        ''')



agg_shipments_table_insert = ('''
                    INSERT INTO idrialug9071_analytics.agg_shipments (
                            ingestion_date,
                            tt_late_shipments,
                            tt_undelivered_shipments
                            )
                            SELECT 
                                now() as ingestion_date, 
                                COUNT(case when (order_date + INTERVAL '6 day') <= shipment_date AND delivery_date IS NULL THEN 1 END) as tt_late_shipments,
                                COUNT(case when delivery_date IS NULL AND shipment_date IS NULL AND '2022-09-05' > (order_date + INTERVAL '15 day') THEN 1 END) as tt_undelivered_shipments
                            FROM (
                                SELECT orders.order_id, order_date, shipment_date, delivery_date
                                FROM idrialug9071_staging.orders 
                                JOIN idrialug9071_staging.shipment_deliveries
                                ON orders.order_id = shipment_deliveries.order_id
                            ) subquery
''')




best_performing_product_table_insert = ('''
        INSERT INTO idrialug9071_analytics.best_performing_product (
        ingestion_date,
        product_name,
        most_ordered_day,
        is_public_holiday,
        tt_review_points,
        pct_one_star_review,
        pct_two_star_review,
        pct_three_star_review,
        pct_four_star_review,
        pct_five_star_review,
        pct_early_shipments,
        pct_late_shipments
        )
         WITH 
              reviews_cte AS (
                SELECT 
                  product_id, 
                  COUNT(review) AS total_reviews, 
                  AVG(review) AS avg_review_points, 
                  (SUM(CASE WHEN review = 1 THEN 1 ELSE 0 END) / COUNT(review))  AS pct_one_star_review, 
                  (SUM(CASE WHEN review = 2 THEN 1 ELSE 0 END) / COUNT(review))  AS pct_two_star_review, 
                  (SUM(CASE WHEN review = 3 THEN 1 ELSE 0 END) / COUNT(review))  AS pct_three_star_review, 
                  (SUM(CASE WHEN review = 4 THEN 1 ELSE 0 END) / COUNT(review))  AS pct_four_star_review, 
                  (SUM(CASE WHEN review = 5 THEN 1 ELSE 0 END) / COUNT(review))  AS pct_five_star_review
                FROM 
                  idrialug9071_staging.reviews
                GROUP BY 
                  product_id
                ORDER BY avg_review_points DESC
              ),

                orders_cte as(
                SELECT 
                        product_id, 
                        order_date,
                        day_of_the_week_num, 
                        working_day,
                        RANK() OVER (PARTITION BY product_id ORDER BY COUNT(*) DESC) as rank
                    FROM orders
                    JOIN if_common.dim_dates ON orders.order_date = dim_dates.calendar_dt
                    GROUP BY product_id, order_date, day_of_the_week_num, working_day
                    ),


              shipments_cte AS (
                SELECT 
                  orders.product_id, 
                  COUNT(case when (order_date + INTERVAL '6 day') >= shipment_date AND delivery_date IS NULL THEN 1 END) as tt_early_shipments,
                  COUNT(case when (order_date + INTERVAL '6 day') <= shipment_date AND delivery_date IS NULL THEN 1 END) as tt_late_shipments
                FROM idrialug9071_staging.orders 
                    JOIN idrialug9071_staging.shipment_deliveries
                    ON orders.order_id = shipment_deliveries.order_id
                GROUP BY 
                  orders.product_id
              )

            SELECT 
              now(),
              dim_products.product_name, 
              orders_cte.order_date as most_ordered_day,
              CASE 
                WHEN day_of_the_week_num <= 5 AND working_day = False THEN TRUE ELSE FALSE 
                END AS is_public_holiday,
              reviews_cte.avg_review_points,
              pct_one_star_review,
              pct_two_star_review,
              pct_three_star_review,
              pct_four_star_review,
              pct_five_star_review,
              (shipments_cte.tt_early_shipments / (shipments_cte.tt_late_shipments + shipments_cte.tt_early_shipments)) AS pct_early_shipments,
              (shipments_cte.tt_late_shipments / (shipments_cte.tt_late_shipments + shipments_cte.tt_early_shipments)) AS pct_late_shipments 
            FROM 
              if_common.dim_products 
            JOIN 
              reviews_cte ON dim_products .product_id = reviews_cte.product_id 
            JOIN 
              orders_cte ON dim_products.product_id = orders_cte.product_id 
            JOIN 
              shipments_cte ON dim_products.product_id = shipments_cte.product_id 
            WHERE 
              reviews_cte.avg_review_points = (
                SELECT 
                  MAX(avg_review_points) 
                FROM 
                  reviews_cte) AND orders_cte.rank = 1

            ORDER BY orders_cte.rank
            LIMIT 1
''')