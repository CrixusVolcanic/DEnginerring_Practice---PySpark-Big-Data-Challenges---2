transformatted_data = """
                create or replace temp view transformatted_data as
                select * 
                    ,concat_ws('/'
                            ,lpad(split_part(order_date, '/', 1), 2, '0')
                            ,lpad(split_part(order_date, '/', 2), 2, '0')
                            ,lpad(split_part(order_date, '/', 3), 4, '0')) order_date_transformatted
                    ,concat_ws('/'
                            ,lpad(split_part(ship_date, '/', 1), 2, '0')
                            ,lpad(split_part(ship_date, '/', 2), 2, '0')
                            ,lpad(split_part(ship_date, '/', 3), 4, '0')) ship_date_transformatted
                from raw_data"""

casted_data = """
                create or replace temp view casted_data as 
                select selected_columns
                    ,to_date(order_date_transformatted, 'MM/dd/yyyy') order_date
                    ,to_date(ship_date_transformatted, 'MM/dd/yyyy') ship_date
                    ,cast(row_id as INT) row_id
                    ,cast(sales as float) sales
                    ,cast(quantity as float) quantity
                    ,cast(discount as float) discount
                    ,cast(profit as float) profit
                    ,year(to_date(order_date_transformatted, 'MM/dd/yyyy')) year
                    ,(cast(sales as float) - Quantity * 10) / CAST(sales as float) profit_margin
                from transformatted_data"""

agg_data = """
                create or replace temp view agg_data as
                select year
                    ,region
                    ,category
                    ,avg(sales) avg_sales
                    ,sum(sales) total_sales
                    ,avg(profit_margin) avg_profit_margin
                from casted_data
                group by 1,2,3
                order by 1, 5"""

top_region_sales = """
                with w as (select year
                                ,region
                                ,sum(total_sales) total_sales
                                ,rank() over(partition by year order by sum(total_sales) desc) rk
                            from agg_data
                            group by 1,2
                            order by 1,3)
                select year
                    ,region
                    ,total_sales
                from w
                where rk = 1
                """

best_margin_category = """
                select category
                    ,avg(avg_profit_margin) avg_profit_margin
                from agg_data
                group by 1
                order by 2 desc
                limit 1
                """