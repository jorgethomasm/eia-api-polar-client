import duckdb
import polars as pl
import plotly.express as px


# Example using DuckDB and Polars for quick data analysis
if __name__ == "__main__":
    
    # ------------ e.g. 1: Get some columns and plot ------------
    con = duckdb.connect("./data/demo/eia_sdge_2024_demo.duckdb")
    
    df = con.execute("""
                     SELECT period AT TIME ZONE 'UTC' AS period, 
                     "subba-name" AS "BalancingAuthority", 
                     value AS MWh 
                     FROM eia_data
                     """).pl()  
    con.close()  
    print(df)
    # Plot
    fig_ts = px.line(df, x='period', y='MWh', 
                     title=f'Hourly Energy Demand from {df["BalancingAuthority"][0]} during {df["period"][0].year}')   
    fig_ts.show()


    # ------------ e.g. 2: Energy demand by month ------------ 
    # SQL query direct from a Polars DataFrame!
    df_monthly = duckdb.sql("""
                             SELECT MONTH(period) AS Month, 
                             ANY_VALUE("BalancingAuthority") AS BalancingAuthority, 
                             SUM(MWh) AS MWh 
                             FROM df 
                             GROUP BY Month 
                             ORDER BY Month
                            """).pl()
   
    print(df_monthly)
    # Plot
    fig_bar = px.bar(df_monthly, x='Month', y='MWh', 
                     title=f'Monthly Energy Demand from {df_monthly["BalancingAuthority"][0]} during {df["period"][0].year}', 
                     opacity=0.6, 
                     text_auto=True)
    fig_bar.update_xaxes(
        tickmode='array',
        tickvals=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        ticktext=["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        )    
    fig_bar.update_traces(textposition='outside')
    fig_bar.show()


    # ------------ e.g. 3 Weekly energy demand ------------    
    # Display a table direct from a Polars DataFrame but with sql syntax!
    duckdb.sql(
        """
        SELECT DATE_TRUNC('week', period) AS week_start, 
        SUM(MWh) AS MWh
        FROM df
        GROUP BY week_start
        ORDER BY week_start
        """).show()


    # ------------ e.g. 4 Working with parquet files ------------ 
    # Read parquet file and summarase yearly energy demand    
    df_pq = pl.read_parquet("./data/demo/eia_SDGE_2020_2024_demo.parquet")    
    
    df_pq_yearly = duckdb.sql("""
                              SELECT YEAR(period) AS Year, 
                              ANY_VALUE("subba-name") AS BalancingAuthority, 
                              SUM(value) AS MWh 
                              FROM df_pq 
                              GROUP BY Year 
                              ORDER BY Year
                              """).pl()

    print(df_pq_yearly)                       
                             
