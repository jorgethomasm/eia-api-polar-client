import duckdb
import plotly.express as px


# Example using DuckDB and Polars for quick data analysis
if __name__ == "__main__":
    
    # ------------ e.g. 1: Get some columns and plot ------------
    con = duckdb.connect("./data/raw/eia_sdge_2024_demo.duckdb")
    
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
