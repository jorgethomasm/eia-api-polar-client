import duckdb
import plotly.express as px


# Example using DuckDB and Polars for quick data analysis
if __name__ == "__main__":
    
    # ------------ Get some columns ------------
    con = duckdb.connect("./data/raw/eia_sdge_2024_demo.duckdb")
    df = con.execute("""
                     SELECT period, "subba-name" AS "BalancingAuthority", value AS MWh 
                     FROM eia_data
                     """).pl()
    con.close()
    print(df)

    # ------------ Plot ------------
    fig_ts = px.line(df, x='period', y='MWh', title=f'Hourly Energy Demand from {df["BalancingAuthority"][0]} during {df["period"][0].year}')   
    fig_ts.show()


    # ------------ Monthy energy demand ------------
    con = duckdb.connect("./data/raw/eia_sdge_2024_demo.duckdb")    
    df_monthly = con.execute("""
                             SELECT MONTH(period) AS Month, ANY_VALUE("subba-name") AS "BalancingAuthority", SUM(value) AS MWh 
                             FROM eia_data 
                             GROUP BY Month 
                             ORDER BY Month
                """).pl()
    con.close()
    print(df_monthly)

    # ------------ Plot ------------
    fig_bar = px.bar(df_monthly, x='Month', y='MWh', 
                 title=f'Monthly Energy Demand from {df_monthly["BalancingAuthority"][0]} during {df["period"][0].year}', 
                 opacity=0.6, 
                 text_auto=True)
    fig_bar.update_xaxes(
    tickmode='array',
    tickvals=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    ticktext=["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    )
    fig_bar.update_yaxes(title_text='MWh', title_standoff=25, automargin=True)
    fig_bar.update_traces(textposition='outside')
    fig_bar.show()





