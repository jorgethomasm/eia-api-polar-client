import os
import sys
import datetime
import  duckdb
import plotly.express as px
# Add the src directory to the PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

# Example usage
if __name__ == "__main__":
    con = duckdb.connect("./data/raw/eia_sdge_2025_demo.duckdb")
    
    df = con.execute("""
    SELECT period, subba-name, value FROM eia_data
                """).pl()
    con.close()

    print(df)






