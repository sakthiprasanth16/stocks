import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go

#database-connection
host = "gateway01.eu-central-1.prod.aws.tidbcloud.com"
port = 4000
user = "42aq8sKC2dkkKnC.root"
password = "YOUR PASSWORD"
database = "stocks"
ssl_ca_path = "C:/certs/cacert.pem"
ssl_args = f"?ssl_ca={ssl_ca_path}"

def get_tidb_engine():
    conn_str = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}{ssl_args}"
    return create_engine(conn_str)

#load-database
@st.cache_data
def load_tidb_data():
    engine = get_tidb_engine()
    query = """
        SELECT
            CAST(date AS DATE) AS date,
            open, close, high, low, volume, ticker
        FROM all_stocks
    """
    df = pd.read_sql(query, engine)
    df['date'] = pd.to_datetime(df['date'])
    return df

#load-sector-data
@st.cache_data
def load_sector_data():
    return pd.read_csv("E:/Sakthi/prasanth/projects/stocks/Scripts/updated_sector.csv")

#data-processing
def process_data(stock_df, sector_df):
    #merge-stocks-and-sector-data
    merged_df = pd.merge(
        stock_df,
        sector_df[['symbol', 'sector', 'company']],
        left_on='ticker',
        right_on='symbol',
        how='left'
    )
    merged_df.drop('symbol', axis=1, inplace=True)
    merged_df = merged_df.sort_values(['ticker', 'date'])
    merged_df['date'] = pd.to_datetime(merged_df['date'])

    #filter-data-from 2023-11-22 to-max
    start_date = pd.Timestamp('2023-11-22')
    end_date = merged_df['date'].max()
    merged_df = merged_df[(merged_df['date'] >= start_date) & (merged_df['date'] <= end_date)]

    #daily-returns
    merged_df['daily_return_decimal'] = merged_df.groupby('ticker')['close'].pct_change()
    merged_df['daily_return_percent'] = merged_df['daily_return_decimal'] * 100

    #volatility-drop-NaN
    merged_df_clean = merged_df.dropna(subset=['daily_return_decimal'])

    #cumulative-returns-fill-NaN-with-0
    merged_df['daily_return_filled'] = merged_df['daily_return_decimal'].fillna(0)
    merged_df['cumulative_return'] = merged_df.groupby('ticker')['daily_return_filled'].transform(
        lambda x: (1 + x).cumprod() - 1
    )

    #yearly-returns
    yearly_returns = merged_df.groupby('ticker').apply(
        lambda x: (1 + x['daily_return_decimal'].dropna()).prod() - 1
    ).reset_index(name='yearly_return')

    yearly_returns['percentage'] = (yearly_returns['yearly_return'] * 100).round(2)

    #add-company-name-by-merging-with-sector_df
    yearly_returns = yearly_returns.merge(
        sector_df[['symbol', 'company', 'sector']],
        left_on='ticker',
        right_on='symbol',
        how='left'
    ).drop('symbol', axis=1)

    #volatility
    volatility = merged_df_clean.groupby('ticker')['daily_return_decimal'].std().reset_index(name='volatility')

    return merged_df, yearly_returns, volatility

#monthly-gainers-and-losers
def get_monthly_top_gainers_losers(merged_df):
    #add-month-column
    merged_df['date_dt'] = pd.to_datetime(merged_df['date'])
    merged_df['month'] = merged_df['date_dt'].dt.to_period('M')

    #get-last-close-of-prev-month-and-this-month-for-each-ticker
    monthly = merged_df.groupby(['ticker', 'company', 'month']).agg({'close': ['first', 'last']}).reset_index()
    monthly.columns = ['ticker', 'company', 'month', 'first_close', 'last_close']
    monthly['monthly_return'] = (monthly['last_close'] - monthly['first_close']) / monthly['first_close']
    monthly['monthly_return_percent'] = monthly['monthly_return'] * 100
    return monthly

#main
def main():
    st.set_page_config(page_title="Stock Analytics", layout="wide")
    st.title("Stock Market Analysis Dashboard")

    #load-data
    stock_df = load_tidb_data()
    sector_df = load_sector_data()

    #call-process-data
    merged_df, yearly_returns, volatility = process_data(stock_df, sector_df)

    #market-summary
    green_count = (yearly_returns['yearly_return'] > 0).sum()
    red_count = (yearly_returns['yearly_return'] <= 0).sum()
    avg_price = merged_df.groupby('ticker')['close'].mean().mean()
    avg_volume = merged_df.groupby('ticker')['volume'].mean().mean()

    #view
    st.header("Market Overview")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Green Stocks", green_count)
    col2.metric("Red Stocks", red_count)
    col3.metric("Average Price", f"₹{avg_price:.2f}")
    col4.metric("Average Volume", f"{avg_volume:,.0f}")

    st.divider()

    #top-10-green-and-red
    top_10_green = yearly_returns.sort_values('yearly_return', ascending=False).head(10).copy()
    top_10_loss = yearly_returns.sort_values('yearly_return').head(10).copy()
    top_10_vol = volatility.sort_values('volatility', ascending=False).head(10)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 10 Performing Stocks")
        df1 = top_10_green.set_index('ticker')[['company', 'yearly_return', 'percentage']]
        df1.index.name = 'Ticker'
        df1 = df1.rename(columns={
            'company': 'Company',
            'yearly_return': 'Yearly Return',
            'percentage': 'Percentage'
        })
        st.dataframe(df1, use_container_width=True)

    with col2:
        st.subheader("Top 10 Losing Stocks")
        df2 = top_10_loss.set_index('ticker')[['company', 'yearly_return', 'percentage']]
        df2.index.name = 'Ticker'
        df2 = df2.rename(columns={
            'company': 'Company',
            'yearly_return': 'Yearly Return',
            'percentage': 'Percentage'
        })
        st.dataframe(df2, use_container_width=True)

    st.divider()

    #top-10-most-volatile-stocks
    st.subheader("Top 10 Most Volatile Stocks")
    color1 = "#f7d203" #color-code
    fig = px.bar(
        top_10_vol,
        x='ticker',
        y='volatility',
        labels={'ticker': 'Ticker', 'volatility': 'Volatility'},
        title="Volatile Stocks",
        color_discrete_sequence=[color1]
    )
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    #cumulative-returns-for-top-5
    top_5_tickers = yearly_returns.nlargest(5, 'yearly_return')['ticker']
    cumulative_df = merged_df[merged_df['ticker'].isin(top_5_tickers)]
    
    st.subheader("Cumulative Returns - Top 5 Performers")
    cumulative_pivot = cumulative_df.pivot_table(
        index='date',
        columns='ticker',
        values='cumulative_return'
    )
    st.line_chart(cumulative_pivot)

    st.divider()

    #sector-wise-performance
    st.header("Sector-wise Performance")
    sector_perf = yearly_returns.groupby('sector')['yearly_return'].mean().sort_values(ascending=False)
    sector_df = sector_perf.reset_index()
    sector_df.columns = ['Sector', 'Average Yearly Return']

    color4 = "#7e06b2"
    fig = px.bar(
        sector_df,
        x='Sector',
        y='Average Yearly Return',
        color_discrete_sequence=[color4],
        title="Sector-wise Performance"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    #stock-price-correlation-heatmap
    st.header("Stock Price Correlation Heatmap")
    price_pivot = merged_df.pivot(index='date', columns='ticker', values='close')
    corr_matrix = price_pivot.corr()
    fig, ax = plt.subplots(figsize=(12, 8))
    sns.heatmap(corr_matrix, cmap='coolwarm', center=0, ax=ax)
    plt.title("Stock Price Correlation Heatmap")
    st.pyplot(fig)

    st.divider()

    #monthly-top-5-gainers-and-losers
    st.header("Monthly Top 5 Gainers and Losers in %")
    monthly = get_monthly_top_gainers_losers(merged_df)
    monthly['month_str'] = monthly['month'].astype(str)
    months = monthly['month_str'].unique()
    months = sorted(months)

    #month-selection
    selected_month = st.selectbox("Select Month", months, index=len(months)-1)
    selected_month_data = monthly[monthly['month_str'] == selected_month]

    #top-5-gainers-and-losers-for-selected-month
    top5_gainers = selected_month_data.sort_values('monthly_return', ascending=False).head(5)
    top5_losers = selected_month_data.sort_values('monthly_return').head(5)

    color2 = "#04e072"
    color3 = "#e01b04"
    col1, col2 = st.columns(2)
    with col1:
        st.subheader(f"Top 5 Gainers in {selected_month}")
        fig_gainers = px.bar(
            top5_gainers,
            x='company',
            y='monthly_return_percent',
            labels={'company': 'Company', 'monthly_return_percent': 'Monthly Return %'},
            color_discrete_sequence=[color2],  
            title=f"Top 5 Gainers in {selected_month}"
        )
        st.plotly_chart(fig_gainers, use_container_width=True)
        #dataframe
        st.dataframe(
            top5_gainers[['ticker', 'company', 'monthly_return_percent']]
            .rename(columns={
                'ticker': 'Ticker',
                'company': 'Company',
                'monthly_return_percent': 'Monthly Return %'
            })
            .set_index('Ticker'),
            use_container_width=True
        )

    with col2:
        st.subheader(f"Top 5 Losers in {selected_month}")
        fig_losers = px.bar(
            top5_losers,
            x='company',
            y='monthly_return_percent',
            labels={'company': 'Company', 'monthly_return_percent': 'Monthly Return %'},
            color_discrete_sequence=[color3],  # Custom color here
            title=f"Top 5 Losers in {selected_month}"
        )
        st.plotly_chart(fig_losers, use_container_width=True)
        #dataframe
        st.dataframe(
            top5_losers[['ticker', 'company', 'monthly_return_percent']]
            .rename(columns={
                'ticker': 'Ticker',
                'company': 'Company',
                'monthly_return_percent': 'Monthly Return %'
            })
            .set_index('Ticker'),
            use_container_width=True
        )
    st.divider()

    #tree-map-based-on-volume
    treemap_df = (
        merged_df.groupby(['ticker', 'company', 'sector'], as_index=False)
        .agg(avg_volume=('volume', 'mean'))
    )

    st.header("Average Volume TreeMap")
    fig = px.treemap(
        treemap_df,
        path=['sector', 'ticker'],
        values='avg_volume',
        color='sector',
        hover_data={'company': True, 'avg_volume': ':.0f'},
        title="All 50 Stocks - Treemap by Average Volume and Sector"
    )
    fig.update_traces(root_color="lightgrey")
    fig.update_layout(margin=dict(t=50, l=25, r=25, b=25))
    st.plotly_chart(fig, use_container_width=True)

    st.divider()


    #data-set
    all_sectors = sorted(merged_df['sector'].dropna().unique())
    default_sector = [all_sectors[0]] if all_sectors else []
    selected_sectors = st.multiselect(
        "Select Sector(s)", options=all_sectors, default=default_sector
    )

    filtered_companies = merged_df[merged_df['sector'].isin(selected_sectors)]['company'].dropna().unique()
    all_companies = sorted(filtered_companies)
    default_company = [all_companies[0]] if all_companies else []
    selected_companies = st.multiselect(
        "Select Company(ies)", options=all_companies, default=default_company
    )

    if not selected_sectors:
        st.warning("Please select at least one sector.")
    elif not selected_companies:
        st.warning("Please select at least one company.")
    else:
        raw_filtered = merged_df[
            merged_df['sector'].isin(selected_sectors) &
            merged_df['company'].isin(selected_companies)
        ][['date', 'open', 'close', 'high', 'low', 'volume', 'sector', 'company']]

        #for-only-date
        raw_filtered['date'] = raw_filtered['date'].dt.date
        st.subheader("Data for Selected Sector(s) and Company(ies)")
        st.dataframe(raw_filtered, use_container_width=True)


if __name__ == "__main__":
    main()
