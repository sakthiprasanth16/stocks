import streamlit as st
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
from sqlalchemy import create_engine

# Database Connection
host = "gateway01.eu-central-1.prod.aws.tidbcloud.com"
port = 4000
user = "42aq8sKC2dkkKnC.root"
password = "E0k2yNcKG1v2koeb"
database = "stocks"
ssl_ca_path = "C:/certs/cacert.pem"
ssl_args = f"?ssl_ca={ssl_ca_path}"

def get_tidb_engine():
    conn_str = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}{ssl_args}"
    return create_engine(conn_str)

# Getting Stock Data
@st.cache_data
def load_stock_data():
    engine = get_tidb_engine()
    query = """
        SELECT CAST(date AS DATE) AS date, open, close, high, low, volume, ticker
        FROM all_stocks
    """
    df = pd.read_sql(query, engine)
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['date'] >= "2023-11-22"]   
    return df

# Getting Sector Data
@st.cache_data
def load_sector_data():
    return pd.read_csv("E:/Sakthi/prasanth/projects/stocks/Scripts/updated_sector.csv")

# Main Function
def main():
    st.set_page_config(page_title="Stock Analysis Dashboard", layout="wide")
    st.title("Nifty 50 Stock Performance")

    # Calling the stock and sector data getting function
    df = load_stock_data()
    sector_df = load_sector_data()

    # Calculating Daily Returns
    df['daily_return'] = df.groupby('ticker')['close'].pct_change()
    
    # Market Summary---------------------
    st.header("Market Summary")

    yearly_returns = (
        df.groupby('ticker')['daily_return']
        .apply(lambda x: (1 + x.dropna()).prod() - 1)
        .reset_index(name='yearly_return')
        .merge(sector_df[['symbol', 'company', 'sector']],
               left_on='ticker', right_on='symbol', how='left')
        .drop(columns='symbol')
    )

    green = (yearly_returns['yearly_return'] > 0).sum()
    red = (yearly_returns['yearly_return'] < 0).sum()
    avg_price = df.groupby('ticker')['close'].mean().mean()
    avg_volume = df.groupby('ticker')['volume'].mean().mean()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Green Stocks", green)
    c2.metric("Red Stocks", red)
    c3.metric("Average Price", f"₹{avg_price:.2f}")
    c4.metric("Average Volume", f"{avg_volume:,.0f}")

    # Top 10 gainers & losers---------------------
    top_10_gain = yearly_returns.sort_values('yearly_return', ascending=False).head(10)
    top_10_loss = yearly_returns.sort_values('yearly_return').head(10)

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Top 10 Green Stocks")
        fig_gain = px.bar(
            top_10_gain,
            x='company',
            y='yearly_return',
            color='yearly_return',
            color_continuous_scale='Greens',
            title="Top 10 Green Stocks (Yearly Return)"
        )
        fig_gain.update_yaxes(tickformat=".1%")
        st.plotly_chart(fig_gain, use_container_width=True)
        st.dataframe(top_10_gain[['ticker', 'company', 'yearly_return']])

    with c2:
        st.subheader("Top 10 Loss Stocks")
        fig_loss = px.bar(
            top_10_loss,
            x='company',
            y='yearly_return',
            color='yearly_return',
            color_continuous_scale='Reds',
            title="Top 10 Loss Stocks (Yearly Return)"
        )
        fig_loss.update_yaxes(tickformat=".1%")
        st.plotly_chart(fig_loss, use_container_width=True)
        st.dataframe(top_10_loss[['ticker', 'company', 'yearly_return']])

    st.divider()

    # Volatility Analysis----------------------

    st.header("Volatility Analysis")

    volatility = (
        df.groupby('ticker')['daily_return']
        .std()
        .reset_index(name='volatility')
        .merge(sector_df[['symbol', 'company', 'sector']],
               left_on='ticker', right_on='symbol', how='left')
        .drop(columns='symbol')
    )

    fig_vol = px.bar(
        volatility.sort_values('volatility', ascending=False).head(10), 
        x='company',
        y='volatility',
        color='volatility',
        color_continuous_scale='Viridis',
        title="Top 10 Most Volatile Stocks"
    )
    st.plotly_chart(fig_vol, use_container_width=True)

    st.divider()

    # Cumulative Return for Top 5 Performing Stocks----------------------

    df['cumulative_return'] = df.groupby('ticker')['daily_return'].transform(lambda x: (1 + x.fillna(0)).cumprod() - 1)
    st.header("Cumulative Return for Top 5 Performing Stocks")

    top_tickers = top_10_gain['ticker'].head(5)
    cum_df = df[df['ticker'].isin(top_tickers)]
    pivot_cum = cum_df.pivot(index='date', columns='ticker', values='cumulative_return')
    st.line_chart(pivot_cum)

    st.divider()

    # Sector-wise Performance----------------------------

    st.header("Sector-wise Performance")

    sector_avg = yearly_returns.groupby('sector')['yearly_return'].mean().sort_values(ascending=False)

    fig_sector = px.bar(
        sector_avg,
        x=sector_avg.index,
        y=sector_avg.values,
        title="Average Yearly Return by Sector",
        color=sector_avg.values,
        color_continuous_scale='Bluered'
    )
    fig_sector.update_yaxes(tickformat=".1%")
    st.plotly_chart(fig_sector, use_container_width=True)

    st.divider()

    # Stock Price Correlation--------------------------------

    st.header("Stock Price Correlation")

    price_pivot = df.pivot(index='date', columns='ticker', values='close')
    corr = price_pivot.corr()

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.heatmap(corr, cmap='coolwarm', center=0, ax=ax)
    plt.title("Stock Price Correlation Heatmap")
    st.pyplot(fig)

    st.divider()

    # Top 5 Gainers and Losers (Month-wise)---------------------
    st.header("Top 5 Gainers and Losers (Month-wise)")

    df['MonthYear'] = df['date'].dt.to_period('M')
    monthly_returns = (
        df.groupby(['ticker', 'MonthYear'])['close']
        .agg(lambda x: (x.iloc[-1] - x.iloc[0]) / x.iloc[0])
        .reset_index(name='monthly_return')
        .merge(sector_df[['symbol', 'company']], left_on='ticker', right_on='symbol', how='left')
        .drop(columns='symbol')
    )

    # Month selector
    available_months = sorted(monthly_returns['MonthYear'].unique())
    selected_month = st.selectbox("Select Month", available_months, index=len(available_months)-1)

    month_data = monthly_returns[monthly_returns['MonthYear'] == selected_month]
    top5_month = month_data.sort_values('monthly_return', ascending=False).head(5)
    bottom5_month = month_data.sort_values('monthly_return').head(5)

    c1, c2 = st.columns(2)
    with c1:
        st.subheader(f"Top 5 Gainers ({selected_month})")
        fig_top_month = px.bar(
            top5_month,
            x='company',
            y='monthly_return',
            color='monthly_return',
            color_continuous_scale='Greens'
        )
        fig_top_month.update_yaxes(tickformat=".1%")
        st.plotly_chart(fig_top_month, use_container_width=True)
        st.dataframe(top5_month[['ticker', 'company', 'monthly_return']])

    with c2:
        st.subheader(f"Top 5 Losers ({selected_month})")
        fig_bottom_month = px.bar(
            bottom5_month,
            x='company',
            y='monthly_return',
            color='monthly_return',
            color_continuous_scale='Reds'
        )
        fig_bottom_month.update_yaxes(tickformat=".1%")
        st.plotly_chart(fig_bottom_month, use_container_width=True)
        st.dataframe(bottom5_month[['ticker', 'company', 'monthly_return']])

    st.divider()

    # Sector Treemap-----------------------------

    st.header("Sector Treemap")

    fig_tree = px.treemap(
        sector_df,
        path=['sector', 'company'],
        title="Treemap: Companies by Sector",
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    st.plotly_chart(fig_tree, use_container_width=True)


# Direct run function
if __name__ == "__main__":
    main()
