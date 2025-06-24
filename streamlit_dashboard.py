# Real-time Transaction Dashboard with Streamlit
# Run with: streamlit run dashboard.py
# Requirements: pip install streamlit plotly pandas psycopg2-binary requests

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import psycopg2
import requests
import time
from datetime import datetime, timedelta
import json

# Page configuration
st.set_page_config(
    page_title="Transaction Analytics Dashboard",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database Configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'transaction_db',
    'user': 'postgres',
    'password': 'password',
    'port': '5432'
}

# API Configuration
API_BASE_URL = "http://localhost:5000/api"

class DashboardData:
    """Handle data fetching and processing"""
    
    def __init__(self):
        self.connection = None
        
    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.connection = psycopg2.connect(**DB_CONFIG)
            return True
        except Exception as e:
            st.error(f"Database connection failed: {e}")
            return False
            
    def get_24h_revenue(self):
        """Get total revenue in last 24 hours"""
        try:
            response = requests.get(f"{API_BASE_URL}/revenue/24h", timeout=5)
            if response.status_code == 200:
                return response.json()
        except:
            pass
        
        # Fallback to direct DB query
        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT COALESCE(SUM(amount_usd), 0) as total_revenue
                FROM processed_transactions
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
            """)
            result = cursor.fetchone()
            cursor.close()
            return {'total_revenue_usd': float(result[0])}
        return {'total_revenue_usd': 0}
        
    def get_revenue_by_country(self):
        """Get revenue breakdown by country"""
        try:
            response = requests.get(f"{API_BASE_URL}/revenue/by_country", timeout=5)
            if response.status_code == 200:
                return response.json()
        except:
            pass
            
        # Fallback to direct DB query
        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT country, COALESCE(SUM(amount_usd), 0) as revenue
                FROM processed_transactions
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
                GROUP BY country
                ORDER BY revenue DESC
            """)
            results = cursor.fetchall()
            cursor.close()
            return [{'country': row[0], 'revenue_usd': float(row[1])} for row in results]
        return []
        
    def get_revenue_by_currency(self):
        """Get revenue breakdown by currency"""
        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT currency, 
                       COALESCE(SUM(amount_usd), 0) as revenue_usd,
                       COUNT(*) as transaction_count
                FROM processed_transactions
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
                GROUP BY currency
                ORDER BY revenue_usd DESC
            """)
            results = cursor.fetchall()
            cursor.close()
            return [{'currency': row[0], 'revenue_usd': float(row[1]), 'count': row[2]} for row in results]
        return []
        
    def get_revenue_by_user(self):
        """Get top users by revenue"""
        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT user_id, 
                       COALESCE(SUM(amount_usd), 0) as revenue_usd,
                       COUNT(*) as transaction_count
                FROM processed_transactions
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
                GROUP BY user_id
                ORDER BY revenue_usd DESC
                LIMIT 10
            """)
            results = cursor.fetchall()
            cursor.close()
            return [{'user_id': row[0], 'revenue_usd': float(row[1]), 'count': row[2]} for row in results]
        return []
        
    def get_fx_rate_trends(self):
        """Get FX rate trends"""
        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT currency, rate_to_usd, timestamp
                FROM fx_rates
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
                ORDER BY currency, timestamp
            """)
            results = cursor.fetchall()
            cursor.close()
            return [{'currency': row[0], 'rate': float(row[1]), 'timestamp': row[2]} for row in results]
        return []
        
    def get_hourly_activity(self):
        """Get hourly transaction activity"""
        try:
            response = requests.get(f"{API_BASE_URL}/transactions/hourly", timeout=5)
            if response.status_code == 200:
                return response.json()
        except:
            pass
            
        # Fallback to direct DB query
        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT 
                    DATE_TRUNC('hour', timestamp) as hour,
                    COUNT(*) as transaction_count,
                    COALESCE(SUM(amount_usd), 0) as revenue_usd
                FROM processed_transactions
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
                GROUP BY hour
                ORDER BY hour
            """)
            results = cursor.fetchall()
            cursor.close()
            return [{'hour': row[0].isoformat(), 'transaction_count': row[1], 'revenue_usd': float(row[2])} for row in results]
        return []
        
    def get_real_time_stats(self):
        """Get real-time statistics"""
        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_transactions,
                    COALESCE(SUM(amount_usd), 0) as total_revenue,
                    COALESCE(AVG(amount_usd), 0) as avg_transaction,
                    COUNT(DISTINCT user_id) as unique_users,
                    COUNT(DISTINCT country) as unique_countries
                FROM processed_transactions
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
            """)
            result = cursor.fetchone()
            cursor.close()
            return {
                'total_transactions': result[0],
                'total_revenue': float(result[1]),
                'avg_transaction': float(result[2]),
                'unique_users': result[3],
                'unique_countries': result[4]
            }
        return {}

# Initialize dashboard data
@st.cache_resource
def init_dashboard():
    dashboard = DashboardData()
    dashboard.connect_db()
    return dashboard

# Main Dashboard
def main():
    st.title("💰 Real-time Transaction Analytics Dashboard")
    st.markdown("---")
    
    # Initialize dashboard
    dashboard = init_dashboard()
    
    # Sidebar controls
    st.sidebar.header("Dashboard Controls")
    auto_refresh = st.sidebar.checkbox("Auto Refresh (30s)", value=True)
    refresh_button = st.sidebar.button("Refresh Now")
    
    if auto_refresh:
        time.sleep(30)
        st.rerun()
    
    if refresh_button:
        st.rerun()
    
    # Real-time stats
    st.header("📊 Real-time Statistics")
    stats = dashboard.get_real_time_stats()
    
    if stats:
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                label="Total Transactions (24h)",
                value=f"{stats['total_transactions']:,}",
                delta=None
            )
            
        with col2:
            st.metric(
                label="Total Revenue USD (24h)",
                value=f"${stats['total_revenue']:,.2f}",
                delta=None
            )
            
        with col3:
            st.metric(
                label="Average Transaction",
                value=f"${stats['avg_transaction']:,.2f}",
                delta=None
            )
            
        with col4:
            st.metric(
                label="Unique Users",
                value=f"{stats['unique_users']:,}",
                delta=None
            )
            
        with col5:
            st.metric(
                label="Countries",
                value=f"{stats['unique_countries']:,}",
                delta=None
            )
    
    st.markdown("---")
    
    # Main content area
    col1, col2 = st.columns(2)
    
    with col1:
        # Revenue by Country
        st.subheader("🌍 Revenue by Country")
        country_data = dashboard.get_revenue_by_country()
        if country_data:
            df_country = pd.DataFrame(country_data)
            fig_country = px.pie(
                df_country, 
                values='revenue_usd', 
                names='country',
                title="Revenue Distribution by Country"
            )
            fig_country.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_country, use_container_width=True)
        else:
            st.info("No country data available")
    
    with col2:
        # Revenue by Currency
        st.subheader("💱 Revenue by Currency")
        currency_data = dashboard.get_revenue_by_currency()
        if currency_data:
            df_currency = pd.DataFrame(currency_data)
            fig_currency = px.bar(
                df_currency, 
                x='currency', 
                y='revenue_usd',
                title="Revenue by Currency",
                labels={'revenue_usd': 'Revenue (USD)', 'currency': 'Currency'}
            )
            st.plotly_chart(fig_currency, use_container_width=True)
        else:
            st.info("No currency data available")
    
    # Hourly Activity
    st.subheader("⏰ Hourly Sales Activity")
    hourly_data = dashboard.get_hourly_activity()
    if hourly_data:
        df_hourly = pd.DataFrame(hourly_data)
        df_hourly['hour'] = pd.to_datetime(df_hourly['hour'])
        
        fig_hourly = go.Figure()
        fig_hourly.add_trace(go.Scatter(
            x=df_hourly['hour'],
            y=df_hourly['revenue_usd'],
            mode='lines+markers',
            name='Revenue (USD)',
            line=dict(color='blue', width=2),
            yaxis='y'
        ))
        
        fig_hourly.add_trace(go.Scatter(
            x=df_hourly['hour'],
            y=df_hourly['transaction_count'],
            mode='lines+markers',
            name='Transaction Count',
            line=dict(color='red', width=2),
            yaxis='y2'
        ))
        
        fig_hourly.update_layout(
            title="Hourly Sales Activity (Last 24 Hours)",
            xaxis_title="Hour",
            yaxis=dict(title="Revenue (USD)", side="left"),
            yaxis2=dict(title="Transaction Count", side="right", overlaying="y"),
            hovermode='x unified'
        )
        
        st.plotly_chart(fig_hourly, use_container_width=True)
    else:
        st.info("No hourly activity data available")
    
    # FX Rate Trends
    st.subheader("📈 FX Rate Changes and Trends")
    fx_data = dashboard.get_fx_rate_trends()
    if fx_data:
        df_fx = pd.DataFrame(fx_data)
        df_fx['timestamp'] = pd.to_datetime(df_fx['timestamp'])
        
        fig_fx = px.line(
            df_fx, 
            x='timestamp', 
            y='rate', 
            color='currency',
            title="FX Rate Trends (Last 24 Hours)",
            labels={'rate': 'Rate to USD', 'timestamp': 'Time'}
        )
        st.plotly_chart(fig_fx, use_container_width=True)
    else:
        st.info("No FX rate data available")
    
    # Top Users
    st.subheader("👥 Top Users by Revenue")
    user_data = dashboard.get_revenue_by_user()
    if user_data:
        df_users = pd.DataFrame(user_data)
        
        col1, col2 = st.columns(2)
        with col1:
            fig_users = px.bar(
                df_users, 
                x='user_id', 
                y='revenue_usd',
                title="Top Users by Revenue",
                labels={'revenue_usd': 'Revenue (USD)', 'user_id': 'User ID'}
            )
            fig_users.update_xaxes(tickangle=45)
            st.plotly_chart(fig_users, use_container_width=True)
        
        with col2:
            st.subheader("User Details")
            st.dataframe(
                df_users[['user_id', 'revenue_usd', 'count']].rename(columns={
                    'user_id': 'User ID',
                    'revenue_usd': 'Revenue (USD)',
                    'count': 'Transactions'
                }),
                use_container_width=True
            )
    else:
        st.info("No user data available")
    
    # Footer
    st.markdown("---")
    st.markdown(f"**Dashboard Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.markdown("**Data Source:** PostgreSQL + Kafka Stream Processing")

if __name__ == "__main__":
    main()