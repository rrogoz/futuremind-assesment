import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
PROJECT_PATH = os.getenv('PROJECT_PATH')

# Page config
st.set_page_config(
    page_title="Movie Revenue Dashboard",
    page_icon="🎬",
    layout="wide"
)

# Custom CSS
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
    }
    </style>
    """, unsafe_allow_html=True)

# Title
st.title("🎬 Movie Revenue Analytics Dashboard")
st.markdown("---")

# Load data
@st.cache_data
def load_data():
    """Load and join all tables from Gold layer"""
    
    # Load fact and dimensions
    fact_path = os.path.join(PROJECT_PATH, "data", "03_gold", "factRevenues")
    dim_movie_path = os.path.join(PROJECT_PATH, "data", "03_gold", "dimMovies")
    dim_dist_path = os.path.join(PROJECT_PATH, "data", "03_gold", "dimDistributor")
    
    fact = pd.read_parquet(fact_path, engine='fastparquet')
    dim_movies = pd.read_parquet(dim_movie_path, engine='fastparquet')
    dim_distributor = pd.read_parquet(dim_dist_path, engine='fastparquet')
    
    # Join fact with dimensions
    df = (fact
          .merge(dim_movies, on='_sk_movie', how='left')
          .merge(dim_distributor, on='_sk_distributor', how='left'))
    
    # Convert date to datetime
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['month_name'] = df['date'].dt.month_name()
    df['day_of_week'] = df['date'].dt.day_name()
    
    # Convert numeric fields
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
    df['theaters'] = pd.to_numeric(df['theaters'], errors='coerce')
    df['imdb_rating'] = pd.to_numeric(df['imdb_rating'], errors='coerce')
    
    return df, fact, dim_movies, dim_distributor

try:
    df, fact, dim_movies, dim_distributor = load_data()
    
    # Sidebar Filters
    st.sidebar.header("🔍 Filters")
    
    # Enrichment filter
    show_enriched_only = st.sidebar.checkbox("Show only enriched movies (with OMDB data)", value=False)
    if show_enriched_only:
        df = df[df['is_enriched'] == 1]
    
    # Date range filter
    min_date = df['date'].min().date()
    max_date = df['date'].max().date()
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    if len(date_range) == 2:
        df = df[(df['date'].dt.date >= date_range[0]) & (df['date'].dt.date <= date_range[1])]
    
    # Distributor filter
    distributors = ['All'] + sorted(df['distributor'].dropna().unique().tolist())
    selected_distributor = st.sidebar.selectbox("Distributor", distributors)
    if selected_distributor != 'All':
        df = df[df['distributor'] == selected_distributor]
    
    # Genre filter (if enriched)
    if 'genre' in df.columns:
        # Split genres (they might be comma-separated)
        all_genres = set()
        for genre_str in df['genre'].dropna():
            all_genres.update([g.strip() for g in str(genre_str).split(',')])
        genres = ['All'] + sorted(list(all_genres))
        selected_genre = st.sidebar.selectbox("Genre", genres)
        if selected_genre != 'All':
            df = df[df['genre'].str.contains(selected_genre, na=False, case=False)]
    
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**Records:** {len(df):,}")
    
    # === KPI METRICS ===
    st.subheader("📊 Key Metrics")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_revenue = df['revenue'].sum()
        st.metric("Total Revenue", f"${total_revenue/1e6:.1f}M")
    
    with col2:
        unique_movies = df['_sk_movie'].nunique()
        st.metric("Unique Movies", f"{unique_movies:,}")
    
    with col3:
        avg_theaters = df['theaters'].mean()
        st.metric("Avg Theaters", f"{avg_theaters:,.0f}")
    
    with col4:
        avg_rating = df['imdb_rating'].mean()
        st.metric("Avg IMDB Rating", f"{avg_rating:.1f}" if pd.notna(avg_rating) else "N/A")
    
    with col5:
        enrichment_rate = (df['is_enriched'].sum() / len(df) * 100) if len(df) > 0 else 0
        st.metric("Enrichment Rate", f"{enrichment_rate:.0f}%")
    
    st.markdown("---")
    
    # === TABS FOR DIFFERENT VIEWS ===
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 Revenue Trends", 
        "🎭 Genre Analysis", 
        "🏢 Distributor Analysis",
        "⭐ Top Performers",
        "📋 Data Quality"
    ])
    
    # TAB 1: Revenue Trends
    with tab1:
        st.subheader("Revenue Over Time")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Daily revenue trend
            daily_revenue = df.groupby('date')['revenue'].sum().reset_index()
            fig_daily = px.line(
                daily_revenue, 
                x='date', 
                y='revenue',
                title="Daily Revenue Trend"
            )
            fig_daily.update_layout(yaxis_title="Revenue ($)", xaxis_title="Date")
            st.plotly_chart(fig_daily, use_container_width=True)
        
        with col2:
            # Monthly revenue
            monthly_revenue = df.groupby(['year', 'month_name'])['revenue'].sum().reset_index()
            monthly_revenue['period'] = monthly_revenue['month_name'] + ' ' + monthly_revenue['year'].astype(str)
            fig_monthly = px.bar(
                monthly_revenue,
                x='period',
                y='revenue',
                title="Monthly Revenue"
            )
            fig_monthly.update_layout(yaxis_title="Revenue ($)", xaxis_title="Month")
            st.plotly_chart(fig_monthly, use_container_width=True)
        
        # Revenue by day of week
        dow_revenue = df.groupby('day_of_week')['revenue'].sum().reindex([
            'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'
        ]).reset_index()
        
        fig_dow = px.bar(
            dow_revenue,
            x='day_of_week',
            y='revenue',
            title="Revenue by Day of Week"
        )
        st.plotly_chart(fig_dow, use_container_width=True)
    
    # TAB 2: Genre Analysis
    with tab2:
        st.subheader("Genre Performance")
        
        if 'genre' in df.columns and df['is_enriched'].sum() > 0:
            enriched_df = df[df['is_enriched'] == 1].copy()
            
            # Split genres and aggregate
            genre_data = []
            for _, row in enriched_df.iterrows():
                if pd.notna(row['genre']):
                    genres = [g.strip() for g in str(row['genre']).split(',')]
                    for genre in genres:
                        genre_data.append({
                            'genre': genre,
                            'revenue': row['revenue'],
                            'theaters': row['theaters'],
                            'imdb_rating': row['imdb_rating']
                        })
            
            genre_df = pd.DataFrame(genre_data)
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Revenue by genre
                genre_revenue = genre_df.groupby('genre')['revenue'].sum().sort_values(ascending=False).head(10)
                fig_genre_rev = px.bar(
                    x=genre_revenue.index,
                    y=genre_revenue.values,
                    title="Top 10 Genres by Revenue",
                    labels={'x': 'Genre', 'y': 'Revenue ($)'}
                )
                st.plotly_chart(fig_genre_rev, use_container_width=True)
            
            with col2:
                # Average rating by genre
                genre_rating = genre_df.groupby('genre')['imdb_rating'].mean().sort_values(ascending=False).head(10)
                fig_genre_rating = px.bar(
                    x=genre_rating.index,
                    y=genre_rating.values,
                    title="Top 10 Genres by Avg IMDB Rating",
                    labels={'x': 'Genre', 'y': 'Avg Rating'}
                )
                st.plotly_chart(fig_genre_rating, use_container_width=True)
            
            # Genre distribution (pie chart)
            genre_count = genre_df['genre'].value_counts().head(8)
            fig_genre_pie = px.pie(
                values=genre_count.values,
                names=genre_count.index,
                title="Genre Distribution (Top 8)"
            )
            st.plotly_chart(fig_genre_pie, use_container_width=True)
        else:
            st.info("📊 Genre analysis requires enriched data (OMDB). Enable enrichment filter to see insights.")
    
    # TAB 3: Distributor Analysis
    with tab3:
        st.subheader("Distributor Performance")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Revenue by distributor
            dist_revenue = df.groupby('distributor')['revenue'].sum().sort_values(ascending=False).head(10)
            fig_dist = px.bar(
                x=dist_revenue.index,
                y=dist_revenue.values,
                title="Top 10 Distributors by Revenue",
                labels={'x': 'Distributor', 'y': 'Revenue ($)'}
            )
            st.plotly_chart(fig_dist, use_container_width=True)
        
        with col2:
            # Number of movies by distributor
            dist_movies = df.groupby('distributor')['_sk_movie'].nunique().sort_values(ascending=False).head(10)
            fig_dist_movies = px.bar(
                x=dist_movies.index,
                y=dist_movies.values,
                title="Top 10 Distributors by # of Movies",
                labels={'x': 'Distributor', 'y': '# of Movies'}
            )
            st.plotly_chart(fig_dist_movies, use_container_width=True)
        
        # Distributor market share
        dist_market = df.groupby('distributor')['revenue'].sum().sort_values(ascending=False).head(8)
        fig_dist_pie = px.pie(
            values=dist_market.values,
            names=dist_market.index,
            title="Distributor Market Share (Top 8)"
        )
        st.plotly_chart(fig_dist_pie, use_container_width=True)
    
    # TAB 4: Top Performers
    with tab4:
        st.subheader("🏆 Top Performing Movies")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### By Total Revenue")
            top_revenue = (df.groupby(['title', '_sk_movie'])
                          .agg({
                              'revenue': 'sum',
                              'theaters': 'mean',
                              'imdb_rating': 'first',
                              'distributor': 'first'
                          })
                          .sort_values('revenue', ascending=False)
                          .head(10)
                          .reset_index())
            
            top_revenue['revenue'] = top_revenue['revenue'].apply(lambda x: f"${x/1e6:.2f}M")
            top_revenue['theaters'] = top_revenue['theaters'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A")
            
            st.dataframe(
                top_revenue[['title', 'revenue', 'theaters', 'imdb_rating', 'distributor']],
                use_container_width=True,
                hide_index=True
            )
        
        with col2:
            st.markdown("#### By IMDB Rating")
            if df['is_enriched'].sum() > 0:
                top_rated = (df[df['is_enriched'] == 1]
                            .groupby(['title', '_sk_movie'])
                            .agg({
                                'imdb_rating': 'first',
                                'revenue': 'sum',
                                'genre': 'first',
                                'year': 'first'  # year from dimMovies
                            })
                            .sort_values('imdb_rating', ascending=False)
                            .head(10)
                            .reset_index())
                
                top_rated['revenue'] = top_rated['revenue'].apply(lambda x: f"${x/1e6:.2f}M")
                
                st.dataframe(
                    top_rated[['title', 'imdb_rating', 'revenue', 'genre', 'year']].rename(columns={'year': 'year'}),
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("Enable enriched data to see ratings")
        
            movie_stats = (df[df['is_enriched'] == 1]
                .groupby('title')
                .agg({
                    'revenue': 'sum',
                    'imdb_rating': 'first',
                    'theaters': 'mean'
                })
                .reset_index()
                .dropna())  # Add this to remove any rows with NaN

            fig_scatter = px.scatter(
                movie_stats,
                x='imdb_rating',
                y='revenue',
                size='theaters',
                hover_data=['title'],
                title="Revenue vs IMDB Rating (bubble size = avg theaters)"
            )
            fig_scatter.update_layout(xaxis_title="IMDB Rating", yaxis_title="Total Revenue ($)")
            st.plotly_chart(fig_scatter, use_container_width=True)
    
    # TAB 5: Data Quality
    with tab5:
        st.subheader("📋 Data Quality Metrics")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Records", f"{len(fact):,}")
            st.metric("Movies in Dimension", f"{len(dim_movies):,}")
            st.metric("Distributors in Dimension", f"{len(dim_distributor):,}")
        
        with col2:
            enriched_count = df['is_enriched'].sum()
            enrichment_pct = (enriched_count / len(df) * 100) if len(df) > 0 else 0
            st.metric("Enriched Records", f"{enriched_count:,}")
            st.metric("Enrichment Rate", f"{enrichment_pct:.1f}%")
            
            missing_revenue = df['revenue'].isna().sum()
            st.metric("Missing Revenue", f"{missing_revenue:,}")
        
        with col3:
            missing_theaters = df['theaters'].isna().sum()
            st.metric("Missing Theaters", f"{missing_theaters:,}")
            
            missing_distributor = df['distributor'].isna().sum()
            st.metric("Missing Distributor", f"{missing_distributor:,}")
        
        # Enrichment over time
        st.markdown("#### OMDB Enrichment Coverage")
        enrichment_pie = df.groupby('is_enriched').size().reset_index()
        enrichment_pie.columns = ['is_enriched', 'count']

        fig_enrich = px.pie(
            enrichment_pie,
            values='count',
            names='is_enriched',
            title="OMDB Enrichment Status"
        )
        fig_enrich.update_traces(labels=['Not Enriched', 'Enriched'])
        st.plotly_chart(fig_enrich, use_container_width=True)
        
        # Sample of data
        st.markdown("#### Sample Data")
        sample_df = df[['date', 'title', 'revenue', 'theaters', 'distributor', 'imdb_rating', 'is_enriched']].head(20)
        st.dataframe(sample_df, use_container_width=True, hide_index=True)

except FileNotFoundError as e:
    st.error(f"❌ Data files not found: {e}")
    st.info("Please ensure the Gold layer data is generated and paths are correct.")
except Exception as e:
    st.error(f"❌ Error loading data: {e}")
    st.exception(e)

# Footer
st.markdown("---")
st.markdown("**Movie Revenue Analytics Dashboard** | Data Engineering Assessment | Built with Streamlit")


