
import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import os
import plotly.express as px
from download_data import Data
from plot_setup import finastra_theme
from live_news import fetch_esg_news
import metadata_parser

st.set_page_config(page_title="ESG AI", page_icon="📊", layout='centered', initial_sidebar_state="collapsed")

@st.cache_data
def load_all_data():
    df = pd.read_csv("Data/all_data.csv", parse_dates=["DATE"])
    companies = df.Organization.sort_values().unique().tolist()
    companies.insert(0, "Select a Company")
    return df, companies

def get_clickable_name(url):
    try:
        T = metadata_parser.MetadataParser(url=url, search_head_only=True)
        title = T.metadata["og"]["title"].replace("|", " - ")
        return f"[{title}]({url})"
    except:
        return f"[{url}]({url})"

def main():
    alt.themes.register("finastra", finastra_theme)
    alt.themes.enable("finastra")
    violet, fuchsia = ["#694ED6", "#C137A2"]

    icon_path = os.path.join(".", "raw", "esg_ai_logo.png")
    _, logo, _ = st.columns(3)
    logo.image(icon_path, width=200)
    st.markdown("<h1 style='text-align:center; font-family: arial black; font-size: 400%'>ESG<sup>AI</sup></h1><br><br>", unsafe_allow_html=True)

    with st.spinner("Loading ESG dataset..."):
        df, companies = load_all_data()

    company = st.selectbox("Select a Company to Analyze", companies)
    if company and company != "Select a Company":
        st.success(f"You selected: {company}")
        df_company = df[df.Organization == company]

        st.subheader("📄 ESG Articles")
        st.dataframe(df_company[["DATE", "SourceCommonName", "URL"]].sort_values("DATE", ascending=False).head())

        # 🔴 Live ESG News
        st.subheader("🔴 Live ESG News")
        try:
            live_articles = fetch_esg_news(company)
            if live_articles:
                for article in live_articles:
                    st.markdown(f"**{article['title']}**")
                    st.markdown(f"*Published: {article['published']}*")
                    st.markdown(f"[Read more]({article['url']})")
                    st.markdown("---")
            else:
                st.info("No ESG-related news found at the moment.")
        except Exception as e:
            st.error(f"Failed to fetch live news: {e}")

        # 📈 ESG Scores Over Time
        esg_cols = ["E_score", "S_score", "G_score"]
        score_cols = [col for col in esg_cols if col in df_company.columns]
        if score_cols:
            st.subheader("📊 ESG Score Trends")
            score_df = df_company[["DATE"] + score_cols].dropna()
            score_df = score_df.melt("DATE", var_name="Category", value_name="Score")
            chart = alt.Chart(score_df).mark_line().encode(
                x="yearmonthdate(DATE):T",
                y="Score:Q",
                color="Category:N"
            ).properties(height=400)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No ESG scores available for this company.")

if __name__ == "__main__":
    main()
