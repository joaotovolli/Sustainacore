
import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import os
import time
import plotly.express as px
import sys
from download_data import Data
from plot_setup import finastra_theme
from live_news import fetch_esg_news
import metadata_parser

st.set_page_config(page_title="ESG AI", page_icon="📊", layout='centered', initial_sidebar_state="collapsed")

@st.cache_data
def filter_company_data(df_company, esg_categories, start, end):
    comps = [df_company[df_company[i] == True] for i in esg_categories]
    df_company = pd.concat(comps)
    return df_company[df_company.DATE.between(start, end)]

@st.cache_resource
def load_data(start_data, end_data):
    data = Data().read(start_data, end_data)
    companies = data["data"].Organization.sort_values().unique().tolist()
    companies.insert(0,"Select a Company")
    return data, companies

@st.cache_data
def filter_publisher(df_company, publisher):
    if publisher != 'all':
        df_company = df_company[df_company['SourceCommonName'] == publisher]
    return df_company

def get_available_date_ranges():
    return sorted([f for f in os.listdir("Data") if os.path.isdir(os.path.join("Data", f))])

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
    style = "text-align:center; padding: 0px; font-family: arial black;, font-size: 400%"
    st.markdown(f"<h1 style='{style}'>ESG<sup>AI</sup></h1><br><br>", unsafe_allow_html=True)

    date_folders = get_available_date_ranges()
    selected_range = st.selectbox("Select a Data Range", date_folders)
    if not selected_range:
        st.warning("Please select a data range.")
        return
    start_data, end_data = selected_range.split("_to_")

    with st.spinner("Fetching Data..."):
        data, companies = load_data(start_data, end_data)
    df_data = data["data"]

    st.sidebar.title("Filter Options")
    esg_categories = st.sidebar.multiselect("Select News Categories", ["E", "S", "G"], ["E", "S", "G"])
    num_neighbors = st.sidebar.slider("Number of Connections", 1, 20, value=8)

    company = st.selectbox("Select a Company to Analyze", companies)
    if company and company != "Select a Company":
        st.success(f"You selected: {company}")
        df_company = df_data[df_data.Organization == company]
        st.subheader("Recent ESG Articles")
        st.dataframe(df_company[["DATE", "SourceCommonName", "URL"]].head())

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
                st.info("No ESG-related news found right now.")
        except Exception as e:
            st.error(f"Failed to fetch live news: {e}")

if __name__ == "__main__":
    main()
