
import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import os
import time
from datetime import datetime
import plotly.express as px
from plot_setup import finastra_theme
from download_data import Data
import sys
import metadata_parser

st.set_page_config(page_title="ESG AI", page_icon="📊", layout='centered', initial_sidebar_state="collapsed")

@st.cache_data(show_spinner=False)
def filter_company_data(df_company, esg_categories, start, end):
    comps = []
    for i in esg_categories:
        X = df_company[df_company[i] == True]
        comps.append(X)
    df_company = pd.concat(comps)
    df_company = df_company[df_company.DATE.between(start, end)]
    return df_company

@st.cache_resource(show_spinner=False)
def load_data(start_data, end_data):
    data = Data().read(start_data, end_data)
    companies = data["data"].Organization.sort_values().unique().tolist()
    companies.insert(0, "Select a Company")
    return data, companies

@st.cache_data(show_spinner=False)
def filter_publisher(df_company, publisher):
    if publisher != 'all':
        df_company = df_company[df_company['SourceCommonName'] == publisher]
    return df_company

def get_clickable_name(url):
    try:
        T = metadata_parser.MetadataParser(url=url, search_head_only=True)
        title = T.metadata["og"]["title"].replace("|", " - ")
        return f"[{title}]({url})"
    except:
        return f"[{url}]({url})"

def get_available_date_ranges():
    folders = [f for f in os.listdir("Data") if os.path.isdir(os.path.join("Data", f))]
    return sorted(folders)

def main():
    alt.themes.register("finastra", finastra_theme)
    alt.themes.enable("finastra")
    violet, fuchsia = ["#694ED6", "#C137A2"]

    icon_path = os.path.join(".", "raw", "esg_ai_logo.png")
    _, logo, _ = st.columns(3)
    logo.image(icon_path, width=200)
    style = "text-align:center; padding: 0px; font-family: arial black;, font-size: 400%"
    st.markdown(f"<h1 style='{style}'>ESG<sup>AI</sup></h1><br><br>", unsafe_allow_html=True)

    # 📆 New dropdown to select date folder
    available_ranges = get_available_date_ranges()
    selected_range = st.selectbox("Select a Data Range", available_ranges)
    if not selected_range:
        st.warning("Please select a valid date folder.")
        return
    start_data, end_data = selected_range.split("_to_")

    with st.spinner("Fetching Data..."):
        data, companies = load_data(start_data, end_data)
    df_conn = data["conn"]
    df_data = data["data"]
    embeddings = data["embed"]
    avg_esg = data["ESG"]

    st.sidebar.title("Filter Options")
    date_place = st.sidebar.empty()
    esg_categories = st.sidebar.multiselect("Select News Categories", ["E", "S", "G"], ["E", "S", "G"])
    pub = st.sidebar.empty()
    num_neighbors = st.sidebar.slider("Number of Connections", 1, 20, value=8)

    company = st.selectbox("Select a Company to Analyze", companies)
    if company and company != "Select a Company":
        st.success(f"You selected: {company}")
        df_company = df_data[df_data.Organization == company]
        st.subheader("Recent Articles")
        st.dataframe(df_company[["DATE", "SourceCommonName", "URL"]].head())

        avg_esg.rename(columns={"Unnamed: 0": "Type"}, inplace=True)
        avg_esg.replace({"T": "Overall", "E": "Environment", "S": "Social", "G": "Governance"}, inplace=True)
        avg_esg["Industry Average"] = avg_esg.select_dtypes(include=[np.number]).mean(axis=1)
        radar_df = avg_esg[["Type", company, "Industry Average"]].melt("Type", value_name="score", var_name="entity")

        st.subheader("ESG Radar")
        radar = px.line_polar(radar_df, r="score", theta="Type",
                              color="entity", line_close=True,
                              color_discrete_map={"Industry Average": fuchsia, company: violet})
        st.plotly_chart(radar, use_container_width=True)

        esg_keys = ["E_score", "S_score", "G_score"]
        score_cols = [f"{company.replace(' ', '_')}_diff"]
        esg_df = pd.concat([data[k][score_cols[0]].rename(k) for k in esg_keys if score_cols[0] in data[k]], axis=1)
        esg_df["DATE"] = data[esg_keys[0]].index
        esg_melted = esg_df.melt(id_vars="DATE", var_name="Category", value_name="Score")

        st.subheader("ESG Score Trends")
        chart = alt.Chart(esg_melted).mark_line().encode(
            x="yearmonthdate(DATE):T",
            y="Score:Q",
            color="Category:N"
        ).properties(width=700, height=400)
        st.altair_chart(chart, use_container_width=True)

if __name__ == "__main__":
    main()
