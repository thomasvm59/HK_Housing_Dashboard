import plotly.graph_objects as go
import html
from streamlit.components.v1 import html as st_html
import pandas as pd
import streamlit as st 
import folium
import numpy as np
from branca.colormap import linear
from plotly.subplots import make_subplots

# Styling Functions
def style_values(value, negative_style="color:red;", positive_style="color:green;"):
    """Apply styles based on value."""
    if pd.isna(value) or isinstance(value, str):
        return None
    return negative_style if value < 0 else positive_style if value > 0 else None


def style_dataframe(df):
    """Apply styles and formatting to a DataFrame."""

    style_format = {
        'unit_price_vs_histo': '{:.2%}',
    }
    
    # Apply styles and formatting to the dataframe
    return (df.style
            .format(style_format)  # Apply HTML escaping for clickable links
            .applymap(style_values, subset=['unit_price_vs_histo']))  # Apply additional styling to specific columns

def plot_map(df):
    # Define the desired center of the map (latitude, longitude)
    center_latitude = 22.3193  # Latitude for Hong Kong
    center_longitude = 114.1694  # Longitude for Hong Kong

    # Create the map centered at the specified coordinates, with a less zoomed-in view
    property_map = folium.Map(location=[center_latitude, center_longitude], zoom_start=13)

    # Ensure necessary columns exist
    if not all(col in df.columns for col in ['name', 'address', 'price', 'latitude', 'longitude', 'url']):
        print("DataFrame is missing one or more required columns.")
        return
    
    # Loop through each property in the DataFrame and add a marker
    for index, row in df.iterrows():
        try:
            province = row['province']
            area = row['area']
            price = row['price']
            floor_size = row['floor_size']
            latitude = float(row['latitude'])  # Ensure it's a float
            longitude = float(row['longitude'])  # Ensure it's a float
            url = row['url']
            url_transit = row['url_transit']
            url_history = row['url_history']

            popup_content = f"""
            {html.escape(province)}<br>
            {html.escape(area)}<br>
            {html.escape(str(price))}<br>
            {html.escape(str(floor_size))} sf<br>
            <a href="{html.escape(url)}" target="_blank">Listing</a><br>
            <a href="{html.escape(url_transit)}" target="_blank">Transit</a><br>
            <a href="{html.escape(url_history)}" target="_blank">History</a>
            """

            
            folium.Marker([latitude, longitude], popup=popup_content).add_to(property_map)
        
        except Exception as e:
            print(f"Error adding marker for index {index}: {e}")

    # Save the map as an HTML string
    map_html = property_map._repr_html_()
    
    # Display the map in Streamlit with custom size
    st_html(map_html, height=800, width=1000)


def plot_map_color(df, coordinates_map):
    # Center of the map
    center_latitude = 22.3193  # Latitude for Hong Kong
    center_longitude = 114.1694  # Longitude for Hong Kong

    df_price_district = df[['area_district','unit_price']].groupby(['area_district']).mean()
    
    # Create the map centered at the specified coordinates, with a less zoomed-in view
    m = folium.Map(location=[center_latitude, center_longitude], zoom_start=10)
    
    # Define color scale (using a linear color map based on unit_price)
    colormap = linear.YlOrRd_09.scale(df_price_district['unit_price'].min(), df_price_district['unit_price'].max())
    
    for key, geo_coords in coordinates_map.items():
        # Get the value associated with the current district
        if key in df_price_district.index:
            value = df_price_district.loc[key, 'unit_price']
        else:
            value = None  # Handle districts without data
        
        # Skip districts with no value
        if value is None:
            continue
        
        # Get the color based on the value (price)
        color = colormap(value)
        
        # Add the polygon to the map with the color
        folium.Polygon(
            locations=[coord[::-1] for coord in geo_coords],  # Reverse (lon, lat) to (lat, lon)
            fill=True,
            fill_color=color,
            color=color,
            weight=2,
            fill_opacity=0.5,
            popup=f"{key}: {value:.2f}"  # Add popup showing district and price
        ).add_to(m)
    
    # Add the colormap to the map
    colormap.caption = "Unit Price by District"
    colormap.add_to(m)
    
    # Save the map as an HTML string
    map_html = m._repr_html_()
    
    # Display the map in Streamlit with custom size
    st_html(map_html, height=800, width=1000)


def plot_unit_price_evolution(df_print,column):
    # Convert DataFrame to long format
    df_melted = df_print.reset_index().melt(id_vars=[column], var_name="Year", value_name="Unit Price")

    # Convert 'Year' to integer for proper sorting
    df_melted["Year"] = df_melted["Year"].astype(int)

    # Sort the DataFrame properly
    df_melted = df_melted.sort_values(by=[column, "Year"])

    # Compute the percentage change
    df_melted["Previous Price"] = df_melted.groupby(column)["Unit Price"].shift(1)
    df_melted["Change (%)"] = ((df_melted["Unit Price"] - df_melted["Previous Price"]) / df_melted["Previous Price"]) * 100

    # Handle NaN values (first year should not have a % change)
    df_melted["Change (%)"] = df_melted["Change (%)"].fillna(0)  # First year is set to 0 for now

    # Define a color scale for -10% (red) to +10% (green), centered at 0 (white)
    def get_color(change):
        if change == 0:
            return "black"  # First year is always black
        elif change < -10:
            return "darkred"
        elif change > 10:
            return "darkgreen"
        else:
            # Interpolate color between red (-10%) to white (0%) to green (+10%)
            red = max(0, min(255, int(255 * (1 - (change + 10) / 20))))
            green = max(0, min(255, int(255 * ((change + 10) / 20))))
            return f"rgb({red},{green},0)"  # Dynamic color

    df_melted["Color"] = df_melted["Change (%)"].apply(get_color)

    # Get the list of districts
    districts = df_melted[column].unique()

    # Define subplot grid size (auto-adjust based on number of districts)
    rows = (len(districts) // 4) + 1  # 4 columns per row
    cols = min(len(districts), 4)  # Maximum 3 per row

    # Create subplots with increased vertical spacing
    fig = make_subplots(
        rows=rows, 
        cols=cols, 
        subplot_titles=districts, 
        vertical_spacing=0.05,  # Increase the vertical space between rows
        shared_xaxes=False,
        shared_yaxes=True,
    )

    # Iterate through districts and add bar charts
    for i, district in enumerate(districts):
        row = (i // 4) + 1
        col = (i % 4) + 1
        
        district_data = df_melted[df_melted[column] == district]

        # Add bar trace
        fig.add_trace(
            go.Bar(
                x=district_data["Year"], 
                y=district_data["Unit Price"], 
                marker=dict(color=district_data["Color"]),  # Color based on % change
                name=district,
                text=[f"{val:.1f}%" if prev != 0 else "N/A" for val, prev in zip(district_data["Change (%)"], district_data["Previous Price"])], 
                textposition="auto"  # Show % change above bars
            ),
            row=row, col=col
        )

    # Update layout for readability
    fig.update_layout(
        title_text="Unit Price Evolution by District (Color Gradual on Change %)",
        showlegend=False,
        height=rows * 300,
        width=1200,
        margin=dict(t=80, b=80, l=50, r=50),
    )

    return fig

        
    
    
    
    
    
    
