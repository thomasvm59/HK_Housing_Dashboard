import streamlit as st
from data import *
from style_and_plot import *

#create dataframes from the function 
now_dt = datetime.datetime.now(tz=datetime.timezone.utc)
now_hr_ts = (now_dt.timestamp() // 3600) * 3600

#update_database(now_hr_ts)

# Add a button to trigger the update
# if st.sidebar.button("Update Database"):
#     with st.spinner("Updating database..."):
#         try:
#             update_database(now_hr_ts)
#             st.success("Database updated successfully!")
#         except Exception as e:
#             st.error(f"Error updating database: {e}")
            
df_listing, df_history, fx_rates, coordinates_map, update_dt=load_data(now_hr_ts)
min_ago = int(max((now_dt - update_dt).total_seconds() // 60, 0))
s = "" if min_ago == 1 else "s"
st.markdown(
    f"""
    Market data was last updated {min_ago} minute{s} ago at
    {str(update_dt)[:16]} UTC."""
)

df_province = df_history[['province', 'lease_year', 'unit_price']].dropna().groupby(['province','lease_year']).mean().unstack()['unit_price']
df_area_district = df_history[['area_district', 'lease_year', 'unit_price']].dropna().groupby(['area_district','lease_year']).mean().unstack()['unit_price']
df_area = df_history[['area', 'lease_year', 'unit_price']].dropna().groupby(['area','lease_year']).mean().unstack()['unit_price']
df_listing['historical_average'] = [df_area_district.mean(axis=1).loc[distric] for distric in df_listing['area_district']]
df_listing['unit_price_vs_histo']=df_listing['unit_price']/df_listing['historical_average']-1

add_sidebar = st.sidebar.selectbox('Listing Search or District Statistics', ('Listing Search', 'Districts Statistics'))
st.title("Hong Kong Property Map")

if add_sidebar == 'Listing Search':
    # Initialize session state for all currencies
    for currency in fx_rates.keys():
        if currency not in st.session_state:
            st.session_state[currency] = 0.0
    
    # Set default value for HKD to 25,000 if it's not already set
    if "HKD" not in st.session_state or st.session_state["HKD"] == 0.0:
        st.session_state["HKD"] = 25000
        # Trigger update to sync all other currencies
        base_value = st.session_state["HKD"] / fx_rates["HKD"]
        for currency in fx_rates.keys():
            if currency != "HKD":
                st.session_state[currency] = base_value * fx_rates[currency]
    
    # Update all currencies based on the modified one
    def update_currencies(changed_currency):
        base_value = st.session_state[changed_currency] / fx_rates[changed_currency]
        for currency in fx_rates.keys():
            if currency != changed_currency:
                st.session_state[currency] = base_value * fx_rates[currency]
    
    # Format function for currency values
    def format_currency(value):
        return f"{value:,.0f}"
    
    # Input Section for FX Conversion
    st.header("Currency Conversion")
    columns = st.columns(len(fx_rates))
    
    for idx, (currency, rate) in enumerate(fx_rates.items()):
        with columns[idx]:
            formatted_value = format_currency(st.session_state[currency])
            st.number_input(
                f"{currency}", 
                key=currency, 
                format="%.0f",  # Ensures precision of 0 in the input
                on_change=lambda curr=currency: update_currencies(curr)
            )
    
    # Filter Form
    with st.form("filter_housing"):
        st.subheader("**Filter Housing**")
    
        # Input widgets
        province_choice = st.multiselect(
        "Province", 
        options=['ALL']+sorted(provinces),  # Sorted list of unique room numbers
        default=['ALL']
        )
        
        if 'ALL' in province_choice:
            possible_areas = hk_island_areas+kowloon_areas+new_t_areas+island_areas
        else:
            possible_areas=[]
            for p in province_choice:
                possible_areas=possible_areas+areas_mapping[p]
        area_choice = st.multiselect(
        "District", 
        options=['ALL']+sorted(possible_areas),  # Sorted list of unique room numbers
        default=['ALL']
        )
    
        # Input widgets
        numbers_of_rooms = st.multiselect(
        "Number of Rooms", 
        options=sorted(set(df_listing["number_of_rooms"])),  # Sorted list of unique room numbers
        default=['2','3']
        )
        price_range = st.slider(
            "Select a price", 
            min_value=0, 
            max_value=100_000, 
            value=(0, 50_000),
            step=1000
        )
        distance_range = st.slider(
            "Select a distance to office", 
            min_value=0, 
            max_value=50, 
            value=(0, 20),
            step=1
        )
        # Set slider values to the minimum and maximum of 'floor_size' (now numeric)
        floor_size_range = st.slider(
            "Select a floor size", 
            min_value=0,  # Convert to int if necessary
            max_value=2000, 
            value=(500, 1000),  # Convert to int if necessary
            step=5
        )
        show_map = st.checkbox("Show map")
    
        # Form submit button
        submitted = st.form_submit_button("Submit")
    
    if submitted:
        filtered_df = df_listing[
            (df_listing["lease_price"] >= price_range[0]) & 
            (df_listing["lease_price"] <= price_range[1]) &
            (df_listing["floor_size"] >= floor_size_range[0]) &
            (df_listing["floor_size"] <= floor_size_range[1]) &
            (df_listing["distance_to_office_km"] >= distance_range[0]) &
            (df_listing["distance_to_office_km"] <= distance_range[1]) 
        ]
        if numbers_of_rooms:  # Check if the user selected any values
            filtered_df = filtered_df[filtered_df["number_of_rooms"].isin(numbers_of_rooms)]
            
        if 'ALL' not in province_choice:
            filtered_df = filtered_df[filtered_df["province"].isin(province_choice)]
        if 'ALL' not in area_choice:
            filtered_df = filtered_df[filtered_df["area"].isin(area_choice)]
    
        if show_map:
            plot_map(filtered_df)
        st.header('Listing list')
        st.markdown(f'number of properties : {len(filtered_df)}')
        df_print = style_dataframe(filtered_df.set_index('property_number').sort_values('unit_price_vs_histo')[DISPLAY_COLUMNS])
        st.dataframe(df_print)
        
        # Function to convert URLs to Markdown format
        def make_clickable(link):
            if pd.isna(link) or link == "":
                return ""  # Return empty string if no valid URL
            return f'<a href="{link}" target="_blank">Link</a>'
        # Apply the function to URL columns
        for col in ["url", "url_transit", "url_history"]:
            filtered_df[col] = filtered_df[col].apply(make_clickable)
        # Display DataFrame with clickable links using unsafe_allow_html=True
        st.markdown(f'details:')
        st.write(filtered_df[SHORT_DISPLAY_COLUMNS].sort_values('unit_price_vs_histo').to_html(escape=False, index=False), unsafe_allow_html=True)

if add_sidebar == 'Districts Statistics':
    st.header('Current listings average price lease (HKD/SF)')
    plot_map_color(df_listing, coordinates_map)
    
    st.header('History listings average price lease (HKD/SF)')
    date_range = st.slider(
            "Select a Date Range", 
            min_value=df_history['lease_date'].min().date(),
            max_value=df_history['lease_date'].max().date(), 
            value=(df_history['lease_date'].min().date(), df_history['lease_date'].max().date()),  # Convert to int if necessary
            format="YYYY-MM-DD"
        )
    df_history_filtered = df_history[
            (df_history["lease_date"] >= date_range[0].isoformat()) & 
            (df_history["lease_date"] <= date_range[1].isoformat()) 
        ]
    plot_map_color(df_history_filtered, coordinates_map)
    st.dataframe(df_history_filtered)
    
    
    st.subheader("Price Evolution by province")
    fig_1 = plot_unit_price_evolution(df_province,'province')
    st.plotly_chart(fig_1)
    st.subheader("Price Evolution by district")
    fig_2 = plot_unit_price_evolution(df_area_district,'area_district')
    st.plotly_chart(fig_2)    
