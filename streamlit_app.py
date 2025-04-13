import streamlit as st
import pandas as pd
import zipfile
import requests
from io import BytesIO
from google.transit import gtfs_realtime_pb2
import urllib.request
from datetime import datetime
import pytz
import folium
from streamlit_folium import st_folium

# NYC timezone
nyc_tz = pytz.timezone('America/New_York')

# Mapping feed names to GTFS-RT URLs
FEED_URLS = {
    "1-2-3": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "4-5-6": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "7": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "A-C-E": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "B-D-F-M": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "G": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    "J-Z": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    "L": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    "N-Q-R-W": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "S": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"
}

# Use session_state to track data
if 'df' not in st.session_state:
    st.session_state['df'] = pd.DataFrame()

if 'selected_trip_id' not in st.session_state:
    st.session_state['selected_trip_id'] = None

@st.cache_resource
def load_gtfs_static_data():
    url = "http://web.mta.info/developers/data/nyct/subway/google_transit.zip"
    response = requests.get(url)
    if response.status_code != 200:
        st.error("Failed to download GTFS static data.")
        return None, None, None, None, None

    zf = zipfile.ZipFile(BytesIO(response.content))
    routes = pd.read_csv(zf.open('routes.txt'))
    trips = pd.read_csv(zf.open('trips.txt'))
    stop_times = pd.read_csv(zf.open('stop_times.txt'))
    stops = pd.read_csv(zf.open('stops.txt'))
    trip_headsign_map = dict(zip(trips['trip_id'], trips['trip_headsign']))

    return routes, trips, stop_times, stops, trip_headsign_map

def get_stops_for_line(feed_key, routes, trips, stop_times, stops):
    prefix = feed_key.lower().replace('gtfs-', '').split('-')[0]
    matching_routes = routes[routes['route_id'].str.lower().str.contains(prefix)]
    route_ids = matching_routes['route_id'].unique()
    trip_ids = trips[trips['route_id'].isin(route_ids)]['trip_id'].unique()
    stop_ids = stop_times[stop_times['trip_id'].isin(trip_ids)]['stop_id'].unique()
    filtered_stops = stops[stops['stop_id'].isin(stop_ids)].drop_duplicates(subset='stop_name')
    return filtered_stops.sort_values(by='stop_name')

def fetch_subway_feed(feed_url):
    try:
        response = urllib.request.urlopen(feed_url)
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.read())
        return feed
    except Exception as e:
        st.error(f"Error fetching feed: {e}")
        return None

def parse_feed(feed, stop_data, stop_id_to_name, start_stop, end_stop, trip_headsign_map):
    data = []
    now = datetime.now(nyc_tz).timestamp()

    try:
        start_base = stop_data[stop_data['stop_name'] == start_stop]['stop_id'].values[0][:3]
        end_base = stop_data[stop_data['stop_name'] == end_stop]['stop_id'].values[0][:3]
    except IndexError:
        st.error("Selected stops not found in GTFS data.")
        return pd.DataFrame()

    for entity in feed.entity:
        if entity.HasField('trip_update'):
            trip = entity.trip_update
            route_id = trip.trip.route_id
            stop_updates = trip.stop_time_update
            trip_id = trip.trip.trip_id

            found_start = None
            found_end = None
            headsign = trip_headsign_map.get(trip_id)

            if not headsign:
                for k, v in trip_headsign_map.items():
                    if trip_id.startswith(k):
                        headsign = v
                        break

            delay = None
            if trip.trip.HasField("schedule_relationship"):
                delay = trip.trip.schedule_relationship

            for update in stop_updates:
                stop_id = update.stop_id
                if stop_id.startswith(start_base) and update.HasField('arrival'):
                    found_start = update.arrival.time
                if stop_id.startswith(end_base) and update.HasField('arrival'):
                    found_end = update.arrival.time

            if found_start and found_end and found_start < found_end and found_start > now:
                minutes_away = int((found_start - now) / 60)
                arrival_time = datetime.fromtimestamp(found_start, tz=nyc_tz).strftime('%I:%M:%S %p')
                destination_time = datetime.fromtimestamp(found_end, tz=nyc_tz).strftime('%I:%M:%S %p')
                status = "Delayed" if delay else "On Time"
                data.append({
                    'Train': route_id,
                    'From': start_stop,
                    'To': end_stop,
                    'Arrival Time (Exact)': arrival_time,
                    'Arriving In': f"{minutes_away} min",
                    # 'Headsign': headsign or "Unknown",
                    'Destination Arrival Time': destination_time,
                    'Status': status,
                    'Trip ID': trip_id
                })

    return pd.DataFrame(data)

def plot_selected_trip_on_map(trip_id, feed, stops):
    now = datetime.now(nyc_tz).timestamp()
    trip_stops = []

    for entity in feed.entity:
        if entity.HasField('trip_update') and entity.trip_update.trip.trip_id == trip_id:
            for update in entity.trip_update.stop_time_update:
                stop_match = stops[stops['stop_id'].str.startswith(update.stop_id[:3])]
                if not stop_match.empty and update.HasField('arrival'):
                    stop_info = stop_match.iloc[0]
                    arrival_time = datetime.fromtimestamp(update.arrival.time, tz=nyc_tz).strftime('%I:%M:%S %p')
                    trip_stops.append({
                        'stop_name': stop_info['stop_name'],
                        'stop_lat': stop_info['stop_lat'],
                        'stop_lon': stop_info['stop_lon'],
                        'arrival': arrival_time,
                        'minutes_away': int((update.arrival.time - now) / 60)
                    })
            break

    if not trip_stops:
        st.warning("No stops found for selected trip.")
        return None

    subway_map = folium.Map(location=[trip_stops[0]['stop_lat'], trip_stops[0]['stop_lon']], zoom_start=14)
    for stop in trip_stops:
        popup = f"{stop['stop_name']}: {stop['arrival']} (in {stop['minutes_away']} min)"
        folium.Marker(
            location=[stop['stop_lat'], stop['stop_lon']],
            popup=popup,
            icon=folium.Icon(color="blue", icon="info-sign")
        ).add_to(subway_map)

    return subway_map

# ------------------------- Streamlit UI ----------------------------
st.set_page_config(page_title="NYC Subway Real-time Tracker", layout="wide")
st.markdown("### 🚇 NYC Subway Real-time Tracker")

feed_name = st.selectbox("Select a subway line:", list(FEED_URLS.keys()))
feed_url = FEED_URLS[feed_name]

routes, trips, stop_times, stops, trip_headsign_map = load_gtfs_static_data()
if routes is None:
    st.stop()

line_stops = get_stops_for_line(feed_name, routes, trips, stop_times, stops)
stop_id_to_name = dict(zip(stops['stop_id'].str[:3], stops['stop_name']))

start_stop = st.selectbox("From:", line_stops['stop_name'].unique())
end_stop = st.selectbox("To:", line_stops['stop_name'].unique())

if st.button("Search Trains"):
    feed = fetch_subway_feed(feed_url)
    df = parse_feed(feed, line_stops, stop_id_to_name, start_stop, end_stop, trip_headsign_map)
    st.session_state['df'] = df
    if not df.empty:
        st.session_state['selected_trip_id'] = df.iloc[0]['Trip ID']
        st.session_state['show_map'] = True
    else:
        st.session_state['selected_trip_id'] = None
        st.session_state['show_map'] = False

if not st.session_state['df'].empty:
    st.dataframe(st.session_state['df'].drop(columns=['Trip ID']), use_container_width=True)

    if st.session_state['show_map'] and st.session_state['selected_trip_id']:
        st.markdown("### Subway Route Map (First Trip)")
        subway_map = plot_selected_trip_on_map(
            st.session_state['selected_trip_id'], fetch_subway_feed(feed_url), stops
        )
        if subway_map:
            st_folium(subway_map, width=700, height=500)