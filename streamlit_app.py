import streamlit as st
import pandas as pd
import zipfile
import requests
from io import BytesIO
from google.transit import gtfs_realtime_pb2
import urllib.request
from datetime import datetime
import pytz

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

@st.cache_resource
def load_gtfs_static_data():
    url = "http://web.mta.info/developers/data/nyct/subway/google_transit.zip"
    response = requests.get(url)
    if response.status_code != 200:
        st.error("Failed to download GTFS static data.")
        return None, None, None, None

    zf = zipfile.ZipFile(BytesIO(response.content))
    routes = pd.read_csv(zf.open('routes.txt'))
    trips = pd.read_csv(zf.open('trips.txt'))
    stop_times = pd.read_csv(zf.open('stop_times.txt'))
    stops = pd.read_csv(zf.open('stops.txt'))

    return routes, trips, stop_times, stops

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

def parse_feed(feed, stop_data, stop_id_to_name, start_stop, end_stop):
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

            for stop_time in trip.stop_time_update:
                stop_id = stop_time.stop_id

                if stop_id.startswith(start_base) or stop_id.startswith(end_base):
                    stop_name = stop_id_to_name.get(stop_id[:3], "Unknown Stop")
                    if stop_time.HasField('arrival'):
                        arrival_time = stop_time.arrival.time
                        arrival_time_nyc = datetime.fromtimestamp(arrival_time, tz=nyc_tz).strftime('%I:%M:%S %p')
                        data.append({
                            'Train': route_id,
                            'Stop ID': stop_id,
                            'Stop Name': stop_name,
                            'Arrival Time (NYC)': arrival_time_nyc
                        })

    return pd.DataFrame(data)

# ------------------------- Streamlit UI ----------------------------
st.title("🚇 NYC Subway Real-time Tracker")

feed_name = st.selectbox("Select a subway line:", list(FEED_URLS.keys()))
feed_url = FEED_URLS[feed_name]

routes, trips, stop_times, stops = load_gtfs_static_data()
if routes is None:
    st.stop()

line_stops = get_stops_for_line(feed_name, routes, trips, stop_times, stops)
stop_id_to_name = dict(zip(stops['stop_id'].str[:3], stops['stop_name']))

start_stop = st.selectbox("Select the starting station", line_stops['stop_name'].unique())
end_stop = st.selectbox("Select the ending station", line_stops['stop_name'].unique())

if st.button("🔍 Get Real-time Arrivals"):
    feed = fetch_subway_feed(feed_url)
    if feed:
        df = parse_feed(feed, stops, stop_id_to_name, start_stop, end_stop)
        if not df.empty:
            st.success("Live arrival info loaded:")
            st.dataframe(df)
        else:
            st.warning("No upcoming trains found between selected stops.")