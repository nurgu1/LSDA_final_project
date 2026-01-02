import json
import boto3
import urllib3
from datetime import datetime

BUCKET_NAME = 'flight-risk-engine-mglgx7' 

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    http = urllib3.PoolManager()
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M')
    
    locations = [
        {"city": "LHR", "lat": "51.47", "lon": "-0.45"},   # London
        {"city": "LAX", "lat": "33.94", "lon": "-118.40"}, # Los Angeles
        {"city": "MIA", "lat": "25.76", "lon": "-80.19"},  # Miami
        {"city": "CDG", "lat": "49.00", "lon": "2.55"},    # Paris
        {"city": "SEA", "lat": "47.45", "lon": "-122.30"}, # Seattle
        {"city": "JFK", "lat": "40.64", "lon": "-73.78"},  # New York
        {"city": "ATL", "lat": "33.64", "lon": "-84.42"},  # Atlanta
        {"city": "DTW", "lat": "42.21", "lon": "-83.35"},  # Detroit
        {"city": "MSP", "lat": "44.88", "lon": "-93.22"},  # Minneapolis
        {"city": "SFO", "lat": "37.62", "lon": "-122.37"}, # San Francisco
        {"city": "AMS", "lat": "52.31", "lon": "4.76"},    # Amsterdam
        {"city": "NRT", "lat": "35.77", "lon": "140.39"},  # Tokyo
        {"city": "SYD", "lat": "-33.93", "lon": "151.17"}, # Sydney
        {"city": "DEN", "lat": "39.85", "lon": "-104.67"}, # Denver
        {"city": "ORD", "lat": "41.97", "lon": "-87.90"},  # Chicago
        {"city": "LGA", "lat": "40.77", "lon": "-73.87"},  # New York LGA
        {"city": "MCO", "lat": "28.43", "lon": "-81.30"},  # Orlando
        {"city": "BOS", "lat": "42.36", "lon": "-71.00"},  # Boston
        {"city": "GRU", "lat": "-23.43", "lon": "-46.47"}, # Sao Paulo
        {"city": "FCO", "lat": "41.80", "lon": "12.24"}    # Rome
    ]
    
    weather_data = []
    solar_data = []
    
    print("Starting API Fetch...")
    
    for loc in locations:
        try:
            weather_url = f"https://api.open-meteo.com/v1/forecast?latitude={loc['lat']}&longitude={loc['lon']}&current_weather=true"
            r_weather = http.request('GET', weather_url)
            w_json = json.loads(r_weather.data.decode('utf-8'))
            
            weather_data.append({
                "city": loc['city'],
                "temperature": w_json['current_weather']['temperature'],
                "windspeed": w_json['current_weather']['windspeed'],
                "weathercode": w_json['current_weather']['weathercode']
            })

            solar_url = f"https://api.sunrise-sunset.org/json?lat={loc['lat']}&lng={loc['lon']}&date=today&formatted=0"
            r_solar = http.request('GET', solar_url)
            s_json = json.loads(r_solar.data.decode('utf-8'))
            
            solar_data.append({
                "city": loc['city'],
                "sunset": s_json['results']['sunset']
            })
            
        except Exception as e:
            print(f"Error for {loc['city']}: {str(e)}")

   
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

    s3.put_object(Bucket=BUCKET_NAME, Key=f"raw/weather/weather_{timestamp}.json", Body=json.dumps(weather_data))
    s3.put_object(Bucket=BUCKET_NAME, Key=f"raw/solar/solar_{timestamp}.json", Body=json.dumps(solar_data))
    return {"status": 200, "message": "Real Data Ingested"}
