from metar import Metar
import requests
import datetime
import influxdb
import time

def parse(message):
    result = {
        'station_id': None,  # 4 letter ICAO airport identifier
        'time': None,  # Unix timestamp
        'wind_dir': None,  # wind direction in deg
        'wind_speed': None,  # wind speed in m/s
        'temp': None,  # temperature in K
        'dewpt': None,  # dew point in K
        'press': None,  # pressure in Pa
    }
    
    try:
        obs = Metar.Metar(message)
        result['station_id'] = obs.station_id if obs.station_id else None
        result['time'] = obs.time.replace(tzinfo=datetime.timezone.utc).timestamp() if obs.time else None
        result['wind_dir'] = obs.wind_dir.value() if obs.wind_dir else None
        result['wind_speed'] = obs.wind_speed.value('KMH') / 3.6 if obs.wind_speed else None
        result['temp'] = obs.temp.value('K') if obs.temp else None
        result['dewpt'] = obs.dewpt.value('K') if obs.dewpt else None
        result['press'] = obs.press.value('HPA') * 100 if obs.press else None
    except Metar.ParserError:
        pass
    
    return result

def hour_now():
    return f'{datetime.datetime.utcnow().hour:02}'

def hour_last():
    return f'{(datetime.datetime.utcnow().hour - 1)%24:02}'

def get_url(hour):
    return f'https://tgftp.nws.noaa.gov/data/observations/metar/cycles/{hour}Z.TXT'

def get_latest():
    latest = {}
    lines = []
    timeout = 10
    lines += requests.get(get_url(hour_last()), timeout=timeout).text.splitlines()
    lines += requests.get(get_url(hour_now()), timeout=timeout).text.splitlines()
    messages = [line for line in lines if not line.startswith('2') and line != '']
    parsed = [parse(message) for message in messages]
    for p in parsed:
        if p['station_id']:
            latest[p['station_id']] = p
    return latest

def get_unsaved(previous, latest):
    unsaved = []
    for station_id in list(latest):
        if station_id not in previous or previous[station_id]['time'] != latest[station_id]['time']:
            unsaved += [latest[station_id]]
    return unsaved

def get_influx_time(timestamp):
    return datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ')

def write(unsaved):
    points = []

    for p in unsaved:
        field_names = [
            'wind_dir',
            'wind_speed',
            'temp',
            'dewpt',
            'press',
        ]
        fields = {}
        for field_name in field_names:
            if p[field_name]:
                fields[field_name] = float(p[field_name])
        
        if fields != {}:
            point = {
                'measurement': 'metar',
                'tags': {
                    'station_id': p['station_id']
                }, 
                'fields': fields,
                'time': get_influx_time(p['time']),
            }
            points += [point]
    
    client = influxdb.InfluxDBClient(database='weather')
    client.write_points(points)

if __name__ == '__main__':
    previous = {}
    while True:
        latest = get_latest()
        unsaved = get_unsaved(previous, latest)
        write(unsaved)
        previous = dict(latest)
        time.sleep(300)
