# download-metar
Download METAR messages with global coverage from NOAA and store them in InfluxDB

## overview

[NOAA](https://www.noaa.gov/) publishes [METAR](https://en.wikipedia.org/wiki/METAR) messages (weather reports) from airports all over the planet at https://tgftp.nws.noaa.gov/data/observations/metar/cycles/. This script downloads the messages in text form periodically, parses them with [python-metar](https://github.com/python-metar/python-metar) and stores them results in InfluxDB.

## install and run

```bash
python3.9 -m venv venv
source venv/bin/activate
pip install metar request influxdb
python download.py
```
