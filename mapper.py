#!/usr/bin/env python3
import sys
import json
import math
from datetime import datetime   

for line in sys.stdin:
    try:
        line = json.loads(line) 
        if math.isnan(line['Severity']) or math.isnan(line['Visibility(mi)']) or math.isnan(line['Precipitation(in)']):
            continue
        starttime = line['Start_Time']
        
        try:
            date = datetime.strptime(starttime ,'%Y-%m-%d %H:%M:%S')
        except:  
            starttime=starttime.split(".")[0]
            date=datetime.strptime(starttime ,'%Y-%m-%d %H:%M:%S')

        hour = date.hour
        severity = line['Severity']
        sunrisesunset = line['Sunrise_Sunset']
        visibility = line['Visibility(mi)']
        precipitation = line['Precipitation(in)']
        weathercondition = line['Weather_Condition']
        description = line['Description']
        
        
        if description.lower().find("lane blocked") != -1 or description.lower().find("shoulder blocked") != -1 or description.lower().find("overturned vehicle") != -1:
            if severity >= 2 and visibility <= 10 and precipitation >= 0.2:
                if sunrisesunset == "Night":
                    if weathercondition in ["Heavy Snow", "Thunderstorm", "Heavy Rain", "Heavy Rain Showers", "Blowing Dust"]:
                        print(f"{hour}")
    except:
        pass


        
