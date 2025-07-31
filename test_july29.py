#!/usr/bin/env python3
"""
Test script to run chronowaffle on July 29, 2025 data.
"""

import json
from connect_db import connect_to_db
from chronowaffle import create_chronowaffle
from sqlalchemy import text

def fetch_fitbit_data_from_db(date_str: str):
    """
    Fetch Fitbit data from database for the given date.
    Returns mock JSON structures similar to Fitbit API.
    """
    engine = connect_to_db()
    
    # Mock sleep JSON structure (we'll populate from DB if data exists)
    sleep_json = {"sleep": []}
    
    # Mock HR JSON structure (we'll populate from DB if data exists)
    hr_json = {"activities-heart-intraday": {"dataset": []}}
    
    with engine.connect() as conn:
        # Check for sleep data
        sleep_query = text("""
            SELECT date_of_sleep, start_time, end_time, 
                   minutes_deep, minutes_light, minutes_rem, minutes_wake
            FROM sleep_sessions 
            WHERE date_of_sleep = :date_str
        """)
        sleep_result = conn.execute(sleep_query, {"date_str": date_str})
        sleep_rows = sleep_result.fetchall()
        
        if sleep_rows:
            for row in sleep_rows:
                # Create a mock sleep session
                session = {
                    "startTime": row.start_time.isoformat() if row.start_time else f"{date_str}T23:00:00.000",
                    "endTime": row.end_time.isoformat() if row.end_time else f"{date_str}T07:00:00.000",
                    "levels": {
                        "data": []
                    }
                }
                
                # Add mock sleep stages if we have the minutes data
                if row.minutes_deep:
                    session["levels"]["data"].append({
                        "dateTime": session["startTime"],
                        "level": "deep",
                        "seconds": row.minutes_deep * 60
                    })
                if row.minutes_light:
                    session["levels"]["data"].append({
                        "dateTime": session["startTime"],
                        "level": "light", 
                        "seconds": row.minutes_light * 60
                    })
                if row.minutes_rem:
                    session["levels"]["data"].append({
                        "dateTime": session["startTime"],
                        "level": "rem",
                        "seconds": row.minutes_rem * 60
                    })
                
                sleep_json["sleep"].append(session)
        
        # Check for heart rate data
        hr_query = text("""
            SELECT datetime, heart_rate FROM heart_rate_readings 
            WHERE DATE(datetime) = :date_str
            ORDER BY datetime
        """)
        hr_result = conn.execute(hr_query, {"date_str": date_str})
        hr_rows = hr_result.fetchall()
        
        if hr_rows:
            for row in hr_rows:
                # Extract time from datetime (convert to local time)
                local_dt = row.datetime.astimezone()
                time_str = local_dt.strftime("%H:%M:%S")
                hr_json["activities-heart-intraday"]["dataset"].append({
                    "time": time_str,
                    "value": row.heart_rate
                })
    
    return sleep_json, hr_json

def main():
    """Test chronowaffle with July 29, 2025 data."""
    date_str = "2025-07-29"
    tz_name = "America/Denver"
    max_hr = 173  # You'll need to provide your actual max HR
    
    print(f"Fetching Fitbit data for {date_str}...")
    
    # Fetch data from database
    sleep_json, hr_json = fetch_fitbit_data_from_db(date_str)
    
    print(f"Found {len(sleep_json['sleep'])} sleep sessions")
    print(f"Found {len(hr_json['activities-heart-intraday']['dataset'])} HR readings")
    
    if not sleep_json['sleep'] and not hr_json['activities-heart-intraday']['dataset']:
        print("No data found in database. Creating mock data for testing...")
        
        # Create mock sleep data (11 PM to 7 AM)
        sleep_json = {
            "sleep": [{
                "startTime": "2025-07-29T23:00:00.000",
                "endTime": "2025-07-30T07:00:00.000",
                "levels": {
                    "data": [
                        {
                            "dateTime": "2025-07-29T23:00:00.000",
                            "level": "light",
                            "seconds": 3600  # 1 hour
                        },
                        {
                            "dateTime": "2025-07-30T00:00:00.000", 
                            "level": "deep",
                            "seconds": 7200  # 2 hours
                        },
                        {
                            "dateTime": "2025-07-30T02:00:00.000",
                            "level": "rem", 
                            "seconds": 3600  # 1 hour
                        },
                        {
                            "dateTime": "2025-07-30T03:00:00.000",
                            "level": "light",
                            "seconds": 14400  # 4 hours
                        }
                    ]
                }
            }]
        }
        
        # Create mock HR data (some sample readings throughout the day)
        hr_json = {
            "activities-heart-intraday": {
                "dataset": [
                    {"time": "08:00:00", "value": 65},  # Sitting
                    {"time": "08:30:00", "value": 75},  # Standing
                    {"time": "09:00:00", "value": 95},  # Active movement
                    {"time": "10:00:00", "value": 110}, # Easy exercise
                    {"time": "10:30:00", "value": 125}, # Higher zone
                    {"time": "14:00:00", "value": 70},  # Afternoon rest
                    {"time": "16:00:00", "value": 105}, # Afternoon activity
                    {"time": "20:00:00", "value": 80},  # Evening
                ]
            }
        }
    
    print("\nGenerating chronowaffle...")
    
    try:
        waffle = create_chronowaffle(
            date_str=date_str,
            tz_name=tz_name,
            sleep_json=sleep_json,
            hr_json=hr_json,
            max_hr=max_hr
        )
        
        print(f"\nChronowaffle Results for {date_str}:")
        print(f"Timezone: {waffle.tz_name}")
        print(f"Grid: {waffle.rows}x{waffle.cols} ({waffle.bin_size_min} min bins)")
        print(f"Total awake minutes: {waffle.awake_minutes}")
        print(f"Unknown minutes: {waffle.unknown_minutes}")
        
        print("\nZone distribution (awake minutes only):")
        for zone, minutes in sorted(waffle.zone_minutes_awake_only.items()):
            if minutes > 0:
                print(f"  Zone {zone}: {minutes} minutes")
        
        print(f"\nFirst 12 cells (first row - midnight to 03:00):")
        for i in range(12):
            cell = waffle.cells[i]
            start_hour = cell.start_min // 60
            start_min = cell.start_min % 60
            end_hour = cell.end_min // 60
            end_min = cell.end_min % 60
            
            status = "Sleep" if cell.is_sleep else f"Zone {cell.dominant_zone}"
            print(f"  {start_hour:02d}:{start_min:02d}-{end_hour:02d}:{end_min:02d}: {status}")
            
    except Exception as e:
        print(f"Error generating chronowaffle: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()