#!/usr/bin/env python3
"""
Data munging script to create chronological waffle data from raw HR and sleep data.

Takes raw intraday heart rate data and sleep data, bins the day into fixed time slots,
and calculates dominant HR zones for each bin to support chronological waffle visualization.
"""

from sqlalchemy import create_engine, text
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Dict, Optional
import json
import argparse
import pytz

# Database connection parameters
DB_HOST = "postgresql-200322-0.cloudclusters.net"
DB_PORT = 19692
DB_NAME = "kat-health"
DB_USER = "kat"
DB_PASSWORD = "daisy00654"

def connect_to_db():
    """Connect to PostgreSQL database"""
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)
    return engine

def calculate_hr_zones(heart_rate: int, max_hr: int = 173) -> int:
    """
    Calculate heart rate zone based on percentage of max HR
    Zone 1: 50-60% (Fat Burn)
    Zone 2: 60-70% (Light Cardio) 
    Zone 3: 70-80% (Moderate Cardio)
    Zone 4: 80-90% (Hard Cardio)
    Zone 5: 90-100% (Peak)
    """
    if heart_rate <= 0:
        return 0
    
    percentage = (heart_rate / max_hr) * 100
    
    if percentage < 50:
        return 1  # Rest/Recovery
    elif percentage < 60:
        return 2  # Fat Burn
    elif percentage < 70:
        return 3  # Light Cardio
    elif percentage < 80:
        return 4  # Moderate Cardio
    elif percentage < 90:
        return 5  # Hard Cardio
    else:
        return 6  # Peak

def get_sleep_periods(date_str: str) -> List[Tuple[datetime, datetime]]:
    """Get sleep periods for a given date"""
    engine = connect_to_db()
    sleep_periods = []
    
    with engine.connect() as conn:
        # Get detailed sleep stages (wake periods are NOT sleep)
        result = conn.execute(text("""
            SELECT st.session_id, st.datetime, st.level, st.seconds
            FROM sleep_sessions ss
            JOIN sleep_stages st ON ss.id = st.session_id
            WHERE ss.date_of_sleep = :date_str
            AND st.level IN ('light', 'deep', 'rem')  -- Only actual sleep, not 'wake'
            ORDER BY st.datetime
        """), {"date_str": date_str})
        
        stages = result.fetchall()
        
        for session_id, stage_datetime, level, seconds in stages:
            stage_end = stage_datetime + timedelta(seconds=seconds)
            sleep_periods.append((stage_datetime, stage_end))
    
    return sleep_periods

def is_sleeping(timestamp: datetime, sleep_periods: List[Tuple[datetime, datetime]]) -> bool:
    """Check if timestamp falls within any sleep period"""
    for sleep_start, sleep_end in sleep_periods:
        if sleep_start <= timestamp <= sleep_end:
            return True
    return False

def create_time_bins(date_str: str, local_tz: pytz.BaseTzInfo, bin_minutes: int = 15) -> List[Tuple[datetime, datetime]]:
    """
    Create time bins for the day in local timezone
    For 15-min bins: 96 bins per day (4 per hour * 24 hours)
    For 20-min bins: 72 bins per day (3 per hour * 24 hours)
    """
    # Start at midnight of the given date in local timezone
    base_date = datetime.strptime(date_str, "%Y-%m-%d")
    base_date = local_tz.localize(base_date)
    
    # Create bins from midnight to midnight next day
    bins = []
    current_time = base_date
    end_of_day = base_date + timedelta(days=1)
    
    while current_time < end_of_day:
        bin_end = current_time + timedelta(minutes=bin_minutes)
        bins.append((current_time, bin_end))
        current_time = bin_end
    
    return bins

def get_heart_rate_data(date_str: str) -> List[Tuple[datetime, int]]:
    """Get all heart rate data for a given date"""
    engine = connect_to_db()
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT datetime, heart_rate
            FROM heart_rate_readings 
            WHERE date = :date_str
            ORDER BY datetime
        """), {"date_str": date_str})
        
        return result.fetchall()

def calculate_dominant_zone_for_bin(
    bin_start: datetime, 
    bin_end: datetime, 
    hr_data: List[Tuple[datetime, int]], 
    sleep_periods: List[Tuple[datetime, datetime]],
    max_hr: int = 173
) -> Dict:
    """
    Calculate the dominant HR zone for a time bin
    Returns dict with bin info, sleep status, zone data
    """
    # Check if this bin is primarily during sleep
    bin_midpoint = bin_start + (bin_end - bin_start) / 2
    is_sleep_bin = is_sleeping(bin_midpoint, sleep_periods)
    
    # Get HR readings that fall within this bin
    bin_hr_readings = []
    for hr_time, hr_value in hr_data:
        if bin_start <= hr_time < bin_end:
            bin_hr_readings.append(hr_value)
    
    # Calculate zones for all HR readings in this bin
    zones = []
    if bin_hr_readings:
        for hr in bin_hr_readings:
            zone = calculate_hr_zones(hr, max_hr)
            zones.append(zone)
    
    # Calculate dominant zone (mode)
    dominant_zone = None
    zone_counts = {}
    if zones:
        for zone in zones:
            zone_counts[zone] = zone_counts.get(zone, 0) + 1
        dominant_zone = max(zone_counts.keys(), key=lambda k: zone_counts[k])
    
    return {
        'bin_start': bin_start,
        'bin_end': bin_end,
        'is_sleep': is_sleep_bin,
        'hr_readings_count': len(bin_hr_readings),
        'avg_hr': sum(bin_hr_readings) / len(bin_hr_readings) if bin_hr_readings else None,
        'min_hr': min(bin_hr_readings) if bin_hr_readings else None,
        'max_hr': max(bin_hr_readings) if bin_hr_readings else None,
        'dominant_zone': dominant_zone,
        'zone_counts': zone_counts,
        'all_zones': zones
    }

def create_chronological_waffle_data(date_str: str, local_tz: pytz.BaseTzInfo, bin_minutes: int = 15, max_hr: int = 173):
    """
    Main function to create chronological waffle data for a given date
    """
    print(f"Creating chronological waffle data for {date_str}")
    print(f"Using {bin_minutes}-minute bins in timezone {local_tz} (max HR: {max_hr})")
    
    # Create time bins
    time_bins = create_time_bins(date_str, local_tz, bin_minutes)
    print(f"Created {len(time_bins)} time bins")
    
    # Get sleep periods
    sleep_periods = get_sleep_periods(date_str)
    print(f"Found {len(sleep_periods)} sleep periods")
    
    # Get heart rate data
    hr_data = get_heart_rate_data(date_str)
    print(f"Found {len(hr_data)} HR readings")
    
    # Process each bin
    waffle_data = []
    for i, (bin_start, bin_end) in enumerate(time_bins):
        bin_data = calculate_dominant_zone_for_bin(bin_start, bin_end, hr_data, sleep_periods, max_hr)
        
        # Add bin index and grid position
        bin_data['bin_index'] = i
        
        # For 8x12 grid (96 bins): row = i // 12, col = i % 12
        # For 10x10 grid (100 bins): row = i // 10, col = i % 10
        if len(time_bins) == 96:  # 8x12 grid
            bin_data['grid_row'] = i // 12
            bin_data['grid_col'] = i % 12
        elif len(time_bins) <= 100:  # 10x10 grid
            bin_data['grid_row'] = i // 10
            bin_data['grid_col'] = i % 10
        else:  # Custom grid
            cols = int(len(time_bins) ** 0.5) + 1
            bin_data['grid_row'] = i // cols
            bin_data['grid_col'] = i % cols
        
        waffle_data.append(bin_data)
    
    return waffle_data

def print_waffle_summary(waffle_data: List[Dict]):
    """Print summary of waffle data"""
    total_bins = len(waffle_data)
    sleep_bins = sum(1 for bin_data in waffle_data if bin_data['is_sleep'])
    awake_bins = total_bins - sleep_bins
    
    # Count zones in awake bins
    zone_counts = {}
    for bin_data in waffle_data:
        if not bin_data['is_sleep'] and bin_data['dominant_zone'] is not None:
            zone = bin_data['dominant_zone']
            zone_counts[zone] = zone_counts.get(zone, 0) + 1
    
    print(f"\nWAFFLE SUMMARY:")
    print(f"Total bins: {total_bins}")
    print(f"Sleep bins: {sleep_bins}")
    print(f"Awake bins: {awake_bins}")
    print(f"Bins with HR data: {sum(1 for b in waffle_data if b['hr_readings_count'] > 0)}")
    
    print(f"\nZone distribution (awake bins only):")
    for zone in sorted(zone_counts.keys()):
        print(f"  Zone {zone}: {zone_counts[zone]} bins")
    
    # Show first few bins as example
    print(f"\nFirst 12 bins (midnight to 3 AM):")
    for i in range(min(12, len(waffle_data))):
        bin_data = waffle_data[i]
        start_time = bin_data['bin_start'].strftime("%H:%M")
        end_time = bin_data['bin_end'].strftime("%H:%M")
        
        if bin_data['is_sleep']:
            status = "SLEEP"
        elif bin_data['dominant_zone'] is not None:
            status = f"Zone {bin_data['dominant_zone']}"
        else:
            status = "No data"
        
        hr_info = ""
        if bin_data['avg_hr']:
            hr_info = f" (avg: {bin_data['avg_hr']:.0f} bpm)"
        
        print(f"  {start_time}-{end_time}: {status}{hr_info}")

def save_to_database(waffle_data: List[Dict], date_str: str):
    """Save waffle data to database table"""
    engine = connect_to_db()
    
    # First, create table if it doesn't exist
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS chronological_waffle_data (
                id SERIAL PRIMARY KEY,
                date VARCHAR(10) NOT NULL,
                bin_index INTEGER NOT NULL,
                bin_start TIMESTAMP WITH TIME ZONE NOT NULL,
                bin_end TIMESTAMP WITH TIME ZONE NOT NULL,
                grid_row INTEGER NOT NULL,
                grid_col INTEGER NOT NULL,
                is_sleep BOOLEAN NOT NULL,
                hr_readings_count INTEGER DEFAULT 0,
                avg_hr FLOAT,
                min_hr INTEGER,
                max_hr INTEGER,
                dominant_zone INTEGER,
                zone_counts JSON,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE(date, bin_index)
            )
        """))
        conn.commit()
        
        # Delete existing data for this date
        conn.execute(text("""
            DELETE FROM chronological_waffle_data WHERE date = :date_str
        """), {"date_str": date_str})
        conn.commit()
        
        # Insert new data
        for bin_data in waffle_data:
            conn.execute(text("""
                INSERT INTO chronological_waffle_data (
                    date, bin_index, bin_start, bin_end, grid_row, grid_col,
                    is_sleep, hr_readings_count, avg_hr, min_hr, max_hr,
                    dominant_zone, zone_counts
                ) VALUES (
                    :date, :bin_index, :bin_start, :bin_end, :grid_row, :grid_col,
                    :is_sleep, :hr_readings_count, :avg_hr, :min_hr, :max_hr,
                    :dominant_zone, :zone_counts
                )
            """), {
                "date": date_str,
                "bin_index": bin_data['bin_index'],
                "bin_start": bin_data['bin_start'],
                "bin_end": bin_data['bin_end'],
                "grid_row": bin_data['grid_row'],
                "grid_col": bin_data['grid_col'],
                "is_sleep": bin_data['is_sleep'],
                "hr_readings_count": bin_data['hr_readings_count'],
                "avg_hr": bin_data['avg_hr'],
                "min_hr": bin_data['min_hr'],
                "max_hr": bin_data['max_hr'],
                "dominant_zone": bin_data['dominant_zone'],
                "zone_counts": json.dumps(bin_data['zone_counts'])  # Store as proper JSON
            })
        
        conn.commit()
        print(f"\nSaved {len(waffle_data)} waffle data points to database")

def main():
    """Main function with command line arguments"""
    parser = argparse.ArgumentParser(description='Create chronological waffle data from Fitbit HR and sleep data')
    parser.add_argument('--date', '-d', required=True, help='Date to process (YYYY-MM-DD format)')
    parser.add_argument('--timezone', '-tz', default='America/Denver', help='Local timezone (default: America/Denver)')
    parser.add_argument('--bin-minutes', '-b', type=int, default=15, help='Minutes per time bin (default: 15)')
    parser.add_argument('--max-hr', '-m', type=int, default=173, help='Maximum heart rate for zone calculation (default: 173)')
    
    args = parser.parse_args()
    
    try:
        # Validate date format
        datetime.strptime(args.date, "%Y-%m-%d")
        
        # Get timezone
        local_tz = pytz.timezone(args.timezone)
        
        print(f"Processing date: {args.date}")
        print(f"Local timezone: {args.timezone}")
        print(f"Bin size: {args.bin_minutes} minutes")
        print(f"Max HR: {args.max_hr} bpm")
        print("-" * 50)
        
        # Create chronological waffle data
        waffle_data = create_chronological_waffle_data(
            args.date, 
            local_tz, 
            args.bin_minutes, 
            args.max_hr
        )
        
        # Print summary
        print_waffle_summary(waffle_data)
        
        # Save to database
        save_to_database(waffle_data, args.date)
        
        print(f"\n✅ Chronological waffle data created successfully for {args.date}")
        
    except ValueError as e:
        print(f"Error: Invalid date format. Use YYYY-MM-DD")
        return 1
    except pytz.exceptions.UnknownTimeZoneError as e:
        print(f"Error: Unknown timezone '{args.timezone}'. Use format like 'America/Denver' or 'UTC'")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    main()