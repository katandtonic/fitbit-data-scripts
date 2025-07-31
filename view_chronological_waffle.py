#!/usr/bin/env python3
"""
View chronological waffle data from database
"""

from sqlalchemy import create_engine, text
from datetime import datetime

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

def view_waffle_data(date_str: str):
    """View chronological waffle data for a given date"""
    engine = connect_to_db()
    
    with engine.connect() as conn:
        # Get waffle data
        result = conn.execute(text("""
            SELECT bin_index, bin_start, bin_end, grid_row, grid_col,
                   is_sleep, hr_readings_count, avg_hr, dominant_zone
            FROM chronological_waffle_data
            WHERE date = :date_str
            ORDER BY bin_index
        """), {"date_str": date_str})
        
        waffle_data = result.fetchall()
        
        if not waffle_data:
            print(f"No waffle data found for {date_str}")
            return
        
        print(f"Chronological Waffle Data for {date_str}")
        print("="*80)
        
        # Print as 8x12 grid (96 bins)
        for row in range(8):
            print(f"\nRow {row} ({row*3:02d}:00 - {(row+1)*3:02d}:00):")
            row_display = ""
            for col in range(12):
                bin_index = row * 12 + col
                if bin_index < len(waffle_data):
                    bin_data = waffle_data[bin_index]
                    is_sleep, hr_count, avg_hr, zone = bin_data[5], bin_data[6], bin_data[7], bin_data[8]
                    
                    if is_sleep:
                        symbol = "💤"  # Sleep
                    elif zone is None:
                        symbol = "⬜"  # No data
                    else:
                        # Zone symbols
                        zone_symbols = {1: "🟦", 2: "🟩", 3: "🟨", 4: "🟧", 5: "🟥", 6: "🟪"}
                        symbol = zone_symbols.get(zone, "⬛")
                    
                    row_display += symbol
                else:
                    row_display += "⬜"
            
            print(f"  {row_display}")
        
        # Print legend
        print(f"\nLegend:")
        print(f"💤 Sleep")
        print(f"⬜ No data")
        print(f"🟦 Zone 1 (Rest/Recovery)")
        print(f"🟩 Zone 2 (Fat Burn)")
        print(f"🟨 Zone 3 (Light Cardio)")
        print(f"🟧 Zone 4 (Moderate Cardio)")
        print(f"🟥 Zone 5 (Hard Cardio)")
        print(f"🟪 Zone 6 (Peak)")
        
        # Summary stats
        sleep_bins = sum(1 for row in waffle_data if row[5])  # is_sleep
        no_data_bins = sum(1 for row in waffle_data if row[6] == 0)  # hr_readings_count
        awake_with_data = len(waffle_data) - sleep_bins - no_data_bins
        
        print(f"\nSummary:")
        print(f"Total bins: {len(waffle_data)}")
        print(f"Sleep bins: {sleep_bins}")
        print(f"No data bins: {no_data_bins}")
        print(f"Awake bins with HR data: {awake_with_data}")
        
        # Zone distribution
        zone_counts = {}
        for row in waffle_data:
            if not row[5] and row[8] is not None:  # not sleep and has zone
                zone = row[8]
                zone_counts[zone] = zone_counts.get(zone, 0) + 1
        
        if zone_counts:
            print(f"\nZone distribution (awake bins):")
            for zone in sorted(zone_counts.keys()):
                print(f"  Zone {zone}: {zone_counts[zone]} bins ({zone_counts[zone]/len(waffle_data)*100:.1f}%)")

def view_detailed_timeline(date_str: str):
    """View detailed timeline showing transitions"""
    engine = connect_to_db()
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT bin_start, bin_end, is_sleep, avg_hr, dominant_zone, hr_readings_count
            FROM chronological_waffle_data
            WHERE date = :date_str
            ORDER BY bin_index
        """), {"date_str": date_str})
        
        waffle_data = result.fetchall()
        
        print(f"\nDetailed Timeline for {date_str}:")
        print("-" * 80)
        
        current_state = None
        state_start = None
        
        for row in waffle_data:
            bin_start, bin_end, is_sleep, avg_hr, zone, hr_count = row
            
            if is_sleep:
                state = "SLEEP"
            elif zone is None:
                state = "NO_DATA"
            else:
                state = f"ZONE_{zone}"
            
            # Print transitions
            if current_state != state:
                if current_state is not None:
                    print(f"{state_start.strftime('%H:%M')} - {bin_start.strftime('%H:%M')}: {current_state}")
                current_state = state
                state_start = bin_start
        
        # Print final state
        if current_state is not None:
            print(f"{state_start.strftime('%H:%M')} - {waffle_data[-1][1].strftime('%H:%M')}: {current_state}")

def main():
    """Main function"""
    date_str = "2025-07-29"
    
    print("Viewing chronological waffle data...")
    view_waffle_data(date_str)
    view_detailed_timeline(date_str)

if __name__ == "__main__":
    main()