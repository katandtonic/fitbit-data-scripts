# Fitbit Data Scripts

## Database Schema

### Chronological Waffle Data Table

**Table Name:** `chronological_waffle_data`

```sql
CREATE TABLE chronological_waffle_data (
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
);
```

## Scripts

### Core Database Scripts

- **`connect_db.py`**: Connects to PostgreSQL database and inspects available tables and data structure.
  ```bash
  python connect_db.py
  ```

- **`examine_sleep_simple.py`**: Examines sleep-related database tables (sleep_sessions, sleep_stages, heart_rate_readings) to understand data structure.
  ```bash
  python examine_sleep_simple.py
  ```

### Chronological Waffle Data Processing

- **`chronological_waffle_munger.py`**: Main data munging script that processes raw HR and sleep data into 96 time bins (8×12 grid) for chronological waffle visualization.
  ```bash
  python chronological_waffle_munger.py --date 2025-07-29 --timezone America/Denver
  python chronological_waffle_munger.py -d 2025-07-30 -tz UTC --bin-minutes 20 --max-hr 180
  ```

- **`view_chronological_waffle.py`**: Displays chronological waffle data as emoji grid and detailed timeline showing when different HR zones occurred.
  ```bash
  python view_chronological_waffle.py
  ```

- **`query_waffle_data.py`**: Executes SELECT query on chronological_waffle_data table and displays all raw data in tabular format.
  ```bash
  python query_waffle_data.py
  ```

### Test Scripts

- **`test_july29.py`**: Test script that runs chronowaffle generation on July 29, 2025 data with mock fallbacks.
  ```bash
  python test_july29.py
  ```

## Setup

1. Activate virtual environment: `source .venv/bin/activate`
2. Install dependencies: `uv add sqlalchemy psycopg2-binary python-dotenv pytz`
3. Ensure `.env` file contains database connection parameters
4. Run scripts as needed

## Data Flow

1. Raw heart rate data (`heart_rate_readings`) + sleep data (`sleep_sessions`, `sleep_stages`)
2. → `chronological_waffle_munger.py` processes into time bins
3. → Stores results in `chronological_waffle_data` table
4. → Ready for frontend chronological waffle visualization