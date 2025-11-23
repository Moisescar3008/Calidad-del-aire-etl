# ETL Project: Urban Air Quality Analysis

## 📋 Table of Contents

1.  [Phase 1: Problem Justification](https://www.google.com/search?q=%23phase-1-problem-justification)
2.  [Phase 2: ETL Pipeline with Airflow](https://www.google.com/search?q=%23phase-2-etl-pipeline-with-airflow)
3.  [Phase 3: Dashboard and Visualizations](https://www.google.com/search?q=%23phase-3-dashboard-and-visualizations)
4.  [Installation Instructions](https://www.google.com/search?q=%23installation-instructions)
5.  [Project Execution](https://www.google.com/search?q=%23project-execution)

-----

## Phase 1: Problem Justification

### 🌍 Relevance

Air quality is a critical public health issue that directly affects millions of people in urban areas. According to the World Health Organization (WHO), air pollution causes approximately 7 million premature deaths globally each year.

### 🎯 Problem to Solve

By analyzing historical air quality data (PM2.5, PM10, NO2, O3, CO), we can:

  * **Identify temporal patterns** of pollution (peak hours, critical days)
  * **Detect high-risk zones** requiring urgent intervention
  * **Correlate** human activities with pollution levels
  * **Predict** periods of poor air quality

### 👥 Beneficiaries

1.  **Municipal Authorities**: To implement data-driven transportation policies, vehicular restrictions, and urban planning.

2.  **Vulnerable Citizens**: Children, the elderly, and people with respiratory problems can plan outdoor activities during safer hours.

3.  **Public Health System**: To prepare medical resources during anticipated pollution peaks.

4.  **Urban Planners**: To design more sustainable cities with better natural ventilation and strategically located green areas.

5.  **Environmental Organizations**: To monitor compliance with regulations and push for policy changes.

-----

## Phase 2: ETL Pipeline with Airflow

### 🏗️ Pipeline Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   EXTRACT   │────▶│  TRANSFORM  │────▶│    LOAD     │────▶│  VALIDATE   │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

### ✅ Implemented Components

#### 1\. **EXTRACT (Data Extraction)**

  * **Source**: Generation of realistic synthetic data simulating air quality sensors
  * **Volume**: 90 days of hourly data × 5 stations = \~10,800 records
  * **Monitored Parameters**:
      * PM2.5 (Fine Particulate Matter)
      * PM10 (Coarse Particulate Matter)
      * NO2 (Nitrogen Dioxide)
      * O3 (Ozone)
      * CO (Carbon Monoxide)
      * Temperature
      * Humidity
  * **Characteristics**:
      * Variation by hour of the day (peaks during traffic hours)
      * Variation by day of the week (less pollution on weekends)
      * Simulation of missing values (2% missing data)

#### 2\. **TRANSFORM (Transformation and Cleaning)**

**Data Cleaning:**

  * Data type conversion (timestamp to datetime)
  * Imputation of missing values using linear interpolation
  * Outlier removal using the IQR (Interquartile Range) method
  * Duplicate record elimination

**Feature Engineering:**

  * **AQI (Air Quality Index)**: Calculation of the EPA index for PM2.5
  * **Quality Categories**: Good, Moderate, Unhealthy for Sensitive, Unhealthy, Very Unhealthy
  * **Temporal Features**:
      * Hour of the day
      * Day of the week
      * Is weekend
      * Is peak hour (7-9 am, 5-7 pm)
      * Month
  * **Compound Pollution Index**: Normalized metric combining multiple pollutants

**Aggregations:**

  * Daily summaries per station (averages, maximums, minimums)
  * Count of peak hours per day
  * Weekly and monthly statistics

#### 3\. **LOAD (Data Loading)**

**Storage Options:**

1.  **Local Files** (Implemented):

      * CSV for universal compatibility
      * Parquet for efficiency and compression
      * Location: `/tmp/air_quality_output/`

2.  **PostgreSQL** (Prepared - commented out):

      * `air_quality_hourly` table for hourly data
      * `air_quality_daily` table for daily aggregations
      * Loading in 1000-record chunks for efficiency

#### 4\. **VALIDATE (Quality Validation)**

  * Verification of minimum data volume
  * Calculation of the percentage of null values
  * Date range validation
  * Verification of unique stations
  * Calculation of quality metrics

### ⚙️ DAG Configuration

```python
Schedule: Daily at 8:00 AM
Retries: 3 attempts
Retry Delay: 5 minutes
Start Date: 2024-01-01
Catchup: False (do not run past dates)
```

### 🛡️ Error Handling

1.  **Try-Except in each function**: Catch and log specific exceptions
2.  **Retry Configuration**: 3 automatic retries with a 5-minute wait
3.  **Extensive Logging**: Detailed logging of each operation
4.  **Data Validations**: Post-load quality check
5.  **Notifications**: Email in case of failure (configurable)

### 📊 Scaling Considerations

1.  **Chunk Processing**:

      * Reading and writing data in blocks of 10,000 records
      * Reduces memory usage for large datasets

2.  **Parquet Format**:

      * Snappy compression
      * Up to 80% less space than CSV
      * Efficient columnar reading

3.  **Early Filtering**:

      * Elimination of irrelevant data in the extraction phase
      * Reduces load on later stages

4.  **Parallel Tasks** (Prepared for expansion):

      * Multiple stations can be processed in parallel
      * Independent aggregations by zone

5.  **Database Connection**:

      * Use of SQLAlchemy for abstraction
      * Connection pooling for multiple workers

### 📅 Scheduling

**Current Schedule**: `0 8 * * *` (8:00 AM daily)

**Justification**:

  * Air quality sensors typically report data from the previous day
  * Morning execution allows analyses to be available for same-day decisions
  * Scheduled outside of server traffic peak hours

**Alternatives by Usage Type**:

  * **Real-Time**: `*/15 * * * *` (every 15 minutes)
  * **Weekly**: `0 8 * * 1` (Monday 8 AM)
  * **Monthly**: `0 8 1 * *` (First day of the month)

-----

## Fase 3: Dashboard and Visualizations

### 📊 Dashboard Components

#### **KPIs (Key Performance Indicators)**

1.  **Average AQI (24h)**

      * Large numeric value with color code
      * Status indicator: GOOD / MODERATE / ALERT
      * Updated with the last 24 hours of data

2.  **Percentage of Days with Good Quality**

      * Historical trend metric
      * Shows numerator and denominator (X of Y days)
      * Target: \>80% for acceptable quality

3.  **Critical Zone**

      * Station with the highest average AQI
      * AQI value in red
      * Allows resource prioritization

#### **Visualizations**

### 1\. **Temporal Evolution of PM2.5** (Line Chart)

**Purpose**: Identify trends, seasonal cycles, and anomalous events

**Why this chart**:

  * Time series are ideal for continuously evolving data
  * Allows comparison of multiple stations simultaneously
  * Reference lines (12 and 35.4 μg/m³) show EPA thresholds
  * Facilitates pattern and anomaly detection

**Insights Provided**:

  * Is air quality improving or worsening?
  * Which stations are consistently the most polluted?
  * Are there specific high-pollution events?

### 2\. **Distribution of Quality Categories** (Pie Chart)

**Purpose**: Visualize the proportion of time spent in each quality level

**Why this chart**:

  * Proportions are best communicated with circular charts
  * Standard EPA colors (green, yellow, orange, red, purple) are recognizable
  * Allows for a quick assessment of the general situation

**Insights Provided**:

  * What percentage of the time is the air breathable?
  * How often are there hazardous conditions?
  * Is compliance with regulations (\>80% good days) met?

### 3\. **Hourly Pollution Pattern** (Bar Chart)

**Purpose**: Identify critical hours for activity planning

**Why this chart**:

  * Bars facilitate comparison between 24 categories
  * Color coding highlights peak hours (7-9 am, 5-7 pm)
  * Clear pattern of vehicular traffic

**Insights Provided**:

  * When is it safe to exercise outdoors?
  * What hours require vehicular restriction?
  * Are current traffic measures working?

### 4\. **Comparison Between Stations** (Grouped Bar Chart)

**Purpose**: Compare multiple pollutants across locations

**Why this chart**:

  * Grouped bars allow for multi-dimensional comparison
  * Facilitates the identification of zones requiring intervention
  * Shows whether pollution is uniform or localized

**Insights Provided**:

  * Which zones need urgent measures?
  * Are there differences in pollutants by zone?
  * Where to prioritize additional monitoring stations?

### 🎨 Design Decisions

**Color Palette**:

  * Based on EPA standards for international consistency
  * High contrast for accessibility
  * Differentiated colors by data type

**Typography**:

  * KPIs in large size (60pt) for quick reading
  * Titles in bold for visual hierarchy
  * Clear labels with units of measure

**Layout**:

  * 3×3 grid structure for logical organization
  * KPIs at the top for immediate visibility
  * Charts ordered by importance

### 💡 Dashboard Impact

**Real-World Problem Resolution**:

1.  **For Authorities**:

      * Objective data to justify investments in public transport
      * Identification of optimal times for vehicular restriction
      * Prioritization of zones for implementing green areas

2.  **For Citizens**:

      * Clear information to decide when to engage in physical activity
      * Visibility of improvements or deterioration in their area
      * Empowerment to demand changes

3.  **For Public Health**:

      * Prediction of days with high demand for respiratory care
      * Focusing preventive campaigns in critical zones
      * Evaluation of the impact of interventions

4.  **Success Measurement**:

      * If \>80% of days are "Good," policies are working
      * If peak hours show reduced pollution, public transport is improving
      * If differences between stations decrease, interventions are equitable

-----

## Installation Instructions

### 📦 Prerequisites

```bash
Python 3.8+
Apache Airflow 2.5+
PostgreSQL 13+ (optional)
```

### 🔧 Step-by-Step Installation

#### 1\. Clone or Create the Project

```bash
mkdir air_quality_etl_project
cd air_quality_etl_project
```

#### 2\. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

#### 3\. Install Dependencies

```bash
# Core dependencies
pip install apache-airflow==2.5.0
pip install pandas numpy matplotlib seaborn
pip install psycopg2-binary sqlalchemy
pip install requests

# Install Airflow providers
pip install apache-airflow-providers-postgres
```

#### 4\. Initialize Airflow

```bash
# Configure Airflow directory
export AIRFLOW_HOME=~/airflow

# Initialize Airflow database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

#### 5\. Configure the DAG

```bash
# Copy the DAG to the Airflow folder
cp air_quality_etl_dag.py ~/airflow/dags/

# Create necessary directories
mkdir -p /tmp/air_quality_output
```

#### 6\. Configure PostgreSQL (Optional)

```bash
# Create database
createdb air_quality_db

# In Airflow UI, add connection:
# Conn Id: postgres_default
# Conn Type: Postgres
# Host: localhost
# Schema: air_quality_db
# Login: your_user
# Password: your_password
# Port: 5432
```

-----

## Project Execution

### 🚀 Start Airflow

#### Terminal 1: Scheduler

```bash
airflow scheduler
```

#### Terminal 2: Webserver

```bash
airflow webserver --port 8080
```

### 🌐 Access Airflow UI

```
URL: http://localhost:8080
User: admin
Password: admin
```

### ▶️ Run the Pipeline

#### Option 1: From the UI

1.  Go to http://localhost:8080
2.  Find the DAG `air_quality_etl_pipeline`
3.  Turn the toggle on (ON)
4.  Click on "Trigger DAG" (▶️ button)

#### Option 2: From CLI

```bash
# Run once
airflow dags trigger air_quality_etl_pipeline

# Run for a specific date range
airflow dags backfill air_quality_etl_pipeline \
    --start-date 2024-01-01 \
    --end-date 2024-01-31
```

### 📊 Generate Dashboard

```bash
# Once the DAG has successfully completed
python air_quality_dashboard.py
```

The dashboard will be generated in: `/tmp/air_quality_dashboard.png`

### 🔍 Verify Results

```bash
# View DAG logs
airflow tasks logs air_quality_etl_pipeline extract_data <date>

# Verify generated files
ls -lh /tmp/air_quality_output/

# View processed data
head /tmp/air_quality_output/air_quality_final.csv
```

### 📈 Monitoring

#### In Airflow UI:

  * **DAGs**: Execution status
  * **Graph**: Task flow
  * **Tree**: Execution history
  * **Logs**: Details of each task

#### Key Metrics:

  * **Success Rate**: \>95% expected
  * **Duration**: \~2-5 minutes per execution
  * **Data Quality**: \<1% null values

-----

## 🏆 Project Highlights

### ✅ Requirements Met

| Requirement | Status | Details |
| :--- | :--- | :--- |
| Extract | ✅ | Generation of realistic synthetic data |
| Transform | ✅ | Complete cleaning + 8 new features |
| Load | ✅ | CSV + Parquet + PostgreSQL ready |
| Scheduling | ✅ | Daily at 8 AM |
| Error Handling | ✅ | Try-catch + Retries + Logging |
| Scaling | ✅ | Chunks + Parquet + Parallel-ready |
| Notifications | ✅ | Detailed logging + Email config |
| Dashboard | ✅ | 3 KPIs + 4 charts |
| Justification | ✅ | Clear problem with beneficiaries |

### 🌟 Additional Improvements Implemented

1.  **Data Quality Validation**: Dedicated post-load task
2.  **Advanced Feature Engineering**: AQI, categories, compound indices
3.  **Multiple Output Formats**: CSV (compatibility) + Parquet (efficiency)
4.  **Extensive Documentation**: Commented code + complete README
5.  **Realistic Data**: Simulation of real pollution patterns
6.  **Professional Visualizations**: Production-ready dashboard

-----

## 📚 References and Resources

### Air Quality Standards

  * [EPA Air Quality Index](https://www.airnow.gov/aqi/)
  * [WHO Air Quality Guidelines](https://www.who.int/news-room/fact-sheets/detail/ambient-\(outdoor\)-air-quality-and-health)

### Technologies Used

  * [Apache Airflow Documentation](https://airflow.apache.org/docs/)
  * [Pandas Documentation](https://pandas.pydata.org/docs/)
  * [Matplotlib Documentation](https://matplotlib.org/stable/contents.html)

### Real Datasets (for expansion)

  * [OpenAQ](https://openaq.org/) - Public air quality API
  * [EPA AirData](https://www.epa.gov/outdoor-air-quality-data) - Historical USA data
  * [IQAir](https://www.iqair.com/world-air-quality) - Global monitoring
