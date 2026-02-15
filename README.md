# IT5006 -- Milestone 1

## Chicago Crime Analysis & Interactive Dashboard

### Project Overview

This project analyzes Chicago crime data (2015--2024) and builds an
interactive dashboard using Streamlit.

The project contains:

-   Exploratory Data Analysis (EDA)
-   Data preprocessing and aggregation
-   Interactive visualization dashboard
-   Reproducible environment setup

------------------------------------------------------------------------

# Repository Structure

```python
IT5006-Milestone1/
│
├── Chicago Crime.ipynb         # EDA & data exploration
├── prepare_agg.py              # Preprocessing & aggregation script
├── app.py                      # Streamlit dashboard
├── requirements.txt            # Python dependencies
└── README.md
```



------------------------------------------------------------------------

# Environment Setup

## 1. Clone Repository

```bash
git clone https://github.com/your-username/IT5006-Milestone1.git
cd IT5006-Milestone1
```

## 2. Create Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate      # Linux / macOS
.venv\Scripts\activate         # Windows
```

## 3. Install Dependencies

```bash
pip install -r requirements.txt
```



------------------------------------------------------------------------

# Execution Order (Important)

The scripts must be run in the following order:

------------------------------------------------------------------------

## Step 1 --- Exploratory Data Analysis

File: `Chicago Crime.ipynb`

Purpose:

- Load raw crime dataset
- Perform cleaning
- Explore temporal and spatial patterns
- Generate processed parquet files

Output:

```bash
data/processed/crime_2015.parquet
data/processed/crime_2016.parquet
...\
data/processed/crime_2024.parquet
```

This notebook performs:

- Date parsing
- Feature selection
- Basic statistical summaries
- Spatial visualization
- Monthly grouping exploration

------------------------------------------------------------------------

## Step 2 --- Data Aggregation

File: `prepare_agg.py`

Purpose:

- Load yearly processed parquet files
- Generate aggregated datasets for dashboard
- Reduce memory usage for deployment

Run:

```python
python prepare_agg.py
```

Output:

```bash
data/agg/monthly_total.parquet
data/agg/monthly_type.parquet
data/agg/sample_points.parquet
```

Descriptions:

| File          | Description                                             |
| ------------- | ------------------------------------------------------- |
| monthly_total | Total monthly crime count                               |
| monthly_type  | Monthly crime count by primary type                     |
| sample_points | Sampled latitude/longitude points for map visualization |

------------------------------------------------------------------------

## Step 3 --- Launch Dashboard

File: `app.py`

Purpose:

- Interactive EDA dashboard
- Filter by date range
- Filter by crime type
- View spatial density map
- View monthly trends
- View Top 10 crime categories

Run locally:

```bash
streamlit run app.py
```

Run on server:

```bash
streamlit run app.py --server.port 8501 --server.address 0.0.0.0
```

And of course you can access to the dashboard via link `milestoneone.hoshinostudio.cn` without deploy `app.py` above :)



------------------------------------------------------------------------

# Dashboard Features

1.  Sidebar Filters
    -   Date range selection
    -   Crime type selection
2.  Summary Metrics
    -   Total records (filtered)
    -   Unique crime types
    -   Selected date range
3.  Spatial Analysis
    -   Density Mapbox visualization
    -   Sampled geolocation data for performance
4.  Temporal Analysis
    -   Monthly crime trend line chart
5.  Crime Type Distribution
    -   Top 10 crime types (filtered)

------------------------------------------------------------------------

# Technologies Used

-   Python
-   Pandas
-   Streamlit
-   Plotly
-   PyArrow
-   Jupyter Notebook

------------------------------------------------------------------------

# Author

Team21\
LIN YIHAN\
XU QIAOYANG\
QI RUIXUAN

------------------------------------------------------------------------

# Reproducibility

The project is fully reproducible:

1.  Install dependencies
2.  Run notebook
3.  Run prepare_agg.py
4.  Launch Streamlit

No external database required.
