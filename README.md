## Milestone 2 – Model Development and Evaluation

**Team 21**

This repository contains the implementation for **Milestone 2** of the IT5006 *Fundamentals of Data Analytics* project.
The goal of this milestone is to build and evaluate machine learning models for **predicting crime hotspots** using historical crime data.

The main analysis and modeling workflow is implemented in a single Jupyter Notebook.

------

# Project Structure

```
project-root/
│
├── data/                 # Dataset files used in the project
│	└── agencies.csv
│	└── chicago_crime_2015_2025.parquet
│	└── NIBRS_incident.csv
│	└── NIBRS_OFFENSE.csv
│	└── NIBRS_OFFENSE_TYPE.csv
│
├── notebooks/            # Jupyter notebooks
│   └── Milestone2_Team21.ipynb       
│
├── requirements.txt      # Python dependencies
│
└── README.md
```

### Notes

- All datasets required to run the notebook are located inside the **`data/`** folder.
- The full analysis workflow is contained in:

```
notebooks/Milestone2_Team21.ipynb
```

The folders `src`, `docs`, and `deployment` are reserved for later milestones and are not used in Milestone 2.

------

# Environment Setup

Before running the notebook, install the required Python libraries.

```bash
pip install -r requirements.txt
```

------

# Running the Notebook

1. Navigate to the project directory.

```
cd project-root
```

1. Start Jupyter Notebook (or Jupyter Lab).

```
jupyter notebook
```

or

```
jupyter lab
```

1. Open the following notebook:

```
notebooks/Milestone2_Team21.ipynb
```

1. Run all cells sequentially.

The notebook performs the following steps:

- Data loading from the **data/** directory
- Data preprocessing and feature engineering
- Training multiple machine learning models
- Model evaluation using metrics such as:
  - Precision
  - Recall
  - F1-score
  - ROC-AUC
- Robustness testing and cross-validation

------

# Data Requirement

All required datasets are already placed in the **`data/`** folder.

The notebook automatically loads data from this directory, so no additional configuration is required.

------

# Output

Running the notebook will generate:

- Model training results
- Evaluation metrics
- Confusion matrices
- Performance comparisons between models

All outputs are displayed directly inside the notebook.

