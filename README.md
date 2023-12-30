## MRJob Library for Frequent Pattern Mining with Hadoop Streaming and Python

### Overview

Welcome to the repository for the MRJob library, designed for Hadoop streaming with Python to perform Frequent Pattern Mining. This code is particularly useful for analyzing the Madrid Airbnb dataset, which can be downloaded [here](https://www.kaggle.com/rusiano/madrid-airbnb-data?select=listings_detailed.csv). Ensure you save the downloaded file in the same folder as the code.

### Prerequisites

Before you start, make sure you have the following dependencies installed:

- [Pandas](https://pandas.pydata.org/)
- [MRJob](https://pythonhosted.org/mrjob/)

Use the provided requirements file for assistance in setting up the required environment.

### How to Run

To execute the code, use the following command:

```bash
(python or python3) start.py (inline or hadoop) (support e.g., 0.7) (passes k e.g., 5) (confidence e.g., 0.93)
```

For example, on Windows:

```bash
python start.py inline 0.7 5 0.93
```