# PySpark Big Data Challenge - Week 2 🚀

This project is part of a series of technical challenges aimed at strengthening PySpark and Big Data skills. In Week 2, the focus was on cleaning, transforming, aggregating, and partitioning a dataset using PySpark.

## 📝 Challenge Description

The dataset for this challenge contains sales records, including information about orders, customers, and regions. The goals for this challenge were:

1. **Data Cleaning:**
   - Convert date columns (`Order Date`, `Ship Date`) into valid `DateType`.
   - Fix date inconsistencies where days or months were represented with a single digit.

2. **Data Transformation:**
   - Extract the year from `Order Date` and create a new column `Year`.
   - Standardize numerical columns (`Sales`, `Profit`, `Discount`, and `Quantity`) to `FloatType`.

3. **Aggregation:**
   - Group data by `Year` and `Region` and calculate:
     - Average sales.
     - Total sales.
     - Average profit margin.

4. **Partitioning:**
   - Save the aggregated results in Parquet format, partitioned by `Year` and `Region`.

---

## 📁 Dataset

- **Source:** [Superstore Dataset (Kaggle)](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final)
- **Fields Used:**
  - `Order Date`, `Ship Date`, `Region`, `Sales`, `Profit`, `Discount`, `Quantity`

---

## 🛠️ Steps Performed

### 1. Data Loading
The dataset was loaded into a PySpark DataFrame.

### 2. Data Cleaning
- Single-digit dates in `Order Date` and `Ship Date` were padded with leading zeros using PySpark's string manipulation functions.
- Columns were converted to appropriate data types:
  - Date columns: `DateType`
  - Numerical columns: `FloatType`

### 3. Data Transformation
- Extracted the year from `Order Date` to create a new column `Year`.

### 4. Aggregation
Calculated the following metrics:
- **Average Sales**: Per `Year` and `Region`.
- **Total Sales**: Per `Year` and `Region`.
- **Average Profit Margin**: Derived as `Profit / Sales`.

### 5. Partitioned Writing
Saved the aggregated results to a Parquet file partitioned by `Year` and `Region`:
```python
df_agg.write.mode("overwrite").format("parquet").partitionBy(["Year", "Region"]).save("results.parquet")