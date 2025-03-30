#  Data Engineering Case Study – EY / Fabernovel

**Author:** Abdellah Laarif  
  


---

##  1. Objective

This case study demonstrates a full data engineering workflow involving:

- Data ingestion from an external API to Google Cloud Storage  
- Construction of a star schema in BigQuery from raw web analytics data  
- Analytical querying to answer key business questions using SQL  
- Code and infrastructure versioning using GitHub  

---

##  2. Technologies Used

- **Python**: Data ingestion and preprocessing  
- **Google Cloud Storage (GCS)**: Raw data storage (Part 1)  
- **Google BigQuery**: Data warehouse and transformation layer  
- **Jupyter Notebook**: Analytical exploration  
- **SQL**: Star schema modeling and insights extraction  
- **Git/GitHub**: Version control and documentation  

---

##  3. Project Structure

ey-fabernovel-case-study/

│

├──  fetch_products_to_gcs.py         # Python script to extract Fake Store API data

│

├── sql/

│       └─── star_schema.sql                  # SQL scripts to create fact/dimension tables

├── notebooks/

│           └── churn_analysis.ipynb             # Exploratory notebook for churn analysis

│

├── EY Fabernovel – Data Engineering Case Study.pdf  # Final report

└── README.md


---

##  4. Data Ingestion (Part 1)

- **Source**: Fake Store API  
- **Target**: Google Cloud Storage  
- **Format**: JSON (stored as blobs in GCS)  
- **Tools**: `requests`, `google-cloud-storage`, `os`  

The Python script extracts product data and uploads it into a GCS bucket for later consumption.

---

##  5. Star Schema Design (Part 2)

**Source**: `bigquery-public-data.google_analytics_sample`

### Fact Table
- `fact_sessions`: Session-level metrics including pageviews, hits, revenue, device type, and traffic source.

### Dimension Tables

| Table               | Description                                 |
|--------------------|---------------------------------------------|
| `dim_users`         | User geographic details                     |
| `dim_device`        | Device and browser information              |
| `dim_traffic`       | Source, medium, and channel grouping        |
| `dim_date`          | Date details (year, month, day of week)     |
| `session_products`  | Bridge between sessions and products        |

All transformations use SQL in BigQuery with appropriate joins, parsing, and unnesting.

---

##  6. Business Questions

The following questions were explored using SQL:

1. **Customer Behavior**  
   - Which countries, regions, and cities are most active?

2. **Product Performance**  
   - What are the top-selling products and categories?

3. **Traffic Source Effectiveness**  
   - Which sources and mediums drive the most conversions?

4. **User Engagement Segmentation**  
   - How do session metrics vary by engagement type?

5. **Churn Rate Analysis**  
   - What is the churn rate per month? How does retention evolve?

---

##  7. Churn Analysis Notebook

The `churn_analysis.ipynb` notebook includes:

- Direct connection to BigQuery using the Python client  
- User segmentation (new vs returning)  
- Monthly active users (MAU)  
- Monthly churn and retention metrics  
- Visualizations using `matplotlib` and `pandas`

---

##  8. Conclusion

This project showcases an end-to-end data engineering pipeline using real-world data.  
It demonstrates strong skills in:

- Cloud-based data pipelines  
- SQL data modeling with star schemas  
- Analytical querying and visualization  
- Clean code, reproducibility, and documentation

---

