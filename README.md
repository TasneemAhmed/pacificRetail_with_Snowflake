# Pacific Retail Data Pipeline with Snowflake

This project implements a data pipeline for Pacific Retail using Snowflake, structured into three layers: Bronze, Silver, and Gold. Each layer represents a stage in the data processing workflow, ensuring data quality and usability.

## Project Structure

- **Bronze_Layer**: Contains raw data ingested from various sources without transformation.
- **Silver_Layer**: Holds data that has undergone cleaning and transformation processes to ensure consistency and accuracy.
- **Gold_Layer**: Includes aggregated and enriched data, optimized for analytics and reporting purposes.
  
![image](https://github.com/user-attachments/assets/90f9ca82-8724-469b-ae57-75e51c7d9750)

## Getting Started

To set up and run this project:

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/TasneemAhmed/pacificRetail_with_Snowflake.git
   ```

2. **Navigate to the Project Directory**:

   ```bash
   cd pacificRetail_with_Snowflake
   ```

3. **Set Up Snowflake Environment**:

   - Ensure you have access to a Snowflake account.
   - Configure your Snowflake connection settings as required.

4. **Load Data into the Bronze Layer**:

   - Use the scripts provided in the `Bronze_Layer` directory to ingest raw data into Snowflake.

5. **Process Data into the Silver Layer**:

   - Execute the transformation scripts in the `Silver_Layer` directory to clean and standardize the data.

6. **Aggregate Data into the Gold Layer**:

   - Run the aggregation scripts located in the `Gold_Layer` directory to prepare data for analysis.

---
