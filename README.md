# Pacific Retail Data Pipeline with Snowflake

This project implements a data pipeline for Pacific Retail using Snowflake, structured into three layers: Bronze, Silver, and Gold. Each layer represents a stage in the data processing workflow, ensuring data quality and usability.

## Project Structure
![image](https://github.com/user-attachments/assets/90f9ca82-8724-469b-ae57-75e51c7d9750)
                                  
- **Bronze_Layer**: Contains raw data ingested from various sources without transformation.

   **Prerequisites**
  
    - Create_DB_Bronze_Schema.sql, External_Stage_Creation.sql, and  Stream_Creation.sql must be executed first
      before Customer_Load.sql, Product_Load.sql and Orders_Load.sql.
  
   The brief description of each script:
    
    **1. Create_DB_Bronze_Schema.sql**: This creates the database and schema for the Bronze Layer to store raw data.​
      
    **2. External_Stage_Creation.sql**: Sets up external stages to facilitate data loading from external storage into Snowflake.​
      
    **3. Stream_Creation.sql**: Creates streams to capture changes in the Bronze Layer tables for downstream processing.
      
    **4. Customer_Load.sql**: loads raw customer data into the Bronze Layer tables.​
      
    **5. Product_Load.sql**: loads raw product data into the Bronze Layer tables.​
      
    **6. Orders_Load.sql**: Loads raw orders data into the Bronze Layer tables.​
  
- **Silver_Layer**: Holds data that has undergone cleaning and transformation processes to ensure consistency and accuracy.
  
    **Prerequisites**
  
  Before running any scripts in this layer, ensure the following:

  - Create_Silver_Tables.sql must be executed first to set up the schema for the Gold Layer.
  - Customer_Transform.sql, Product_Transform.sql and  Orders_Transform.sql depend on tables/views created in Create_Silver_Tables.sql.
 
    The brief description of each script:
    
      **1. Create_Silver_Tables.sql**: Creates tables in the Silver Layer to store transformed data.​
      
      **2. Customer_Transform.sql**: Processes and cleanses customer data from the Bronze Layer, then loads it into the Silver Layer.​
      
      **3. Product_Transform.sql**: Processes and cleanses product data from the Bronze Layer, then loads it into the Silver Layer.​
      
      **4. Orders_Transform.sql**: Processes and cleanses orders data from the Bronze Layer, then loads it into the Silver Layer.
      
- **Gold_Layer**: Includes aggregated and enriched data, optimized for analytics and reporting purposes.
  
  **Prerequisites**
  
  Before running any scripts in this layer, ensure the following:

  - Create_Schema.sql must be executed first to set up the schema for the Gold Layer.
  - GoldLayer_View1.sql and GoldLayer_View2.sql depend on tables/views created in Create_Schema.sql.
  - 
    The brief description of each script:
    
      **1. Create_Schema.sql**: Creates the schema for the Gold Layer to store aggregated data.​
      
      **2. GoldLayer_View1.sql**: Creates a view that combines and aggregates data from the Silver Layer for specific analytical purposes.​
      
      **3. GoldLayer_View2.sql**: Creates another view with different aggregations or perspectives on the data to support various reporting needs.​
  
## PACIFICRETAIL_DB Structure in Snowflake

![image](https://github.com/user-attachments/assets/eff1f769-fc9d-4742-8468-7ca3c513de38)


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
