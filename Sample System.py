import unittest
import pandas as pd
from sqlalchemy import create_engine
import sqlite3

# Step 1: Extract Data from Source Database
def extract_data(source_conn_str):
    engine = create_engine(source_conn_str)
    query = "SELECT * FROM source_table"
    df = pd.read_sql(query, engine)
    return df

# Step 2: Data Mapping and Transformation
def map_transform_data(df):
    df = df.rename(columns={
        'source_col1': 'target_col1',
        'source_col2': 'target_col2'
    })
    df['target_col1'] = df['target_col1'].astype(int)
    df['target_col2'] = pd.to_datetime(df['target_col2'])
    return df

# Step 3: Data Cleansing
def cleanse_data(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['target_col1', 'target_col2'])
    df['target_col2'] = df['target_col2'].dt.strftime('%Y-%m-%d')
    return df

# Step 4: Load Data into Data Warehouse
def load_data(df, warehouse_conn_str):
    engine = create_engine(warehouse_conn_str)
    df.to_sql('warehouse_table', engine, if_exists='replace', index=False)

# Step 5: Business Intelligence Report
def generate_bi_report(warehouse_conn_str):
    engine = create_engine(warehouse_conn_str)
    query = """
    SELECT target_col1, COUNT(*) as count, AVG(target_col2) as avg_date
    FROM warehouse_table
    GROUP BY target_col1
    """
    df = pd.read_sql(query, engine)
    return df

# Main function to run all steps
def main():
    source_conn_str = 'sqlite:///source.db'  # Replace with actual source DB connection string
    warehouse_conn_str = 'sqlite:///warehouse.db'  # Replace with actual warehouse DB connection string

    # Extract data
    df = extract_data(source_conn_str)
    
    # Map and transform data
    df = map_transform_data(df)
    
    # Cleanse data
    df = cleanse_data(df)
    
    # Load data into data warehouse
    load_data(df, warehouse_conn_str)
    
    # Generate BI report
    report_df = generate_bi_report(warehouse_conn_str)
    print(report_df)

if __name__ == "__main__":
    main()

# Unit tests
class TestDataPipeline(unittest.TestCase):
    def setUp(self):
        self.source_data = pd.DataFrame({
            'source_col1': [1, 2, 2, None],
            'source_col2': ['2023-01-01', '2023-01-02', '2023-01-02', '2023-01-03']
        })
        self.mapped_data = pd.DataFrame({
            'target_col1': [1, 2, 2, None],
            'target_col2': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-02', '2023-01-03'])
        })
        self.cleaned_data = pd.DataFrame({
            'target_col1': [1, 2],
            'target_col2': ['2023-01-01', '2023-01-02']
        })
        self.warehouse_conn_str = 'sqlite:///test_warehouse.db'

    def test_extract_data(self):
        with sqlite3.connect('source.db') as conn:
            self.source_data.to_sql('source_table', conn, if_exists='replace', index=False)
        extracted_df = extract_data('sqlite:///source.db')
        pd.testing.assert_frame_equal(extracted_df, self.source_data)

    def test_map_transform_data(self):
        transformed_df = map_transform_data(self.source_data)
        pd.testing.assert_frame_equal(transformed_df, self.mapped_data)

    def test_cleanse_data(self):
        cleaned_df = cleanse_data(self.mapped_data)
        pd.testing.assert_frame_equal(cleaned_df, self.cleaned_data)

    def test_load_data(self):
        load_data(self.cleaned_data, self.warehouse_conn_str)
        engine = create_engine(self.warehouse_conn_str)
        loaded_df = pd.read_sql('SELECT * FROM warehouse_table', engine)
        pd.testing.assert_frame_equal(loaded_df, self.cleaned_data)

    def test_generate_bi_report(self):
        load_data(self.cleaned_data, self.warehouse_conn_str)
        report_df = generate_bi_report(self.warehouse_conn_str)
        expected_report_df = pd.DataFrame({
            'target_col1': [1, 2],
            'count': [1, 1],
            'avg_date': ['2023-01-01', '2023-01-02']
        })
        pd.testing.assert_frame_equal(report_df, expected_report_df)

if __name__ == "__main__":
    unittest.main()
