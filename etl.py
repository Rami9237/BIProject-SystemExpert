import clickhouse_driver
import csv
from datetime import datetime

def upload_csv_to_clickhouse(csv_file, clickhouse_config):
    """
    Reads a CSV file and uploads data to ClickHouse.

    :param csv_file: Path to the CSV file.
    :param clickhouse_config: Dictionary with ClickHouse connection details.
    """
    try:
        # Connect to ClickHouse
        client = clickhouse_driver.Client(**clickhouse_config)

        # Create the table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS cars (
            car_id UInt32,
            timestamp DateTime,
            gear String,
            accelerator_pressed UInt8,
            indicator_left_on UInt8,
            indicator_right_on UInt8,
            traffic_light String,
            intersection UInt8,
            other_vehicle_approaching UInt8,
            pedestrian_crossing UInt8,
            pedestrian_present UInt8,
            current_speed Float32,
            speed_limit Float32,
            merge_lane_clear UInt8,
            engine_off UInt8,
            PRIMARY KEY (car_id, timestamp)
        ) ENGINE = MergeTree()
        ORDER BY (car_id, timestamp)
        """
        client.execute(create_table_query)
        print("Table 'cars' has been created or already exists.")

        # Read the CSV file and prepare rows for insertion
        with open(csv_file, 'r') as file:
            reader = csv.DictReader(file)
            rows = []
            for row in reader:
                # Validate and convert row data
                try:
                    timestamp = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S')  # Validate DateTime
                    rows.append((
                        int(row['car_id']),
                        timestamp.strftime('%Y-%m-%d %H:%M:%S'),  # Format as string for ClickHouse
                        row['gear'],
                        max(0, int(row['accelerator_pressed'])),
                        max(0, int(row['indicator_left_on'])),
                        max(0, int(row['indicator_right_on'])),
                        row['traffic_light'],
                        max(0, int(row['intersection'])),
                        max(0, int(row['other_vehicle_approaching'])),
                        max(0, int(row['pedestrian_crossing'])),
                        max(0, int(row['pedestrian_present'])),
                        float(row['current_speed']),
                        float(row['speed_limit']),
                        max(0, int(row['merge_lane_clear'])),
                        max(0, int(row['engine_off'])),
                    ))
                except (ValueError, KeyError) as e:
                    print(f"Skipping invalid row: {row}. Error: {e}")

        # Insert data into the table
        if rows:
            insert_query = """
            INSERT INTO cars (
                car_id, timestamp, gear, accelerator_pressed, indicator_left_on, 
                indicator_right_on, traffic_light, intersection, other_vehicle_approaching, 
                pedestrian_crossing, pedestrian_present, current_speed, speed_limit, 
                merge_lane_clear, engine_off
            ) VALUES
            """
            client.execute(insert_query, rows)
            print(f"{len(rows)} rows have been inserted into the 'cars' table.")
        else:
            print("No valid rows to insert.")

    except Exception as e:
        print(f"An error occurred: {e}")
