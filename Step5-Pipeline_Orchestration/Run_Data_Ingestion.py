import sys
import logging
from Tracker import Tracker

def track_data_ingestion():
    tracker = Tracker("Data_Ingestion")
    job_id = tracker.assign_job_id()
    connection = tracker.get_db_connection()
    try:
        # In addition, create methods to assign job_id and get db connection.
        tracker.data_ingestion()
        tracker.update_job_status("Successful Data Ingestion.", job_id, connection)
    except Exception as e:
        print(e)
        tracker.update_job_status("Failed Data Ingestion.", job_id, connection)
    return


if __name__ == "__main__":
    track_data_ingestion()