import os
from app.processing.data_batching import send_data_to_kafka
from app.processing.data_preparation import prepare_dataframe, prepare_data_for_mongo
from app.utils.files_utils import get_file_path
from dotenv import load_dotenv

load_dotenv(verbose=True)

db_topic = os.environ["DB_TOPIC"]


def produce_csv():
    file1 = get_file_path("../assets/globalterrorismdb_0718dist.csv")
    file2 = get_file_path("../assets/RAND_Database_of_Worldwide_Terrorism_Incidents.csv")

    merged_df = prepare_dataframe(file1, file2)
    mongo_data = prepare_data_for_mongo(merged_df)
    send_data_to_kafka(mongo_data, db_topic,"terror_event")