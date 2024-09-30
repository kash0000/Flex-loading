def handle_nan_values(row):
    """Converts NaN values to appropriate types before inserting."""
    return [("" if pd.isna(value) else value) for value in row]  # Converts NaN to empty string

for chunk in reader:
    data_chunk = [handle_nan_values(parse_dates(row)) for row in chunk.itertuples(index=False, name=None)]

/////////////////////////////////////////////////
for row in data_chunk:
    print([type(value) for value in row])  # Log types of each value in the row
///////////////////////////////////////////
2nd

from datetime import datetime
import pandas as pd

def parse_dates(row):
    """
    Handles date parsing for specific columns without changing formats unnecessarily.
    Also handles converting specific date formats to datetime objects.
    """
    def convert_date(value, date_format):
        """
        Converts a string date value to a datetime object based on the provided format.
        """
        if pd.notna(value):  # Check if the value is not NaN
            if isinstance(value, str):
                return datetime.strptime(value, date_format)
            elif isinstance(value, datetime):
                return value
        return None  # Return None if the value is NaN or an invalid date

    # Convert the tuple to a list to allow modification
    row = list(row)

    # Example handling for specific date columns
    if pd.not
