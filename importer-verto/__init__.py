import datetime
import logging
import pyodbc
import os
import requests
import json

import azure.functions as func

ORG_ID = 1
DATA_URL = "https://vaccineto.ca/api/slots"
LOCATION_MAP = {
    "RPV": 1,
    "WCC": 2,
    "RUV": 3,
    "SMV": 4,
    "SJV": 5,
    "WPA": 6,
    "CPH": 7
}

ELIGIBILITY_MAP = {
    "Special": 1,
    "High": 2,
    "HealthComm": 3,
    "Communities": 4,
    "IndigenousAdults": 5,
    "Caregivers": 6
}

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    logging.info('DB Connectiong String: %s', os.environ.get("DB_URL"))
    conn = pyodbc.connect(os.environ.get("DB_URL"))
    cursor = conn.cursor()
    logging.info('DB Connected successfully!')

    logging.info('Reading from %s', DATA_URL)
    verto = requests.get(DATA_URL)
    logging.info('Response Code: %d', verto.status_code)

    data = verto.json()
    for location_code in data['data'].keys():
        location_id = LOCATION_MAP[location_code]
        availabilities = data['data'][location_code]['availabilities']
        for eligibility in availabilities.keys():
            eligibility_id = ELIGIBILITY_MAP[eligibility]
            for day in availabilities[eligibility]:
                available = availabilities[eligibility][day]
                existing_check = cursor.execute(
                    "SELECT [vaccine_availability].* FROM [vaccine_availability] JOIN [vaccine_availability_requirements] ON [vaccine_availability].[id] = [vaccine_availability_requirements].[vaccine_availability] WHERE [vaccine_availability].[location] = {} AND [vaccine_availability].[date] = '{}' AND [vaccine_availability_requirements].[requirement] = {}".format(
                        location_id,
                        day,
                        eligibility_id
                    )
                )

                existing = existing_check.fetchone()
                if existing is not None:
                    logging.info('Updating {} for {} to {}'.format(location_code, day, available))
                    cursor.execute(
                        "UPDATE [vaccine_availability] SET numberAvailable = {}, numberTotal = {}, created_at = CURRENT_TIMESTAMP WHERE id = '{}'".format(
                            available, available, existing.id
                        )
                    )
                else:
                    sql = """
                    SET NOCOUNT ON
                    DECLARE @VaxId uniqueidentifier
                    SET @VaxId = newid()
                    INSERT INTO [vaccine_availability] (id, numberAvailable, numberTotal, date, location, vaccine, inputType, created_at) VALUES (@VaxId, {}, {}, '{}', {}, 1, 1, CURRENT_TIMESTAMP)
                    INSERT INTO [vaccine_availability_requirements] (id, vaccine_availability, requirement, active, created_at) VALUES (newid(), @VaxId, {}, 1, CURRENT_TIMESTAMP)
                    """.format(
                        available, available, day, location_id, eligibility_id
                    )
                    logging.info('Adding {} for {} = {}'.format(location_code, day, available))
                    cursor.execute(sql)
                
                conn.commit()
    
    conn.close()

