"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

from config import KSQL_URL
from consumers import topic_check
from topics import TURNSTILE_SUMMARY_TABLE_NAME, TURNSTILE_TOPIC_NAME

logger = logging.getLogger(__name__)

KSQL_STATEMENT = f"""
CREATE STREAM turnstile_stream (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC='{TURNSTILE_TOPIC_NAME}',
    VALUE_FORMAT='AVRO'
);

CREATE TABLE {TURNSTILE_SUMMARY_TABLE_NAME}
    WITH (KAFKA_TOPIC='{TURNSTILE_SUMMARY_TABLE_NAME}')
    AS SELECT station_id, count(*) as count FROM turnstile_stream GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists(TURNSTILE_SUMMARY_TABLE_NAME) is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
