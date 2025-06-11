from datetime import datetime
import logging

from fastapi import FastAPI, Header, HTTPException
import pyodbc

from wrapper_function.models import SessionMetrics
from wrapper_function.config import AZURE_SQL_DATABASE_CONN_STR

app = FastAPI()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.post("/insert-session-metrics")
async def insert_session_metrics(
    session_metrics: SessionMetrics
):
    session_id = session_metrics.session_id
    saved_at = datetime.now()
    user_id = session_metrics.user_id
    username = session_metrics.username
    start_time = session_metrics.start_time
    end_time = session_metrics.end_time
    active_duration = session_metrics.active_duration
    pause_duration = session_metrics.pause_duration
    attention_span = session_metrics.attention_span
    frequency_unfocus = session_metrics.frequency_unfocus
    focus_duration = session_metrics.focus_duration
    unfocus_duration = session_metrics.unfocus_duration
    
    conn = None
    try:
        conn = pyodbc.connect(AZURE_SQL_DATABASE_CONN_STR)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO YourTableName (
                session_id, saved_at, user_id, username, start_time, end_time,
                active_duration, pause_duration, attention_span, frequency_unfocus,
                focus_duration, unfocus_duration
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        cursor.execute(
            insert_query,
            session_id,
            saved_at,
            user_id,
            username,
            start_time,
            end_time,
            active_duration,
            pause_duration,
            attention_span,
            frequency_unfocus,
            focus_duration,
            unfocus_duration
        )

        conn.commit()
        logger.info(f"Data successfully inserted for session_id='{session_id}' and user_id='{user_id}'")

        return {
            "status": "success"
        }
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        if sqlstate == '28000': # Authentication error
            logger.error(f"SQL Authentication Error: {ex}")
            return {
                "status": "error",
                "code": 401,
                "message": f"Authentication failed for SQL Database. Please check your credentials. Error: {ex}"
            }
        else:
            logger.error(f"SQL Database error during insertion: {ex}")
            return {
                "status": "error",
                "code": 500,
                "message": f"Database insertion failed: {ex}"
            }
    except ValueError as ve:
        logger.error(f"Configuration error: {ve}")
        return {
            "status": "error",
            "code": 500,
            "message": f"Server configuration error: {ve}"
        }
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return {
            "status": "error",
            "code": 500,
            "message": f"An unexpected error occurred: {e}"
        }
    finally:
        if conn:
            conn.close() # Ensure the connection is closed


