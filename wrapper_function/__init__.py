from datetime import datetime, timedelta
import logging

from fastapi import FastAPI
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
    """Insert session metrics into the Azure SQL Database."""

    # Extract data from the payload
    session_id = session_metrics.session_id
    saved_at = datetime.now()
    user_id = session_metrics.user_id
    username = session_metrics.username
    start_time = datetime.strptime(session_metrics.start_time, "%Y-%m-%d %H:%M:%S.%f")
    end_time = datetime.strptime(session_metrics.end_time, "%Y-%m-%d %H:%M:%S.%f")
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
            INSERT INTO session_metrics (
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
        # Ensure the connection is closed
        if conn:
            conn.close() 


@app.get("/weekly-top5-attention-span")
async def get_weekly_top5_attention_span():
    """Get the top 5 users with the highest average attention span for the current week."""

    conn = None
    try:
        conn = pyodbc.connect(AZURE_SQL_DATABASE_CONN_STR)
        cursor = conn.cursor()

        query = """
            SELECT TOP 5 
                user_id,
                username,
                AVG(CAST(attention_span AS FLOAT)) as avg_attention_span,
                COUNT(*) as session_count
            FROM session_metrics 
            WHERE start_time >= (DATEADD(week, DATEDIFF(week, 0, GETDATE()), 0) - 1)
            AND start_time < (DATEADD(week, DATEDIFF(week, 0, GETDATE()) + 1, 0) - 1)
            GROUP BY user_id, username
            HAVING COUNT(*) > 0
            ORDER BY avg_attention_span DESC
        """

        cursor.execute(query)
        results = cursor.fetchall()

        # Convert results to list of dictionaries
        top5_users = []
        for row in results:
            top5_users.append({
                "user_id": row[0],
                "username": row[1],
                "avg_attention_span": round(float(row[2]), 1),
            })

        logger.info(f"Successfully retrieved weekly top 5 attention span data. Found {len(top5_users)} users.")

        # Week starts on Sunday and ends on Saturday
        today = datetime.now()
        days_since_sunday = (today.weekday() + 1) % 7 # Add 1 to adjust for Sunday as the first day of the week
        sunday = today - timedelta(days=days_since_sunday)
        saturday = sunday + timedelta(days=6)

        return {
            "status": "success",
            "data": {
                "week_period": {
                    "start": sunday.strftime("%Y-%m-%d"),
                    "end": saturday.strftime("%Y-%m-%d")
                },
                "top5_users": top5_users
            }
        }

    except pyodbc.Error as e:
        sqlstate = e.args[0]
        if sqlstate == '28000':  # Authentication error
            logger.error(f"SQL Authentication Error: {e}")
            return {
                "status": "error",
                "code": 401,
                "message": f"Authentication failed for SQL Database. Please check your credentials. Error: {e}"
            }
        else:
            logger.error(f"SQL Database error during query: {e}")
            return {
                "status": "error",
                "code": 500,
                "message": f"Database query failed: {e}"
            }
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return {
            "status": "error",
            "code": 500,
            "message": f"Server configuration error: {e}"
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
            conn.close()  # Ensure the connection is closed