from pydantic import BaseModel

class SessionMetrics(BaseModel):
    session_id: str
    user_id: str
    username: str
    start_time: str
    end_time: str
    active_duration: float
    pause_duration: float
    attention_span: float
    frequency_unfocus: int
    focus_duration: float
    unfocus_duration: float