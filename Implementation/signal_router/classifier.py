# Adapted from Signal-Router-Service/classifier.py
# Purpose: classify signals into emotional/physiological/mixed/invalid
from typing import Optional
from pydantic import BaseModel


class SignalPayload(BaseModel):
    user_id: Optional[str] = None
    text: Optional[str] = None
    hrv: Optional[dict] = None


class SignalClassifier:
    def classify(self, data: SignalPayload) -> str:
        # If we have both text and HRV, classify as mixed_signal
        if data.text and data.hrv is not None:
            return "mixed_signal"

        # If we have text, classify as emotional_signal
        if data.text:
            return "emotional_signal"

        # If we have HRV, classify as physiological_signal
        if data.hrv is not None:
            return "physiological_signal"

        return "invalid_signal"
