"""
Drift detector adapted from Signal-Router-Service/drift_detector.py
This version removes file-based state persistence and exposes a programmatic API.
"""
from typing import Dict, Any, Tuple
from datetime import datetime


class DriftDetector:
    def __init__(self, window_size: int = 5):
        self.user_states: Dict[str, Dict[str, Any]] = {}
        self.WINDOW_SIZE = window_size
        self.DRIFT_SIGNALS = {
            "per my last message": (0.7, "Passive-aggressive; implies 'you didn't read my message'."),
            "whatever": (0.84, "Dismissive tone."),
            "i thought": (0.72, "Potential contradiction."),
            "noted": (0.53, "Dismissive acknowledgement."),
            "maybe": (0.4, "Hesitant language."),
            # Add more rules as needed
        }

    def _base_score_message(self, text: str) -> Tuple[float, str]:
        text_lower = (text or "").lower()
        for signal, (score, reason) in self.DRIFT_SIGNALS.items():
            if signal in text_lower:
                return float(score), reason
        return 0.0, "No drift detected"

    def _context_score_message(self, user_id: str, current_text: str, last_score: float, synthetic_hrv: int) -> Tuple[float, str]:
        current_score, reason = self._base_score_message(current_text)

        if current_score > 0.5 and last_score > 0.5:
            current_score = min(1.0, current_score + 0.1)
            reason += " (Pattern of Negative Behavior)"

        is_sudden_shift = last_score < 0.2 and current_score > 0.7
        if is_sudden_shift:
            current_score = 1.0
            reason += " (Sudden Negative Shift)"

        if current_score > 0.4 and synthetic_hrv < 40:
            current_score = min(1.0, current_score + 0.1)
            reason += " (Affected by Low HRV)"

        return round(current_score, 2), reason

    def process(self, user_id: str, text: str, hrv: int) -> Dict[str, Any]:
        if user_id not in self.user_states:
            self.user_states[user_id] = {"recent_scores": [], "last_score": 0.0}

        user_state = self.user_states[user_id]
        drift_score, reason = self._context_score_message(user_id, text, user_state["last_score"], hrv)

        user_state["recent_scores"].append(drift_score)
        if len(user_state["recent_scores"]) > self.WINDOW_SIZE:
            user_state["recent_scores"].pop(0)

        user_state["last_score"] = drift_score

        # Update system coherence for convenience (max average across users)
        highest_avg = 0.0
        for s in self.user_states.values():
            if len(s["recent_scores"]) == self.WINDOW_SIZE:
                avg = sum(s["recent_scores"]) / self.WINDOW_SIZE
                if avg > highest_avg:
                    highest_avg = avg

        system_coherence = round(1.0 - highest_avg, 2)
        signal_tag = "stable" if system_coherence >= 0.8 else ("rising_stress" if system_coherence >= 0.5 else "critical_drift")

        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "user": user_id,
            "text": text,
            "hrv": hrv,
            "individual_drift_score": drift_score,
            "reason": reason,
            "system_coherence_score": system_coherence,
            "system_signal_tag": signal_tag,
        }

        return log_entry
