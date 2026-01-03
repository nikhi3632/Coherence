"""
Biometric helpers adapted from Signal-Router-Service/biometric.py
This module provides HRV fusion helpers used by the coherence scoring pipeline.
No filesystem writes or secrets.
"""
import random
from typing import Union


def get_hrv(base_drift_score: Union[float, int]) -> int:
    """
    Generate a synthetic HRV (ms) based on a base drift score.
    Higher drift reduces HRV; function is deterministic-enough for tests but
    includes small randomness to simulate measurement noise.

    Args:
        base_drift_score: float in [0, 1], higher means more linguistic drift
    Returns:
        int: HRV in milliseconds
    """
    baseline_hrv = 65
    drift_impact = float(base_drift_score) * 30
    random_noise = random.uniform(-5, 5)
    final_hrv = baseline_hrv - drift_impact + random_noise
    return int(final_hrv)
