import os
import sys
import json
import pytest
from fastapi.testclient import TestClient

# Ensure tests can import the Implementation package
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from cryptography.fernet import Fernet

# Ensure ENCRYPTION_KEY is set for the tests with a valid Fernet key
os.environ.setdefault("ENCRYPTION_KEY", Fernet.generate_key().decode())

from Implementation.router.app import app


client = TestClient(app)


def stub_analyzer_response(monkeypatch, status_code=200, body=None):
    class DummyResp:
        def __init__(self, status_code, body):
            self.status_code = status_code
            self._body = body or {}

        def json(self):
            return self._body

    async def dummy_post(url, json=None):
        return DummyResp(status_code, body)

    # Patch AsyncClient.post used in router.app
    monkeypatch.setattr("httpx.AsyncClient.post", lambda self, url, json=None: dummy_post(url, json=json))



@pytest.mark.parametrize("classification,expected_agents", [
    ("assist", ["Axis"]),
    ("policy", ["M"]),
    ("emergency", ["M", "Axis"]),
])
def test_synthesis_mapping(monkeypatch, classification, expected_agents):
    body = {"classification": classification, "coherence_score": 0.9}
    stub_analyzer_response(monkeypatch, 200, body)

    resp = client.post("/ingest", json={"tenant_id": "t1", "payload": {"text": "test"}})
    assert resp.status_code == 200
    j = resp.json()
    assert j["status"] == "routed"
    assert j["routed_agents"] == expected_agents
    assert "message_id" in j


def test_low_score_goes_to_dlq(monkeypatch, tmp_path):
    body = {"classification": "assist", "coherence_score": 0.1}
    stub_analyzer_response(monkeypatch, 200, body)

    resp = client.post("/ingest", json={"tenant_id": "t1", "payload": {"text": "maybe stressed"}})
    assert resp.status_code == 200
    j = resp.json()
    assert j["status"] == "dlq_stored"
    assert "dlq_path" in j
    # file should exist
    assert os.path.exists(j["dlq_path"]) is True

def test_unknown_classification_goes_to_dlq(monkeypatch):
    body = {"classification": "unknown", "coherence_score": 0.9}
    stub_analyzer_response(monkeypatch, 200, body)

    resp = client.post("/ingest", json={"tenant_id": "t1", "payload": {"text": "unknown class"}})
    assert resp.status_code == 200
    j = resp.json()
    assert j["status"] == "dlq_stored"
    assert "dlq_path" in j


def test_analyzer_unavailable(monkeypatch):
    async def raise_exc(url, json=None):
        raise Exception("connection error")

    monkeypatch.setattr("httpx.AsyncClient.post", lambda self, url, json=None: raise_exc(url, json=json))

    resp = client.post("/ingest", json={"tenant_id": "t1", "payload": {"text": "hi"}})
    assert resp.status_code == 503


def test_validation_missing_tenant():
    resp = client.post("/ingest", json={"payload": {"text": "hi"}})
    # Pydantic validation returns 422 for missing required fields during parsing
    assert resp.status_code == 422
