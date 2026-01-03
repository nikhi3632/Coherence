import os
from cryptography.fernet import Fernet

# Ensure ENCRYPTION_KEY is set for encryption helper
os.environ.setdefault('ENCRYPTION_KEY', Fernet.generate_key().decode())

from Implementation.signal_router.api import AnalyzeRequest, analyze
from Implementation.utils.canonical_hash import generate_canonical_message_id
from Implementation.utils.encryption import encrypt_payload, decrypt_payload


def main():
    req = AnalyzeRequest(
        tenant_id='t1',
        event_id='evt1',
        user_id='user1',
        payload_version=1,
        ts='2025-01-01T00:00:00Z',
        text='I feel fine but maybe stressed',
        hrv={'hrv_ms': 35}
    )

    res = analyze(req)
    print('ANALYZE RESPONSE:', res)

    payload = {'text': 'I feel fine but maybe stressed', 'hrv': {'hrv_ms': 35}}
    mid = generate_canonical_message_id('t1', 'evt1', 'user1', '2025-01-01T00:00:00Z', 1, payload)
    print('CANONICAL MESSAGE ID:', mid)

    enc = encrypt_payload({'message_id': mid, 'payload': payload})
    print('ENCRYPTED (len):', len(enc))
    dec = decrypt_payload(enc)
    print('DECRYPTED:', dec)

if __name__ == '__main__':
    main()
