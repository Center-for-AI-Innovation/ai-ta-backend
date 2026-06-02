import base64
import hashlib
import json
import os
import re

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


def encrypt(text: str, key: str) -> str:
  if not text or not key:
    print('Error encrypting because text or key is not available', text, key)
    raise ValueError(f'Invalid input: Error encrypting because text or key is not available. Text: {text}, Key: {key}')

  pw_utf8 = key.encode('utf-8')
  pw_hash = hashlib.sha256(pw_utf8).digest()
  iv = os.urandom(12)
  cipher = Cipher(algorithms.AES(pw_hash), modes.GCM(iv), backend=default_backend())
  encryptor = cipher.encryptor()
  encrypted = encryptor.update(text.encode('utf-8')) + encryptor.finalize()
  encrypted_base64 = base64.b64encode(encrypted + encryptor.tag).decode('utf-8')
  iv_base64 = base64.b64encode(iv).decode('utf-8')
  version = 'v1'
  return f"{version}.{encrypted_base64}.{iv_base64}"


def decrypt(encrypted_text: str, key: str) -> str:
  if not encrypted_text or not key:
    print('Error decrypting because encryptedText or key is not available', encrypted_text, key)
    raise ValueError(
        f'Invalid input: Error decrypting because encryptedText or key is not available. Encrypted text: {encrypted_text}, Key: {key}'
    )

  try:
    version, encrypted_base64, iv_base64 = encrypted_text.split('.')
    if not version or not encrypted_base64 or not iv_base64:
      raise ValueError('Invalid encrypted text format')
    if version != 'v1':
      raise ValueError(f'Unsupported encryption version: {version}')

    pw_utf8 = key.encode('utf-8')
    pw_hash = hashlib.sha256(pw_utf8).digest()
    iv = base64.b64decode(iv_base64)
    encrypted = base64.b64decode(encrypted_base64)
    tag = encrypted[-16:]
    ciphertext = encrypted[:-16]

    cipher = Cipher(algorithms.AES(pw_hash), modes.GCM(iv, tag), backend=default_backend())
    decryptor = cipher.decryptor()
    decrypted = decryptor.update(ciphertext) + decryptor.finalize()
    return decrypted.decode('utf-8')
  except Exception as error:
    raise ValueError('Failed to decrypt data: ' + str(error))


def is_encrypted(s: str) -> bool:
  if not s:
    return False
  parts = s.split('.')
  if len(parts) != 3:
    return False
  version, encrypted_base64, iv_base64 = parts
  if version != 'v1':
    return False
  base64_regex = r'^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{4})$'
  return (re.match(base64_regex, encrypted_base64) is not None and re.match(base64_regex, iv_base64) is not None)


def decrypt_if_needed(key: str) -> str:
  if key and is_encrypted(key):
    try:
      decrypted_text = decrypt(key, os.environ.get('NEXT_PUBLIC_SIGNING_KEY', ''))
      return decrypted_text
    except Exception as error:
      print('Failed to decrypt key:', error)
      raise
  return key


def encrypt_if_needed(key: str) -> str:
  if key and not is_encrypted(key):
    try:
      encrypted_text = encrypt(key, os.environ.get('NEXT_PUBLIC_SIGNING_KEY', ''))
      return encrypted_text
    except Exception as error:
      print('Failed to encrypt key:', error)
      raise
  return key


def _get_connection_encryption_key() -> str:
  key = os.environ.get('ENCRYPTION_MASTER_KEY', '')
  if not key:
    raise ValueError('ENCRYPTION_MASTER_KEY environment variable is not set')
  return key


def decrypt_config(stored: dict) -> dict:
  """Decrypt a config dict from project_external_connections.

  This is the only entry point the backend needs into the
  project-connection encryption envelope — the Next.js frontend is the
  sole writer (see uiuc-chat-frontend
  src/pages/api/UIUC-api/projectConnections*).

  Expects {"encrypted": "v1.xxx.yyy"} as stored in the JSONB column.
  Returns the original config dict.
  """
  if not stored:
    return None
  encrypted_str = stored.get("encrypted")
  if not encrypted_str:
    return None
  plaintext = decrypt(encrypted_str, _get_connection_encryption_key())
  return json.loads(plaintext)
