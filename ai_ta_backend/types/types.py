import datetime
from typing import Any, Dict, List, Optional

import pydantic


class DocumentMetadata(pydantic.BaseModel):
  authors: list[str]
  journal_name: str
  publication_date: datetime.date  # Changed from datetime.date to str
  keywords: list[str]
  doi: str
  title: str
  subtitle: Optional[str]
  visible_urls: list[str]
  field_of_science: str
  concise_summary: str
  specific_questions_document_can_answer: list[str]
  additional_fields: Optional[Dict[str, Any]] = {}


class ClerkUser(pydantic.BaseModel):
  backup_code_enabled: bool
  banned: bool
  create_organization_enabled: bool
  created_at: int
  delete_self_enabled: bool
  email_addresses: List[Dict[str, Any]]
  external_accounts: List[Dict[str, Any]]
  external_id: Optional[str]
  first_name: Optional[str]
  has_image: bool
  id: str
  image_url: str
  last_active_at: int
  last_name: Optional[str]
  last_sign_in_at: int
  locked: bool
  lockout_expires_in_seconds: Optional[int]
  object: str
  passkeys: List
  password_enabled: bool
  phone_numbers: List
  primary_email_address_id: Optional[str]
  primary_phone_number_id: Optional[str]
  primary_web3_wallet_id: Optional[str]
  private_metadata: Dict[str, Any]
  profile_image_url: str
  public_metadata: Dict[str, Any]
  saml_accounts: List
  totp_enabled: bool
  two_factor_enabled: bool
  unsafe_metadata: Dict[str, Any]
  updated_at: int
  username: Optional[str]
  verification_attempts_remaining: int
  web3_wallets: List
