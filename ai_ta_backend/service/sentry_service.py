import os

import sentry_sdk
from injector import inject


class SentryService:

  @inject
  def __init__(self, dsn: str):
    # Sentry.io error logging
    sentry_sdk.init(
        dsn=os.getenv("SENTRY_DSN"),
        # Set traces_sample_rate to 1.0 to capture 100% of transactions for performance monitoring.
        traces_sample_rate=1.0,
        # Set profiles_sample_rate to 1.0 to profile 100% of sampled transactions.
        # We recommend adjusting this value in production.
        profiles_sample_rate=1.0,
        enable_tracing=True)

  def capture_exception(self, exception: Exception):
    sentry_sdk.capture_exception(exception)
    
