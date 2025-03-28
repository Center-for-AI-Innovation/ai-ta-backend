import os

from injector import inject
from posthog import Posthog


class PosthogService:

  @inject
  def __init__(self):
    self.posthog = Posthog(
        sync_mode=False,
        project_api_key=os.environ["POSTHOG_API_KEY"],
        host="https://app.posthog.com",
    )

  def capture(self, event_name, properties):
    self.posthog.capture("distinct_id_of_the_user", event=event_name, properties=properties)