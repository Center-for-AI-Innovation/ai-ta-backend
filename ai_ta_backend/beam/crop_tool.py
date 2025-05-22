"""
To deploy: beam deploy crop_tool.py
For testing: beam serve crop_tool.py
Use CAII gmail to auth.
"""

import re
from typing import Any, Dict

import beam
import requests
from beam import App, QueueDepthAutoscaler, Runtime  # RequestLatencyAutoscaler,

# python script to send a POST request to GDD url endpoint and gett the response
from bs4 import BeautifulSoup

requirements = ["beautifulsoup4==4.12.3"]

app = App(
    "crop_tool",
    runtime=Runtime(
        cpu=1,
        memory="2Gi",
        image=beam.Image(
            python_version="python3.10",
            python_packages=requirements,
            # commands=["apt-get update && apt-get install -y ffmpeg tesseract-ocr"],
        ),
    ))

# Set up the GDD URL
url = 'https://warm.isws.illinois.edu/warm/cropdata/calcresult.asp'

# Set up the headers
headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Referer': 'https://warm.isws.illinois.edu/warm/cropdata/cropddcalc.asp'
}


# Set up the dynamic payload
def payload(base, placebox, areabox, date, plap):
  return {'base': str(base), 'placebox': str(placebox), 'areabox': str(areabox), 'date': str(date), 'plap': str(plap)}


# Parse the response using regex
def parse_response(response, pattern):
  soup = BeautifulSoup(response.text, 'html.parser')
  soup_text = soup.get_text()
  match = re.search(pattern, soup_text, re.DOTALL)
  if match:
    return match.group()
  else:
    return None


# Send the POST request to the GDD URL
def send_request(payload, pattern):
  try:
    response = requests.post(url, data=payload, headers=headers, timeout=20)
    if response.status_code == 200:
      return parse_response(response, pattern)
  except requests.exceptions.RequestException as e:
    print(f'Error in sending request: {e}')
    # log.error(f'Error in sending request: {e}')
    return None


autoscaler = QueueDepthAutoscaler(max_tasks_per_replica=2, max_replicas=3)


# @app.task_queue(
@app.rest_api(
    # workers=1,
    # callback_url is used for 'in progress' & 'failed' tracking. But already handeled by other Beam endpoint.
    # callback_url='https://uiuc-chat-git-ingestprogresstracking-kastanday.vercel.app/api/UIUC-api/ingestTaskCallback',
    max_pending_tasks=1_000,
    max_retries=3,
    timeout=60,
    # loader=loader,
    autoscaler=autoscaler)
def main(**inputs: Dict[str, Any]):
  base = inputs.get('base', '')
  placebox = inputs.get('placebox', '')
  areabox = inputs.get('areabox', '')
  date = inputs.get('date', '')
  plap = inputs.get('plap', '')
  print(base, placebox, areabox, date, plap)

  # Set up the payload
  payload_data = payload(base, placebox, areabox, date, plap)
  # log.info(f'Payload data: {payload_data}')
  print(f'Payload data: {payload_data}')
  if base == 50:
    pattern = r"Corn growing\s+degree-days\s+at\s+{}\..*?Two-week:\s+\d+".format(plap)
  else:
    pattern = r"Crop growing\s+degree-days\s+\(base\s+40° F\)\s+at\s+{}\..*?Two-week:\s+\d+".format(plap)
  # Send the request
  response = send_request(payload_data, pattern)
  print(f"response {response}")
  if response:
    return response
  else:
    print('Error in getting response')
    return 'Error in getting response, got None'
