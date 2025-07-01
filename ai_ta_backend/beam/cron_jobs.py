"""
CRON job functions to update conversation and document maps in Nomic. 
Run twice a day.
"""

# Call /updateConversationMap and /updateDocumentMap endpoints in Nomic

import requests
from beam import schedule


@schedule(when="@daily", name="update-nomic-maps")
def task():
  print("Hi, from your task running at midnight daily!")
  base_url = "https://flask-production-751b.up.railway.app"

  # Update conversation maps
  url = f"{base_url}/updateConversationMaps"
  try:
    response = requests.get(url, timeout=30)
    if response.status_code == 200:
      print("Conversation maps updated successfully")
    else:
      print(f"Failed to update conversation maps - Status code: {response.status_code}")
      print(f"Response text: {response.text}")
  except Exception as e:
    print(f"Error updating conversation maps: {str(e)}")

  # Update document maps
  url = f"{base_url}/updateDocumentMaps"
  try:
    response = requests.get(url, timeout=30)
    if response.status_code == 200:
      print("Document maps updated successfully")
    else:
      print(f"Failed to update document maps - Status code: {response.status_code}")
      print(f"Response text: {response.text}")
  except Exception as e:
    print(f"Error updating document maps: {str(e)}")

  return "Task completed successfully"
