# UIUC.chat ingest and retrieval

A Flask application hosting endpoints for UIUC.chat. 

# [Start with the docs here](https://docs.uiuc.chat)

## 👉 [Developer quickstart here](https://docs.uiuc.chat/developers/developer-quickstart)

### 🛠️ Technical Architecture

Hosted (mostly for free) on [Railway](https://railway.app/).
Architecture diagram of Flask + Next.js & React hosted on Vercel.
![Architecture diagram](https://github.com/UIUC-Chatbot/ai-ta-backend/assets/13607221/bda7b4d6-79ce-4d12-bf8f-cff9207c37af)

## Documentation

* **Extensive usage docs on [UIUC.chat](docs.uiuc.chat/)**

## 🏎️ Quickstart 

1. Rename `.env.template` to `.env` and fill in the required variables
2. Install Python requirements `pip install -r requirements.txt`
3. Start the server for development (with live reloads) `cd ai_ta_backend` then `flask --app ai_ta_backend.main:app --debug run --port 8000`

## 📣 Development
Install the Trunk "superlinter" so your commits are formatted. Just one step:

Mac: brew install trunk-io
Linux: curl https://get.trunk.io -fsSL | bash

### Course metadata structure

```text
'text': doc.page_content,
'readable_filename': doc.metadata['readable_filename'],
'course_name ': doc.metadata['course_name'],
's3_path': doc.metadata['s3_path'],
'pagenumber': doc.metadata['pagenumber_or_timestamp'], # this is the recent breaking change!!
# OPTIONAL properties
'url': doc.metadata.get('url'), # wouldn't this error out?
'base_url': doc.metadata.get('base_url'),
```
