import os
import json
import shutil
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

load_dotenv()


client = Minio(
    os.environ['MINIO_ENDPOINT'],
    access_key=os.environ['MINIO_ACCESS_KEY'],
    secret_key=os.environ['MINIO_SECRET_KEY'],
    secure=False,
)

bucket_name = "clinical-trials"

if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)

input_dir = "/projects/uiucchat/ctg-data/clinical_trials_data"
output_dir = "/projects/uiucchat/minio-to-qdrant/output"
os.makedirs(output_dir, exist_ok=True)

def extract_relevant_text(data):
    ps = data.get("protocolSection", {})
    lines = []

    def get(*keys, default=""):
        d = ps
        for key in keys:
            d = d.get(key, {})
        if isinstance(d, str):
            return d
        elif isinstance(d, dict):
            return d.get("text", default)
        return default

    lines.append(f"Official Title: {get('identificationModule', 'officialTitle')}")
    lines.append(f"Detailed Description: {get('descriptionModule', 'detailedDescription')}")

    status = ps.get("statusModule", {})
    start_date = status.get("startDateStruct", {}).get("date")
    primary_completion = status.get("primaryCompletionDateStruct", {}).get("date")
    completion = status.get("completionDateStruct", {}).get("date")

    if start_date:
        lines.append(f"Start Date: {start_date}")
    if primary_completion:
        lines.append(f"Primary Completion Date: {primary_completion}")
    if completion:
        lines.append(f"Study Completion Date: {completion}")

    party = ps.get("sponsorCollaboratorsModule", {}).get("responsibleParty", {})
    investigator = party.get("investigatorFullName")
    title = party.get("investigatorTitle")
    affiliation = party.get("investigatorAffiliation")

    if investigator:
        lines.append(f"Investigator: {investigator}")
    if title:
        lines.append(f"Title: {title}")
    if affiliation:
        lines.append(f"Affiliation: {affiliation}")

    conditions = ps.get("conditionsModule", {}).get("conditions", [])
    if conditions:
        lines.append(f"Conditions: {', '.join(conditions)}")

    study_type = ps.get("designModule", {}).get("studyType")
    if study_type:
        lines.append(f"Study Type: {study_type}")

    phases = ps.get("designModule", {}).get("phases", [])
    if phases:
        lines.append(f"Phase(s): {', '.join(phases)}")

    interventions = ps.get("armsInterventionsModule", {}).get("interventions", [])
    if interventions:
        names = [i.get("name") for i in interventions if "name" in i]
        lines.append(f"Interventions: {', '.join(names)}")

    outcomes = ps.get("outcomesModule", {}).get("primaryOutcomes", [])
    if outcomes:
        lines.append("Primary Outcomes:")
        for outcome in outcomes:
            measure = outcome.get("measure", "").strip()
            description = outcome.get("description", "").strip()
            time_frame = outcome.get("timeFrame", "").strip()
            line = f"- {measure}"
            if time_frame:
                line += f" ({time_frame})"
            if description:
                line += f": {description}"
            lines.append(line)

    return "\n".join([line for line in lines if line.strip() and "None" not in line])

for filename in os.listdir(input_dir):
    if filename.endswith(".json"):
        input_path = os.path.join(input_dir, filename)
        txt_output_path = os.path.join(output_dir, f"{os.path.splitext(filename)[0]}.txt")
        json_output_path = os.path.join(output_dir, filename)

        with open(input_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        relevant_text = extract_relevant_text(data)
        with open(txt_output_path, "w", encoding="utf-8") as out_f:
            out_f.write(relevant_text)

        shutil.copy(input_path, json_output_path)

        print(f"Saved cleaned text to: {txt_output_path}")
        print(f"Copied original JSON to: {json_output_path}")

        MAX_FILES_PER_FOLDER = 10_000
        all_objects = list(client.list_objects(bucket_name, recursive=True))
        current_file_count = len(all_objects)

        batch_index = current_file_count // MAX_FILES_PER_FOLDER + 1
        batch_folder = f"batch_{batch_index:04d}"

        object_name = f"{batch_folder}/{os.path.basename(txt_output_path)}"

        client.fput_object(
            bucket_name,
            object_name,
            txt_output_path,
            content_type="text/plain"
        )

        print(f"Uploaded to MinIO: {bucket_name}/{object_name}")
        break
