#get rid off non-necessary metadata
import os
import json

input_dir = "/projects/uiucchat/ctg-data/clinical_trials_data"
output_dir = os.path.join(input_dir, "text_output")

os.makedirs(output_dir, exist_ok=True)

def format_json(y, indent=0):
    lines = []
    indent_str = "  " * indent
    if isinstance(y, dict):
        for k, v in y.items():
            lines.append(f"{indent_str}{k}")
            lines.extend(format_json(v, indent + 1))
    elif isinstance(y, list):
        for item in y:
            lines.extend(format_json(item, indent))
    else:
        lines.append(f"{indent_str}- {y}")
    return lines

for filename in os.listdir(input_dir):
    if filename.endswith(".json"):
        input_path = os.path.join(input_dir, filename)
        output_path = os.path.join(output_dir, f"{os.path.splitext(filename)[0]}.txt")

        with open(input_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        formatted_text = format_json(data)
        with open(output_path, "w", encoding="utf-8") as out_f:
            out_f.write("\n".join(formatted_text))

print(f"Formatted text files saved to: {output_dir}")