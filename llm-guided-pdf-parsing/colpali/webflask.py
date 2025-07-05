from flask import Flask, request, jsonify
import torch
from PIL import Image
from io import BytesIO
from colpali_engine.models import ColQwen2, ColQwen2Processor

app = Flask(__name__)

colpali_model = ColQwen2.from_pretrained(
    "vidore/colqwen2-v1.0",
    device_map="cpu",
).eval()
colpali_processor = ColQwen2Processor.from_pretrained("vidore/colqwen2-v1.0")

@app.route('/process_image', methods=['POST'])
def process_image():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']
    
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    try:
        image = Image.open(BytesIO(file.read()))

        with torch.no_grad():
            processed_image = colpali_processor.process_images([image]).to(colpali_model.device)
            image_embedding = colpali_model(**processed_image)[0]

        image_embedding_list = image_embedding.cpu().float().numpy().tolist()
        
        return jsonify({"embedding": image_embedding_list})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
