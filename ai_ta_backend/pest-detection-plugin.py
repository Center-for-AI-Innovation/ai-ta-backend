# AIFARMS CropWizard Plugin for Pest Detection and Classification
# By Aditya Sengupta

# Google Colab Implementation:
# https://colab.research.google.com/drive/1GO-lw2PJtVewlA-xhBfgBLId8v4v-BE2?usp=sharing

# Import the required libraries
from PIL import Image
from ultralytics import YOLO
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.image as mpimg

# Load a custom trained YOLOv8n model for pest detection and classification
# The model I trained can be found below:
# https://www.dropbox.com/scl/fi/xf8wi0jy72kuk3xl47dnx/Aditya-Pest-Detection-YOLO-V1.pt

model = YOLO('/Users/adityasengupta/Downloads/pest-detection/Aditya-Pest-Detection-YOLO-V1.pt')

# If an image of a pest is input into CropWizard, it can call the below function to get the annotated image with bounding boxes and class labels

def pest_detection_plugin(image_path):
    # Run inference
    results = model(image_path)  # results object with inference results
    
    annotated_images = []

   # Show the results (multiple will be returned if there are multiple images)
    for r in results[0]:
        im_array = r.plot()  # plot a BGR numpy array of predictions
        im = Image.fromarray(im_array[..., ::-1])  # RGB PIL image (annotated with bounding boxes and class labels)
        annotated_images.append(im)
    
    # See Ultralytics YOLOv8 documentation for more details on extracting other information from the model results object
    # https://docs.ultralytics.com/modes/predict/

    return annotated_images

# Sample usage with a web image
image_url = 'https://www.arborday.org/trees/health/pests/images/figure-japanese-beetle-3.jpg'
output = pest_detection_plugin(image_url)

# Print annotated images
for image in output:
    image.show()