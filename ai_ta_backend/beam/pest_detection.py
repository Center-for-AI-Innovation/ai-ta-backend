"""
To deploy: beam deploy pest_detection.py
For testing: beam serve pest_detection.py
Use CAII gmail to auth.
"""

from typing import Any, Dict, List

import beam

if beam.env.is_remote():
  import inspect
  import io
  import json
  import os
  import time
  import traceback
  import uuid

  import boto3
  import requests
  from PIL import Image
  from ultralytics import YOLO

# Simpler image, but slower cold starts: beam.Image.from_registry('ultralytics/ultralytics:latest-cpu')

requirements = [
    "torch==2.2.0",
    "ultralytics==8.1.0",
    "torchvision==0.17.0",
    "opencv-python",
    "boto3==1.34",
    "pillow",
    "numpy<2",
]

volume_path = "./models"

image = beam.Image(
    python_version="python3.10",
    python_packages=requirements,
    commands=["apt-get update && apt-get install -y libgl1-mesa-glx libglib2.0-0"],
)

ourSecrets = [
    "CLOUDFLARE_ACCOUNT_ID",
    "CLOUDFLARE_ACCESS_KEY_ID",
    "CLOUDFLARE_SECRET_ACCESS_KEY",
]


def loader():
  """Model weights are downloaded once at image build time using the @build hook and saved into the image. 'Baking' models into the modal.Image at build time provided the fastest cold start. """
  print("Inside loader")

  model_url = "https://assets.kastan.ai/pest_detection_model_weights.pt"
  model_path = f'{volume_path}/pest_detection_model_weights.pt'
  start_time = time.monotonic()

  print("After model path: ", model_path)

  # save/load model to/from volume
  if not os.path.exists(model_path):
    print("Downloading model from the internet")
    response = requests.get(model_url, timeout=60)
    with open(model_path, "wb") as f:
      f.write(response.content)
    model = YOLO(response.content)
  else:
    print("Loading model from volume")
    model = YOLO(model_path)

  s3 = boto3.client(
      service_name="s3",
      endpoint_url=f"https://{os.environ['CLOUDFLARE_ACCOUNT_ID']}.r2.cloudflarestorage.com",
      aws_access_key_id=os.environ['CLOUDFLARE_ACCESS_KEY_ID'],
      aws_secret_access_key=os.environ['CLOUDFLARE_SECRET_ACCESS_KEY'],
      # region_name="<location>", # Must be one of: wnam, enam, weur, eeur, apac, auto
  )

  print(f"⏰ Runtime to load model: {(time.monotonic() - start_time):.2f} seconds")
  return model, s3


# autoscaler = QueueDepthAutoscaler(max_tasks_per_replica=2, max_replicas=3)
@beam.endpoint(name="pest_detection",
               workers=1,
               cpu=1,
               memory="3Gi",
               max_pending_tasks=10,
               timeout=60,
               keep_warm_seconds=60 * 3,
               secrets=ourSecrets,
               on_start=loader,
               image=image,
               volumes=[beam.Volume(name="my_models", mount_path=volume_path)])
def predict(context, **inputs: Dict[str, Any]):
  """
    Run the pest detection plugin on an image and return image data as blobs.
    """

  def _detect_pests(model, image_paths: List[str]) -> List[Image.Image]:
    """ Run pest detection on the given images. """
    # Run inference
    results = model(image_paths)  # results object with inference results

    annotated_images = []

    # Extract annotated images from the results object
    # Flatten the results object to get the annotated images for each input image
    for result_set in results:
      for r in result_set:
        im_array = r.plot()  # plot a BGR numpy array of predictions
        im = Image.fromarray(im_array[..., ::-1])  # RGB PIL image (annotated with bounding boxes and class labels)
        annotated_images.append(im)
    return annotated_images

  print("Inside predict() endpoint")
  model, s3 = context.on_start_value
  image_urls: List[str] = inputs.get('image_urls', [])  # type: ignore
  print(f"Image URLs: {image_urls}")

  if image_urls and isinstance(image_urls, str):
    image_urls = json.loads(image_urls)
  print(f"Final image URLs: {image_urls}")

  if not image_urls:
    return "❌ No input image URLs provided."

  try:
    # Run the plugin
    annotated_images = _detect_pests(model, image_urls)
    print(f"annotated_images found: {len(annotated_images)}")
    img_urls = []

    for image in annotated_images:
      # Convert image to bytes
      img_byte_arr = io.BytesIO()
      image.save(img_byte_arr, format='JPEG')
      img_byte_arr = img_byte_arr.getvalue()

      # Upload image to R2 bucket 'public-assets/cropwizard/pest_detection/...'
      upload_key = f"cropwizard/pest_detection/{uuid.uuid4()}.jpg"
      img_url = f"https://assets.kastan.ai/{upload_key}"

      s3.upload_fileobj(io.BytesIO(img_byte_arr), 'public-assets', upload_key)
      img_urls.append(img_url)
      print("Image URL: ", img_url)

    return {"image_urls": img_urls}
  except Exception as e:
    err = f"❌❌ Error in (pest_detection): `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n{traceback.format_exc()}"  # type: ignore
    print(err)
    # sentry_sdk.capture_exception(e)
    return err
