---
description: Detailed information about the Pest Detection tool
---

# Pest Detection Tool

Introducing the powerful new **LeAF Pest Detection Model**, now integrated into CropWizard to enhance your pest management capabilities! This tool leverages custom-trained computer vision models to accurately identify a vast range of agricultural pests directly from images. Built on a convolutional neural network (CNN) architecture, specifically a refined YOLOv8x model, LeAF has been trained on a massive dataset of over 1 million pest images, encompassing 3,580 different agricultural pest classes. This extensive training ensures high accuracy and reliability, allowing it to recognize even subtle differences between pest species.\


<figure><img src="https://lh7-rt.googleusercontent.com/docsz/AD_4nXclKXzCBOvRQBSbUghfVl6765koeh82eaD0U6izrPdhYhQAZWdJ9eOU-frDYyACKJbydVprQy95LAd2vvKiaZ1vCQpO5IJ65noeY5HexoyTxQL4CPPMtOSPipkyVIMx9W3vDwnt?key=QHnn7PILO5P3XH-4ReMzTztZ" alt=""><figcaption></figcaption></figure>

The LeAF model goes beyond simple classification. It not only identifies the pest in an image, but also provides bounding box detection, precisely pinpointing the pest's location within the image. The LeAF model (to be released in the AIFARMS blog this week) is also lightweight and capable of running on edge devices, such as farmer mobile phones or agricultural robots, which allows for easy model deployment and accessibility for all farmers. This is crucial for on-the-spot analysis and helps in monitoring pest populations across your fields, assisting with targeted treatment strategies.

The model as of today has been integrated within CropWizard as a tool. Simply upload an image containing a suspected pest, and you'll receive both the identification and the model detection, directly integrated into the context of your other questions and CropWizard's broader knowledge base.

With LeAF, CropWizard now provides an even more comprehensive approach to pest analysis. You can use this capability to quickly diagnose pest problems, track infestations, and get more informed treatment recommendations – all within the familiar CropWizard interface. This integration empowers you with faster, more accurate decision-making, leading to healthier crops, reduced pesticide use, and increased efficiency in your farming operations. It is constantly being improved, with plans to expand, and be retrained to perform even better with testing and feedback.\


<figure><img src="https://lh7-rt.googleusercontent.com/docsz/AD_4nXeGexIZKEBBJedSJm02jlyIpBRVkbAl4hm2bIlEFsW91uy-JuGLZudo6EEct6Iwy-ZCiR45OEQzVHFryGwTfqkMoFmksH7R6-M3IemgMG177lAFU4dfhVwRGbrS4q8NmH1UPSxOVQ?key=QHnn7PILO5P3XH-4ReMzTztZ" alt=""><figcaption></figcaption></figure>

\
