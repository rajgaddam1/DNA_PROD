
from PIL import Image, ImageDraw

# Read input image
img = Image.open("input_image.jpg")

# Read annotation file
with open("annotations.txt", "r") as f:
    annotations = f.readlines()

# Parse annotations to get bounding box coordinates
bboxes = []
for annotation in annotations:
    label, xmin, ymin, xmax, ymax = annotation.split()
    bboxes.append((int(xmin), int(ymin), int(xmax), int(ymax)))

# Draw bounding boxes on image
draw = ImageDraw.Draw(img)
for bbox in bboxes:
    draw.rectangle(bbox, outline="red")

# Save modified image with bounding boxes
img.save("output_image.jpg")
