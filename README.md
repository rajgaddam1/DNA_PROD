
from PIL import Image, ImageDraw

# Read input image
img = Image.open("input_image.jpg")

# Read annotation file
with open("annotations.txt", "r") as f:
    annotations = f.readlines()

# Define class names (if available)
class_names = ["class1", "class2", "class3"]

# Define function to convert YOLO coordinates to pixel coordinates
def yolo_to_pixel(box, image_size):
    x, y, w, h = box
    width, height = image_size
    xmin = int((x - w/2) * width)
    xmax = int((x + w/2) * width)
    ymin = int((y - h/2) * height)
    ymax = int((y + h/2) * height)
    return (xmin, ymin, xmax, ymax)

# Draw bounding boxes on image
draw = ImageDraw.Draw(img)
for annotation in annotations:
    class_id, x, y, w, h = map(float, annotation.split())
    box = yolo_to_pixel((x, y, w, h), img.size)
    draw.rectangle(box, outline="red")
    if class_names:
        class_name = class_names[int(class_id)]
        draw.text((box[0], box[1]), class_name)

# Save modified image with bounding boxes
img.save("output_image.jpg")
