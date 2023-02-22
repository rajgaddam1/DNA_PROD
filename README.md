import cv2
import pandas as pd

# Read annotations file
df = pd.read_csv('annotations.csv')

# Loop through rows in the annotations file
for index, row in df.iterrows():
    # Load image
    img = cv2.imread(row['image_path'])
    
    # Extract bounding box coordinates
    x, y, w, h = row['x'], row['y'], row['w'], row['h']
    
    # Draw bounding box
    cv2.rectangle(img, (x, y), (x+w, y+h), (0, 255, 0), 2)
    
    # Show annotated image
    cv2.imshow('Annotated Image', img)
    cv2.waitKey(0)
    
    # Save annotated image to disk
    cv2.imwrite('annotated_' + row['image_path'], img)
