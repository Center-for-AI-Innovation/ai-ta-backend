import fitz
import os
import re
import argparse
from pathlib import Path
import logging

def sanitize_filename(name):
    """Create a Linux-safe directory name from the PDF filename."""
    return re.sub(r'[^\w\-_.]', '_', name)

def extract_images_with_captions(pdf_path, output_dir=None, verbose=False):
    """
    Extract images from PDF and attempt to identify their captions.
    
    Args:
        pdf_path: Path to the PDF file
        output_dir: Output directory (defaults to PDF filename)
        verbose: Whether to print detailed info during extraction
    """
    # Set up logging
    log_level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(level=log_level, format='%(message)s')
    
    # Get PDF filename without extension for the output directory
    pdf_name = os.path.basename(pdf_path)
    pdf_name_no_ext = os.path.splitext(pdf_name)[0]
    pdf_name_safe = sanitize_filename(pdf_name_no_ext)
    
    # Create output directory if not specified
    if output_dir is None:
        output_dir = pdf_name_safe
    
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    logging.info(f"Processing: {pdf_path}")
    logging.info(f"Output directory: {output_dir}")
    
    # Open the PDF
    doc = fitz.open(pdf_path)
    
    # Track extracted images to avoid duplicates
    extracted_images = set()
    image_count = 0
    
    # Process each page
    for page_num, page in enumerate(doc):
        logging.info(f"Processing page {page_num + 1}/{len(doc)}")
        
        # Get the page's text blocks for caption detection
        blocks = page.get_text("blocks")
        text_blocks = [(b[0], b[1], b[2], b[3], b[4]) for b in blocks]
        
        # Dictionary to store potential captions
        potential_captions = {}
        
        # Identify potential figure captions
        for block in text_blocks:
            text = block[4].strip()
            if text.lower().startswith(("figure", "fig.", "fig ")):
                # Store the block coordinates and text as potential caption
                potential_captions[block[:4]] = text
        
        # Extract images from the page
        image_list = page.get_images(full=True)
        
        for img_index, img_info in enumerate(image_list):
            xref = img_info[0]  # Cross-reference number of the image
            
            # Skip if we've already extracted this image
            if xref in extracted_images:
                continue
                
            extracted_images.add(xref)
            
            # Get image data and type
            base_image = doc.extract_image(xref)
            image_bytes = base_image["image"]
            image_ext = base_image["ext"]
            
            # For academic papers, we want to filter out small decorative images
            # The exact threshold depends on the document, but this is a good starting point
            if len(image_bytes) < 5000:  # Skip very small images (likely decorative elements)
                continue
            
            # Generate output filename
            image_count += 1
            image_filename = f"{pdf_name_safe}_image_{image_count:03d}.{image_ext}"
            image_path = os.path.join(output_dir, image_filename)
            
            # Save the image
            with open(image_path, "wb") as img_file:
                img_file.write(image_bytes)
            
            logging.info(f"Saved image: {image_filename}")
            
            # Try to identify a caption for this image
            # Get the image rectangle on the page
            if img_info[1]:  # Check if image has a transformation matrix
                # Get the image rectangle using the transformation matrix
                image_rect = None
                try:
                    pix = fitz.Pixmap(doc, xref)
                    bbox = page.get_image_bbox(img_info)
                    if bbox:
                        image_rect = bbox
                except Exception as e:
                    logging.warning(f"Error getting image rectangle: {e}")
            
                # Find the closest caption below the image
                closest_caption = None
                min_distance = float('inf')
                
                if image_rect:
                    for caption_rect, caption_text in potential_captions.items():
                        # Check if caption is below the image
                        if caption_rect[1] >= image_rect[3]:  # caption top >= image bottom
                            # Calculate horizontal overlap
                            h_overlap = min(caption_rect[2], image_rect[2]) - max(caption_rect[0], image_rect[0])
                            if h_overlap > 0:  # There is horizontal overlap
                                distance = caption_rect[1] - image_rect[3]
                                if distance < min_distance:
                                    min_distance = distance
                                    closest_caption = caption_text
                
                # If we found a caption, save it
                if closest_caption and min_distance < 100:  # Limit the distance to avoid false matches
                    caption_filename = f"{pdf_name_safe}_image_{image_count:03d}_caption.txt"
                    caption_path = os.path.join(output_dir, caption_filename)
                    with open(caption_path, "w", encoding="utf-8") as caption_file:
                        caption_file.write(closest_caption)
                    logging.info(f"Saved caption: {caption_filename}")
    
    # Additional attempt to extract complete figures (which might be groups of images and vector graphics)
    try:
        for page_num, page in enumerate(doc):
            # Extract vector graphics and text as images
            pix = page.get_pixmap(alpha=False)
            if pix.width > 100 and pix.height > 100:  # Only process reasonable sized pages
                # Look for figures with vector graphics or complex layouts
                # that might not be captured by the regular image extraction
                
                # Detect figure regions
                figure_regions = []
                text = page.get_text("blocks")
                
                for block in text:
                    block_text = block[4].lower().strip()
                    if block_text.startswith(("figure", "fig.", "fig ")):
                        # Check previous blocks to see if they might be part of a figure
                        figure_y_min = block[1] - 300  # Look up to 300 points above the caption
                        figure_y_max = block[3]
                        figure_x_min = block[0] - 50
                        figure_x_max = block[2] + 50
                        
                        figure_regions.append((figure_x_min, figure_y_min, figure_x_max, figure_y_max, block[4]))
                
                # If we found potential figure regions, extract them as images
                for idx, region in enumerate(figure_regions):
                    x0, y0, x1, y1, caption = region
                    
                    # Ensure coordinates are within page bounds
                    x0 = max(0, x0)
                    y0 = max(0, y0)
                    x1 = min(pix.width, x1)
                    y1 = min(pix.height, y1)
                    
                    # Skip if region is too small
                    if x1 - x0 < 100 or y1 - y0 < 100:
                        continue
                    
                    # Create a cropped pixmap of the figure region
                    clip_rect = fitz.Rect(x0, y0, x1, y1)
                    try:
                        clip_pix = page.get_pixmap(clip=clip_rect, alpha=False)
                        
                        # Save the figure image
                        image_count += 1
                        figure_filename = f"{pdf_name_safe}_figure_{image_count:03d}.png"
                        figure_path = os.path.join(output_dir, figure_filename)
                        clip_pix.save(figure_path)
                        
                        # Save the caption
                        caption_filename = f"{pdf_name_safe}_figure_{image_count:03d}_caption.txt"
                        caption_path = os.path.join(output_dir, caption_filename)
                        with open(caption_path, "w", encoding="utf-8") as caption_file:
                            caption_file.write(caption)
                            
                        logging.info(f"Saved figure: {figure_filename}")
                    except Exception as e:
                        logging.warning(f"Error extracting figure region: {e}")
    except Exception as e:
        logging.error(f"Error in figure extraction: {e}")
    
    doc.close()
    logging.info(f"Extracted {image_count} images/figures to {output_dir}")
    return image_count

def main():
    parser = argparse.ArgumentParser(description="Extract images and captions from PDF files")
    parser.add_argument("pdf_path", help="Path to the PDF file or directory containing PDFs")
    parser.add_argument("-o", "--output-dir", help="Output directory (defaults to PDF filename)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print detailed information")
    parser.add_argument("-r", "--recursive", action="store_true", help="Process PDF files in subdirectories")
    
    args = parser.parse_args()
    
    # Check if the path is a file or directory
    path = Path(args.pdf_path)
    
    if path.is_file() and path.suffix.lower() == ".pdf":
        # Process a single PDF file
        extract_images_with_captions(str(path), args.output_dir, args.verbose)
    elif path.is_dir():
        # Process all PDFs in the directory
        pdf_pattern = "**/*.pdf" if args.recursive else "*.pdf"
        pdf_files = path.glob(pdf_pattern)
        
        for pdf_file in pdf_files:
            try:
                # Create a subdirectory in the output directory (if specified)
                output_subdir = None
                if args.output_dir:
                    pdf_name_safe = sanitize_filename(pdf_file.stem)
                    output_subdir = os.path.join(args.output_dir, pdf_name_safe)
                
                extract_images_with_captions(str(pdf_file), output_subdir, args.verbose)
            except Exception as e:
                logging.error(f"Error processing {pdf_file}: {e}")
    else:
        logging.error(f"The specified path is not a PDF file or directory: {path}")

if __name__ == "__main__":
    main()