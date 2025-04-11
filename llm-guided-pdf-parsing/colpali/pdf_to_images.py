import fitz  # PyMuPDF
import os
from tempfile import NamedTemporaryFile

def pdf_to_images(pdf_path, output_dir, zoom_x=2.0, zoom_y=2.0):
    """
    Converts a PDF into images, one image per page, saved in the specified output directory.

    :param pdf_path: Path to the input PDF file.
    :param output_dir: Directory where the images will be saved.
    :param zoom_x: Horizontal zoom factor for higher resolution. Default is 2.0.
    :param zoom_y: Vertical zoom factor for higher resolution. Default is 2.0.
    """
    doc = fitz.open(pdf_path)
    os.makedirs(output_dir, exist_ok=True)
    mat = fitz.Matrix(zoom_x, zoom_y)
    for i, page in enumerate(doc):
        pix = page.get_pixmap(matrix=mat)
        
        output_path = os.path.join(output_dir, f"page_{i + 1}.png")
        pix.save(output_path)
        print(f"Saved: {output_path}")

    doc.close()
    print("Completed!")
