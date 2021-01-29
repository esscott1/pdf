import os
import img2pdf

print('--- tiff to PDF ---')

with open("output.pdf", "wb") as f:
    f.write(img2pdf.convert([i for i in os.listdir('.') if
    i.endswith(".tif")]))

print('ran converter, check for output.pdf file')


