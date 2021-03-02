from pdfrw import PdfReader, PdfWriter

input_file = "C:/src/egit/pdf/Flint_opt_good_test2.pdf"
output_file = "C:/src/egit/pdf/Flint_opt_good_test2_reordered.pdf"

reader_input = PdfReader(input_file)
writer_output = PdfWriter()
pages_list = [1,3,6,2,4,5]
correct_order = []
print(f'the number of pages are {len(reader_input.pages)}')
print(f'index of page 6 is {pages_list.index(6,0,6)}')


for current_page in range(len(reader_input.pages)):
    #if current_page > 1:
    source_page_no = pages_list[current_page] - 1
    writer_output.addpage(reader_input.pages[source_page_no])
    print("adding page %i" % (current_page +1))

writer_output.write(output_file)