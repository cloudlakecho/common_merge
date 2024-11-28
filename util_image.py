#
# util_image.py
# from August 21, 2024
#
# Function:
#     Extract image from PDF
#     Performe SSIM
#     Most common shape in the image and count of it


"""

  To do
    test using "unit_test.py"

  work? - not even completed - September 11, 2024

  reference:
    image extraction from PDF
      https://askubuntu.com/a/150106/789450

"""

import random
import os, pdb, sys
import subprocess


DEBUGGING = True


def extract_image(args):
    file_location = args.in_folder
    file_format = args.file_format
    files_to_use = [os.path.join(dirpath, f)
      for dirpath, dirnames, files in os.walk(file_location)
        for f in fnmatch.filter(files, "*." + file_format)
    ]
    option = "-all"
    option_format = "-j"

    if (DEBUGGING):
        sel = random.randint(0, len(files_to_use))
        print ("one of file: {}".format(files_to_use[sel]))
        pdb.set_trace()


    for i in files_to_use:
        #
        # Error spot
        #   It should be like "input file directory"/extracted_image
        #
        dir_cur = os.path.dirname(files_to_use)
        image_root = os.path.join(dir_cur, 'extracted_image')

        if (DEBUGGING):
            pdb.set_trace()
        #
        # Error
        #   SyntaxError: EOL while scanning string literal
        #
        try:
            subprocess.run(
              ['pdfimages`, option, option_format, i, image_root]
            )
            # subprocess.run(['cat', i])
        except Exception as e:
            print (files, e.args)
        else:
            print (files)
