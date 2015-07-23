__author__ = 'acampbell'
import sys

import utils.extractor

print 'Relative imports are working!'

if __name__ == "__main__":
    if len(sys.argv) < 2:
            print 'ERROR: need to pass the path of the '