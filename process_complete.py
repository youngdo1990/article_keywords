# -*- coding = utf-8 -*-

from preprocessing import *
from postprocessing_complete import *

if __name__ == "__main__":
    preprocessing(truncate=False)
    postprocessing()