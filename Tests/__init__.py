import sys
import os
current_directory = os.path.dirname(os.path.abspath(__file__))
root_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(root_directory)