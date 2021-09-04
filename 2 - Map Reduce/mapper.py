# mapper
import sys
# import string

for line in sys.stdin:
    line = line.strip()
    words = line.split()
    # it was not declare rather destinguish between capital letters or not, therefore we did not implement the following row
    #words = [word.strip(string.punctuation).lower() for word in line.split()]
    for word in words:
        print (word + '\t1')