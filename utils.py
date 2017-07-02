import csv


def read_keywords(filename):
    ''' Reads in keywords from txt to a list '''

    file = open(filename)
    reader = csv.reader(file)

    keywords = [row[0] for row in reader]
    print('Track:', keywords)
    return keywords
