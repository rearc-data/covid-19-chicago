import os
import pandas as pd
import boto3
from urllib.request import urlopen
from html.parser import HTMLParser
import datetime

class MyHTMLParser(HTMLParser):

    def __init__(self):
        HTMLParser.__init__(self)
        self.data = None

    def handle_starttag(self, tag, attr):
        if tag.lower() == 'a':
            for item in attr:
                if item[0].lower() == 'href' and item[1].endswith('.pdf'):
                    if self.data == None:
                        self.data = item[1]

source = 'https://www.chicago.gov/city/en/sites/covid-19/home/latest-data.html'

html = urlopen(source)
str_html = html.read().decode()

parser = MyHTMLParser()
parser.feed(str_html)

base_url = 'https://www.chicago.gov'
pdf_path = parser.data

def source_dataset(s3_bucket, new_s3_key):

    today = pd.datetime.today().date().strftime('%Y-%m-%d')

    url = urlopen(base_url + pdf_path)
    output = open('/tmp/' + today + '.pdf', 'wb')
    output.write(url.read())
    output.close()

	#uploading new s3 dataset
    s3 = boto3.client('s3')
    s3.upload_file('/tmp/' + today + '.pdf', s3_bucket, new_s3_key)
