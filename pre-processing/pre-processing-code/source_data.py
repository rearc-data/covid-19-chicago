import os
import boto3
from urllib.request import urlopen, urlretrieve
from html.parser import HTMLParser

class MyHTMLParser(HTMLParser):

    def __init__(self):
        HTMLParser.__init__(self)
        self.data = None

    def handle_starttag(self, tag, attr):
        if tag.lower() == 'a' and self.data == None:
            for item in attr:
                if item[0].lower() == 'href' and item[1].endswith('.pdf'):
                        self.data = item[1]

def source_dataset(new_filename, s3_bucket, new_s3_key):

    source = 'https://www.chicago.gov/city/en/sites/covid-19/home/latest-data.html'

    html = urlopen(source)
    str_html = html.read().decode()

    parser = MyHTMLParser()
    parser.feed(str_html)

    base_url = 'https://www.chicago.gov'
    pdf_path = parser.data

    urlretrieve(base_url + pdf_path, '/tmp/' + new_filename)

    s3 = boto3.client('s3')

    s3.upload_file('/tmp/' + new_filename, s3_bucket, new_s3_key)
