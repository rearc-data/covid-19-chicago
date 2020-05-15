import boto3
from urllib.request import urlopen, urlretrieve
from urllib.error import URLError, HTTPError
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

	source_dataset_url = 'https://www.chicago.gov/city/en/sites/covid-19/home/latest-data.html'

	# throws error occured if there was a problem accessing data
	# otherwise downloads and uploads to s3

	try:
		response = urlopen(source_dataset_url)

	except HTTPError as e:
		raise Exception('HTTPError: ', e.code, new_filename)

	except URLError as e:
		raise Exception('URLError: ', e.reason, new_filename)

	else:
		html = response.read().decode()

		parser = MyHTMLParser()
		parser.feed(html)

		base_url = 'https://www.chicago.gov'
		pdf_path = parser.data

		try:
			pdf = urlopen(base_url + pdf_path)

		except HTTPError as e:
			raise Exception('HTTPError: ', e.code, new_filename)

		except URLError as e:
			raise Exception('URLError: ', e.reason, new_filename)

		else:
			file_location = '/tmp/' + new_filename

			with open(file_location, 'wb') as f:
				f.write(pdf.read())

			s3 = boto3.client('s3')
			s3.upload_file(file_location, s3_bucket, new_s3_key)

			return [{'Bucket': s3_bucket, 'Key': new_s3_key}]

source_dataset('test.pdf', 1, 2)
