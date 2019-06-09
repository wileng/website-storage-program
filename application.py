import boto3
import botocore.vendored.requests.packages.urllib3 as urllib3
from boto3.dynamodb.conditions import Key
from botocore.client import ClientError
from flask import Flask, render_template, request, redirect, url_for, flash
import time

application = Flask(__name__)
application.secret_key = 'aoweirWE#(0wv9(#@()#'
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
dynamodb_client = boto3.client('dynamodb', region_name='us-west-2')
s3 = boto3.client('s3', region_name='us-west-2')
dimpsey_url =  'https://s3-us-west-2.amazonaws.com/css490/input.txt'
bucket = 'css490eng'
key = 'input.txt'

@application.route('/', methods = ['POST', 'GET'])
@application.route('/')
def home():
	if request.method == 'POST':
		arr = request.form
		if request.form['button'] == 'Query':
			#return request.form['FirstName'] + " " + request.form['LastName']
			first = request.form['FirstName']
			last = request.form['LastName']
			if not first and not last:
				flash("You forgot to enter a first and last name!")
				return render_template('base.html')
			#return redirect(url_for('error', error="You forgot to enter a first or last name!"))
			person = query(first, last)
			if not person:
				if not first:
					flash("No one with the last name \'" + last + "\' exists in the database!")
				elif not last:
					flash("No one with the first name \'" + first + "\' exists in the database!")
				else:
					flash("No one with the first name \'" + first + "\' and last name \'" + last + "\' exists in the database!")

			else:
				flash("RESULTS: \n")
				for x in person:
					flash(x[0] + " " + x[1])
					for attr in person[x]:
						flash(attr)

		if request.form['button'] == 'Clear':
			clear()
			clear_s3()

		if request.form['button'] == 'Load':
			flash(load())

		if request.form['button'] == 'Return To Main Page':
			return render_template('base.html')
	return render_template('base.html')

#----------------------------------------------------------------------

def query(first, last):
	table = dynamodb.Table('Person')
	print("FIRST" + first + "LAST" + last)
	if not first or not last:
		if first:
			try:
				response = table.query(
					KeyConditionExpression=Key('FirstName').eq(first)
				)
				return format_results(response)
			except Exception, e:
				print(e)
				return []
		elif last:
			try:
				response = table.query(
					IndexName= 'LastName-FirstName-index',
					KeyConditionExpression=Key('LastName').eq(last)
				)
				return format_results(response)
			except Exception, e:
				print(e)
				return []
	else:
		try:
			response = table.query(
				KeyConditionExpression=Key('FirstName').eq(first) &
									   Key('LastName').eq(last)
			)
			return format_results(response)
		except Exception, e:
			print(e)
			return []


def format_results(response):
	formatted_dict = {}

	for i in response['Items']:
		attr = []
		for x in i:
			if x != 'FirstName' and x != 'LastName':
				attr.append(x + ": " + i[x])
		formatted_dict[(i["FirstName"], i["LastName"])] = attr
	return formatted_dict

#----------------------------------------------------------------------

def load():
	if not test_connections(1):
		return "Unable to reach Dimpsey's S3 bucket after 5 tries, please try again in a few minutes"
	make_table()
	raw = fetch()
	formatted = format_raw(raw)
	return updateDB(formatted)

def make_table():
	try:
		table = dynamodb.create_table(
			TableName=('Person'),
			KeySchema=[
				{
					'AttributeName': 'FirstName',
					'KeyType': 'HASH'
				},
				{
					'AttributeName': 'LastName',
					'KeyType': 'RANGE'
				 }
			],
			AttributeDefinitions=[
				{
					'AttributeName': 'FirstName',
					'AttributeType': 'S'
				},
				{
					'AttributeName': 'LastName',
					'AttributeType': 'S'
				},

			],
			ProvisionedThroughput={
				'ReadCapacityUnits': 5,
				'WriteCapacityUnits': 5
			}
		)
		waiter = dynamodb_client.get_waiter('table_exists')
		waiter.wait(TableName='Person')
		return True
	except Exception, e:
		print(e)
		print("Table already exists?")


def fetch():
	http = urllib3.PoolManager()
	r = http.request('GET', dimpsey_url)
	s3.put_object(ACL='public-read', Body = r.data, Bucket = bucket, Key = key, )
	return r.data

def format_raw(raw):
	step0 = raw.split('\n')		#split by line
	step1 = [x.strip() for x in step0]	#strip whitespace from ends
	step2 = []		#split into idiv attributes
	step3 = []

	for x in step1:
		#print(x)
		step2.append(x.split())

	for person in step2:
		if len(person) >= 2:
			x = {}
			x['FirstName'] = person[0]
			x['LastName'] = person[1]
			for attr in person[2:]:
				attr_val = attr.split('=')
				x[attr_val[0]] = attr_val[1]
			step3.append(x)

	return step3

def updateDB(x):
	try:
		table = dynamodb.Table('Person')
		for person in x:
			print(person)
			response = table.put_item(Item=person)
			'''

		DO SOMETHING HERE
		
		'''
		return "Successfully updated db!"
	except Exception, e:
		print(e)
		if isinstance(e, dynamodb_client.exceptions.ResourceNotFoundException):
			return "This table is in the process of being cleared, please give it a minute!"
		'''
		DO SOMETHING HERE
		'''
		return str(e)

#----------------------------------------------------------------------
#wait is number of seconds to try clearing again
def clear():
	try:
		table = dynamodb.Table('Person')

		scan = table.scan(
			ProjectionExpression='#pk, #sk',
			ExpressionAttributeNames={
				'#pk': 'LastName',
				'#sk': 'FirstName'
			}
		)
		print(scan)
		for each in scan['Items']:
			print(each)

		with table.batch_writer() as batch:
			for each in scan['Items']:
				batch.delete_item(Key=each)

		flash("Successful cleared the table!")

	except Exception, e:
		flash("Something went wrong :( :" + str(e))

def clear_s3():
	try:
		s3.delete_object(Bucket=bucket, Key=key)
		flash("Cleared S3 Object!")
	except Exception, e:
		flash(str(e))
#----------------------------------------------------------------------
def test_connections(wait):
	time.sleep(wait)
	http = urllib3.PoolManager()
	try:
		r = http.request('GET', dimpsey_url)
		if r.status >= 400:
			if wait > 5:
				return False

		else:
			return True
	except:
		if wait > 5:
			return False
		return test_connections(wait + 1)

if __name__ == '__main__':
	application.run(debug = True)

