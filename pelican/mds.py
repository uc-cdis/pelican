import requests
import json
import base64
import time

def mds_submit(hostname, access_token, guid, file_type, message = None):
	"""
	:params hostname: url of the commons
	:params access_token: token for submitting to the metadata service"
	:params guid: indexd guid of file that metadata is being written for
	:params file_type: type of file that the submission of the metadata service is describing
	:param message: optional message to submit to the metadata service
	"""

	mds_hostname = hostname + "/mds/metadata/" + guid

	unix_time_4_weeks = 4 * 24 * 3600

	# experiation time of pfbs should be 4 weeks
	exp_time = int(time.time()) + unix_time_4_weeks
	
	details = {}
	details["guid"] = guid
	if message:
		details["msg"] = message
	details["type"] = file_type
	details["date_to_delete"] = exp_time

	print("-------------")
	print("posting to metadata service: ", json.dumps(details))
	print("-------------")

	r = requests.post(mds_hostname, data=json.dumps(details), headers={"content-type": "application/json", "Authorization": "Bearer " + access_token})

	if r.status_code == 201:
		return r.json()
	else:
		raise Exception(f"Submission to indexd failed with {r.status_code} ------- {r.text}")
