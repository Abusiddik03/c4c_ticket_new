# ==============================================================================
# lambda:       dst_c4c_ticket_collection
# Author:       iLink Systems
# Date:         Jan 20, 2020
# Description:  Lambda function to extract Ticket Collection data from
#               C4C API, Write out results to S3 and to Redshift.
#
# Load Frequency: Daily Incremental Load - Every Day
# ==============================================================================

from botocore.exceptions import ClientError
from requests.auth import HTTPBasicAuth
from dateutil.parser import parse
from datetime import date, timedelta
import xmltodict
import requests
import base64
import boto3
import time
import csv
import os

# Initializing boto3 attributes
sns_client = boto3.client('sns')
s3 = boto3.client('s3')
s3Resource = boto3.resource('s3')

# Actual part of extracting Ticket Collection Data
def get_ticket_data(user_name, password, event, account_id, region_name):
    # Getting necessary info from cloudwatch event
    s3_bucket = event["s3_bucket"]
    s3_keyspace = event["s3_keyspace"]
    run_time = event["run_time"]

    # Getting Ingestion Date and Extracted Date
    ingest_date = date.today()
    extracted_date = ingest_date.strftime("%Y-%m-%d %H:%M:%S")

    # Preparing a string format of ingestion date for creating s3 keyspace and for filename.
    date1 = ingest_date.strftime("%m_%d_%Y")

    # Creating a dynamic file name based on the ingestion date
    file_name = "c4c_ticket_data_" + date1 + ".txt"
    print(file_name)

    # Calculate yesterday's date for calling API in the date range of yesterday and today
    yesterday = ingest_date - timedelta(days=1)

    # Formatting the date in an appropriate format. Example - '2019-12-08'
    d_val = yesterday.strftime("%Y-%m-%d")

    # Last run time of this API
    t_val = run_time

    # Getting count api from cloud watch event for getting the total number of records for extracting the data batch by batch
    api_for_count = event["count_api_base"]
    api_for_count = api_for_count.format(d_val, t_val)

    # Calling Requests function to get the Number of records
    count_json = requests.get(api_for_count, auth=HTTPBasicAuth(user_name, password)).json()

    # Extracting only the count from the json
    count = int(count_json["d"]["__count"])
    print("Total Number of Records:    " + str(count))

    # Calling sleep function to avoid hitting the api simultaneously
    time.sleep(2)

    # Getting Actual Data Batch by Batch. Batch size is 2000
    for i in range(0, count, 2000):

        # Initializing an empty data_list.
        data_list = []

        # Creating empty string container to load the json data
        data = ""

        # Set the skip and flag to Zero for the first iteration.
        # From the next iteration skip will be increment by 2000 to extract the Next 3000 batch.
        # Set Flag to zero for writing the header only once. from the next iteration flag will be zero to avoid writing the header multiple times.
        if i == 0:
            skip, flag = 0, 0

        # Building actual API with corresponding skip and top parameters
        api_ticket = event['actual_api_base']
        api_ticket = api_ticket.format(d_val, t_val) + str(skip) + "&$top=2000"

        # Increment skip by 2000 for the next iteration
        skip = i + 2000

        # Calling sleep function to avoid hitting the api simultaneously
        time.sleep(2)

        # Extracting Actual data and store it in a xml format
        data = requests.get(api_ticket, auth=HTTPBasicAuth(user_name, password))
        data = data.text

        # Split the data to get only selected information between <entry> & </entry> tags
        data = data.split("<?xml version=")[1].split('title="ServiceRequestCollection"/>')[1].replace("</feed>", "")

        # Build a custom xml format by adding open and closed tags. so we can parse this xml to json using xmltodict package.

        # Adding open tag to string variable called final_data
        final_data = "<xmldata>"

        # Appending actual data extracted from api to the final_data
        final_data += data

        # Appending close tag to final_data
        final_data += "</xmldata>"

        # Replace pipe symbols from the data if exist
        final_data = final_data.replace("|", "")

        # Parsing the custom xml data to a json
        jsonData = xmltodict.parse(final_data)

        # Loops through each records for preparing a dictionary to load it to data_list.
        for c in jsonData["xmldata"]["entry"]:

            # Loading each records into a dictionary called finalDict
            finalDict = {}
            finalDict = c["content"]["m:properties"]

            # Hadling date columns from each records.
            # This condition will check for a proper date format to parse it as a valid date format. Or else it will consider as blanks

            # ETag
            if "d:ETag" in finalDict.keys() and finalDict["d:ETag"] and "null" not in str(finalDict["d:ETag"]):
                finalDict["d:ETag"] = parse(finalDict["d:ETag"]).strftime("%Y-%m-%d %H:%M:%S")
            else:
                finalDict["d:ETag"] = ""

            # CreationDatetime
            if "d:CreationDateTime" in finalDict.keys() and finalDict["d:CreationDateTime"] and "null" not in str(
                    finalDict["d:CreationDateTime"]):
                finalDict["d:CreationDateTime"] = parse(finalDict["d:CreationDateTime"]).strftime("%Y-%m-%d %H:%M:%S")
            else:
                finalDict["d:CreationDateTime"] = ""

            # EscalationDateTime
            if "d:EscalationDateTime" in finalDict.keys() and finalDict["d:EscalationDateTime"] and "null" not in str(
                    finalDict["d:EscalationDateTime"]):
                finalDict["d:EscalationDateTime"] = parse(finalDict["d:EscalationDateTime"]).strftime(
                    "%Y-%m-%d %H:%M:%S")
            else:
                finalDict["d:EscalationDateTime"] = ""

            # CompletedOnDateTime
            if "d:CompletedOnDateTime" in finalDict.keys() and finalDict["d:CompletedOnDateTime"] and "null" not in str(
                    finalDict["d:CompletedOnDateTime"]):
                finalDict["d:CompletedOnDateTime"] = parse(finalDict["d:CompletedOnDateTime"]).strftime(
                    "%Y-%m-%d %H:%M:%S")
            else:
                finalDict["d:ETag"] = ""

            # ResolvedOnDateTime
            if "d:ResolvedOnDateTime" in finalDict.keys() and finalDict["d:ResolvedOnDateTime"] and "null" not in str(
                    finalDict["d:ResolvedOnDateTime"]):
                finalDict["d:ResolvedOnDateTime"] = parse(finalDict["d:ResolvedOnDateTime"]).strftime(
                    "%Y-%m-%d %H:%M:%S")
            else:
                finalDict["d:ResolvedOnDateTime"] = ""

            # LastChangeDateTime
            if "d:LastChangeDateTime" in finalDict.keys() and finalDict["d:LastChangeDateTime"] and "null" not in str(
                    finalDict["d:LastChangeDateTime"]):
                finalDict["d:LastChangeDateTime"] = parse(finalDict["d:LastChangeDateTime"]).strftime(
                    "%Y-%m-%d %H:%M:%S")
            else:
                finalDict["d:LastChangeDateTime"] = ""

            # WarrantyEndDate
            if "d:WarrantyEndDate" in finalDict.keys() and finalDict["d:WarrantyEndDate"] and "null" not in str(
                    finalDict["d:WarrantyEndDate"]):
                finalDict["d:WarrantyEndDate"] = parse(finalDict["d:WarrantyEndDate"]).strftime("%Y-%m-%d %H:%M:%S")
            else:
                finalDict["d:WarrantyEndDate"] = ""

            # VECEventOccuredOn_KUT
            if "d:VECEventOccuredOn_KUT" in finalDict.keys() and finalDict[
                "d:VECEventOccuredOn_KUT"] and "null" not in str(finalDict["d:VECEventOccuredOn_KUT"]):
                finalDict["d:VECEventOccuredOn_KUT"] = parse(finalDict["d:VECEventOccuredOn_KUT"]).strftime(
                    "%Y-%m-%d %H:%M:%S")
            else:
                finalDict["d:VECEventOccuredOn_KUT"] = ""

            # Getting extracted date to know when the api gets ran
            finalDict["extracted_date"] = extracted_date

            # Appending the final dictionary to list.
            data_list.append(finalDict.copy())

        # Getting the Headers from the list.
        header = [k for k, v in data_list[0].items()]

        # Converts the list of json data to a csv and load it to /tmp/ folder
        export_dict_list_to_csv(data_list, header, flag, file_name)
        print(i)

        # Reset the flag value to avoid writing the headers more than once.
        flag = 1

    # Cleanups the preceeding d: from the header
    cleanup_header(file_name, date1)

    # Calling Upload to s3 Method
    uploadToS3(file_name, date1, ingest_date, s3_bucket, s3_keyspace)

    return "Success"


# Converts list of json to a csv file
def export_dict_list_to_csv(content_list, header, flag, filename):
    if flag == 0:
        # empty output file just in case lambda runs again and tries to use same /tmp space
        if os.path.exists("/tmp/" + filename):
            os.remove("/tmp/" + filename)
            print("Removed the file %s" % "/tmp/" + filename)

    # Write the data as Pipe delimited format
    csv.register_dialect('myDialect', delimiter='|', quoting=csv.QUOTE_ALL)
    with open("/tmp/" + filename, 'a', newline='', encoding="utf-8-sig") as csvfile:

        # Initializing DictWriter
        writer = csv.DictWriter(csvfile, fieldnames=header, dialect="myDialect")

        # Writes header only once when flag equals to zero
        if flag == 0:
            print("header wrote successfully")
            writer.writeheader()

        # Writing actual list
        writer.writerows(content_list)


# Removes preceeding d: from the headers
def cleanup_header(file_name, ingest_date_str):
    # Opening the extracted file from /tmp keyspace in read mode
    with open("/tmp/" + file_name, "r", encoding="utf-8-sig") as f:
        # Reading actual data as string
        data = f.read()

        # Replacing d: from the data
        data = data.replace("d:", "")

        # Writing it to /tmp space with valid file name
        with open("/tmp/c4c_ticket_" + ingest_date_str + ".txt", "w", encoding="utf-8-sig") as fw:
            fw.write(data)

    print("Fact Tables Created")


# Uploads the CSV from /tmp location to S3 bucket
def uploadToS3(filename, ingest_date_str, ingest_date, s3bucket, s3_keyspace):
    # Preparing file name with path
    fileNameWithPath = "/tmp/c4c_ticket_" + ingest_date_str + ".txt"
    data = open(fileNameWithPath, 'r')

    # Preparing s3 object
    s3object = s3_keyspace + "/ingest_date=" + str(ingest_date) + "/" + filename

    # Uploading csv file to corresponding s3 path
    response = s3Resource.Bucket(s3bucket).upload_file(fileNameWithPath, s3object)
    print("Uploaded file " + filename + " to S3 path. Response is :" + str(response))
    print("*#*" * 50)


def lambda_handler(event, context):
    """"Entry point for Lambda"""
    try:

        # Getting Account id and Region for building ARNs dynamically
        functionARN = context.invoked_function_arn
        print("ARN for current lambda :::: " + str(functionARN))
        account_id = functionARN.split(':')[4]
        region_name = functionARN.split(':')[3]
        print("Account Id is :: " + str(account_id))
        print("Region is :: " + str(region_name))

        # Getting c4c secret name from the cloud watch event.
        user_name = event["userId"]
        password = event["password"]

        # Calling the actual API Extraction part with credentials and event information
        get_ticket_data(user_name, password, event, account_id, region_name)

    except Exception as e:

        # Handling Exceptions
        print("Exception occured " + str(e))
        raise e
