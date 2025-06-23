from datetime import date, datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
import json
import os
import sys
import psycopg2
import datetime
import boto3


# Utility function to retrieve a specific key from an AWS Secrets Manager secret.

def get_secret_value(secret_name, key):
    """
    Returns the requested AWS Secret

    Args:
        secret_name (str): The name of the AWS Secrets Manager secret
        key (str): The key inside the secret's key-value pair

    Returns:
        str: The requested secret value

    Raises:
        Exception: If there is an issue accessing the secret
    """
    client = boto3.client('secretsmanager', region_name='ca-central-1')
    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])
        print(f"Successfully retrieved key '{key}' from secret '{secret_name}'")
        return secret[key]
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise


# Assign credentials and collector information
endpoint = os.environ['ES_ENDPOINT']
index = os.environ['ES_INDEX']
REDSHIFT_DATABASE = os.environ['REDSHIFT_DATABASE']
REDSHIFT_USER = os.environ['REDSHIFT_USER']
REDSHIFT_PASSWD = get_secret_value('SBC-RT-REDSHIFT-PASSWORD', 'REDSHIFT_PASSWD')


REDSHIFT_PORT = os.environ['REDSHIFT_PORT']
REDSHIFT_ENDPOINT = os.environ['REDSHIFT_ENDPOINT']
API_ENV = os.environ['API_ENV']


with open('./serviceBCOfficeList.json') as json_file:
    # Get the list of Service Centers and their IDs
    service_centers = json.load(json_file)


def lambda_handler(event, context):

    # To access query string parameters: event['queryStringParameters']['param']
    # To access path parameters: event['pathParameters']['param']
    
    all_offices = False
    office_ids = []
    # Retrieve any office id's from query string parameters
    if 'queryStringParameters' in event:
        if 'id' in event['queryStringParameters']:
            office_ids = event['queryStringParameters']['id'].split(",")
        else:
            for service_center in service_centers:
                office_ids.append(service_center['cfms_poc.office_id'])
                all_offices = True
    else:
        for service_center in service_centers:
            office_ids.append(service_center['cfms_poc.office_id'])
            all_offices = True
    
    
    # Query Redshift and Elastic Search
    rs_query = "SELECT office_id, time_per AS time FROM servicebc.servetime;"
    rs_result = query_redshift(rs_query)
  
    es_result = query_elasticsearch_realtime(office_ids)

    # Generate the API response using the data from Redshift and ElasticSearch
    times = {}
    for row in rs_result:
        times[row[0]] = row[1] # row[0] is the office_id and row[1] is the time calculation

    api_response_data = []
    for office in es_result:
        office_id = int(office['office_id'])
        current_line_length = max(0,office['current_line_length']) # ensure that we don't report fewer than 0 people in line
        estimated_wait = ''
        num_agents = int(office['num_agents'])
        if office_id in times.keys():
            estimated_wait = round( (times[office_id] / max(1,num_agents)) * current_line_length / 60) #  (time_per / #agents) * length
            #this is rounded to the nearest minute, for now, as the app doesn't display fractional minutes well
            
        api_response_data.append({"office_id": office_id, "current_line_length": current_line_length, "estimated_wait": estimated_wait })

    vis = False
    # Check if the api was queried for a visualization (html) response 
    if 'queryStringParameters' in event and 'vis' in event['queryStringParameters'] and event['queryStringParameters']['vis'].lower() == 'true':
        vis = True

    return generate_api_response(office_ids,api_response_data,vis,all_offices)

    
# Generate either a JSON response or a html response based on the 
# query string parameters
def generate_api_response(office_ids,api_response_data,vis,all_offices):
    # check if this is a visualization request
    if vis:
        body = build_wait_times_graph(api_response_data,all_offices)
        headers = {"content-type": "text/html"}
    else:
        body = json.dumps({
            "api_name": "sbc-wt",
            "api_env": API_ENV,
            "api_version": "0.1",
            "response_tstamp": datetime.datetime.now(),
            "results_count": len(office_ids),
            "data": api_response_data
        },default=str)
        headers = {"content-type": "application/json"}
            
    # Return the API response data
    return {
        "statusCode": 200,
        "headers": headers,
        "body": body
    }


# Executes a query on the Redhshift database 
# and returns the results
def query_redshift(query_string):
    
    try:
        conn = psycopg2.connect(
            dbname=REDSHIFT_DATABASE,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWD,
            port=REDSHIFT_PORT,
            host=REDSHIFT_ENDPOINT
        )
    except Exception as ERROR:
        print(f"Connection Issue: {ERROR}")

    try:
        cursor = conn.cursor()
        cursor.execute(query_string)
        result = cursor.fetchall()
        cursor.close()
        conn.commit()
        conn.close()
    except Exception as ERROR:
        print(f"Execution Issue: {ERROR}")

    return result


def query_elasticsearch_realtime(office_ids):
    # Queries ElasticSearch for:
    #   - the number of citizens in line for a given office
    #   - the number of front-of-office events for a given office   
    # It then returns the results.
    anchordate = date.today().strftime("%Y-%m-%d")
    client = Elasticsearch(endpoint)

    results_list = []
    for id in office_ids:
    
        # Query for number of addcitizen events.
        # Note: Uses IANA time zone code 'America/Vancouver'
        # to account for PDT and UTC offset.
        params = Q('term', app_id='TheQ') & \
            Q('term', event_name='addcitizen') & \
            Q('term', **{'contexts_ca_bc_gov_cfmspoc_office_1.office_id':
                         id}) & \
            Q('range', derived_tstamp={'gte': anchordate}) & \
            Q('range', derived_tstamp={'lte': "now"}) & \
            Q('range', derived_tstamp={'time_zone': "America/Vancouver"})
        try:
            add_citizen_search_result = Search(using=client, index=index).filter(params)
        except Exception as e:
            print(e)
    
        add_citizen_count = add_citizen_search_result.count()
    
        # Query for number of customerleft events.
        params = Q('term', app_id='TheQ') & \
            Q('term', event_name='customerleft') & \
            Q('term', **{'contexts_ca_bc_gov_cfmspoc_office_1.office_id':
                         id}) & \
            Q('range', derived_tstamp={'gte': anchordate}) & \
            Q('range', derived_tstamp={'lte': "now"}) & \
            Q('range', derived_tstamp={'time_zone': "America/Vancouver"})
        try:
            customer_left_search_result = Search(using=client, index=index).filter(params)
        except Exception as e:
            print(e)
        customer_left_count = customer_left_search_result.count()
    
        # Query for number of finish events.
        params = Q('term', app_id='TheQ') & \
            Q('term', event_name='finish') & \
            Q('term', **{'contexts_ca_bc_gov_cfmspoc_office_1.office_id':
                         id}) & \
            Q('range', derived_tstamp={'gte': anchordate}) & \
            Q('range', derived_tstamp={'lte': "now"}) & \
            Q('range', derived_tstamp={'time_zone': "America/Vancouver"})
        try:
            finish_events_search_result = Search(using=client, index=index).filter(params)
        except Exception as e:
            print(e)
        finish_events_count = finish_events_search_result.count()

        # Calculate timedelta for one hour ago to now
        now_minus_1_hour = datetime.datetime.now() - timedelta(hours=1)
        
        # Query for all front-office events in the last hour
        params = Q('term', app_id='TheQ') & \
            Q('term', **{'contexts_ca_bc_gov_cfmspoc_office_1.office_id':
                         id}) & \
            ~Q('term', **{'unstruct_event_ca_bc_gov_cfmspoc_chooseservice_3.program_name':'back-office'}) & \
            ~Q('term', **{'unstruct_event_ca_bc_gov_cfmspoc_chooseservice_3.channel':'Back Office'}) & \
            Q('range', derived_tstamp={'gte': now_minus_1_hour}) & \
            Q('range', derived_tstamp={'lte': "now"}) & \
            Q('range', derived_tstamp={'time_zone': "America/Vancouver"})
        try:
            events_search_result = Search(using=client, index=index).filter(params)
        except Exception as e:
            print(e)
    
        events = events_search_result.execute()
        agent_list = []
        for event in events:
            # pull out any agent id that appears in the result set from the last hour
            agent_list.append(event['contexts_ca_bc_gov_cfmspoc_agent_4'][0]['agent_id'])
            
        # Get the unique set of agent IDs for the last hour
        num_agents = len(list(set(agent_list)))
        
        
         # Calculate the current number of citizens in this office line
        line_size = add_citizen_count - (customer_left_count + finish_events_count)

        office_result = {
            "office_id": id,
            "current_line_length": line_size,
            "num_agents": num_agents
        }

        results_list.append(office_result)
        
    return results_list
    
    
# This function generates an html snippet for loading onto Service BC 
# Office pages in CMS Lite.
def build_wait_times_graph(api_response_data,all_offices):
    graph = ''
    if all_offices:
        graph += f"<div style=\"background-color: #f1f1f2;font-family: 'BC Sans', 'Noto Sans', Arial, 'sans serif';font-size: 16px;\">"
        for office in api_response_data:
            office_name = ''
            for center in service_centers:
                if int(center["cfms_poc.office_id"]) == int(office['office_id']):
                    office_name = center["cfms_poc.office_name"]
                    
                    
            link="https://www2.gov.bc.ca/gov/content/governments/organizational-structure/ministries-organizations/ministries/citizens-services/servicebc/service-bc-location-"
            link+=office_name.lower().replace(' ','-')
            
            if office['estimated_wait'] == "":
                wait = 0
            else:
                wait = int(office['estimated_wait'])
            hours = str(int(wait/60)).zfill(2)
            minutes = str(wait%60).zfill(2)
            graph += (f"<div><p><a href={link}>{office_name}</a></strong></p>"
                     f"<p><strong>Wait Times</strong></p>"
                     f"Number of Customers Currently in Line: "
                     f"{ str(office['current_line_length']) }"
                     f"</p><p>Estimated Wait Time in Minutes: " 
                     f"{ hours }:{ minutes }:00"
                     f"</p></div>")
        graph+=f"</p></div>"
    else:    
        for office in api_response_data:
            if office['estimated_wait'] == "":
                wait = 0
            else:
                wait = int(office['estimated_wait'])
            hours = str(int(wait/60)).zfill(2)
            minutes = str(wait%60).zfill(2)
            graph += (f"<div style=\"background-color: #f1f1f2;font-family: 'BC Sans', 'Noto Sans', Arial, 'sans serif';font-size: 16px;\">"
                f"<p><strong>Wait Times</strong></p>"
                f"Number of Customers Currently in Line: "
                f"{ str(office['current_line_length']) }"
                f"</p><p>Estimated Wait Time in Minutes: " 
                f"{ hours }:{ minutes }:00"
                f"</p></div>")
    return graph
    
