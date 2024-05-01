import boto3
import json
import uuid
import logging

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Define the helper function outside of lambda_handler
def get_text(result, blocks):
    text = ""
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    for block in blocks:
                        if block['Id'] == child_id:
                            if 'Text' in block:
                                text += block['Text']+ " "
    return text.strip()
def handle_with_llm(query):
    bedrock = boto3.client(service_name='bedrock-runtime')
    #print(query)
    # #prompt = "\n\nSystem: Your job is to convert given input to specific date format, examples\n\n\nHuman:"+query+"\n\nAssistant:"
    prompt = "nSystem: Your job is to give me a single value for each question with no explanation of the asked question\n\n\nHuman:"+query+"\n\nAssistant:"
    body = json.dumps({
        "prompt": prompt,
        "max_tokens_to_sample": 300,
        "temperature": 0.7
    })

    modelId = 'anthropic.claude-v2'
    accept = 'application/json'
    contentType = 'application/json'
    
    response = bedrock.invoke_model(body=body, modelId=modelId, accept=accept, contentType=contentType)
    
    response_body = json.loads(response.get('body').read())
    # text
    return response_body.get('completion')
    
def lambda_handler(event, context):
    try:
        # Initialize AWS clients
        s3_client = boto3.client('s3')
        textract = boto3.client('textract')
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('Invoice-xfpnmpybwzfilps3uo7bhgahte-staging')
    

        # Get the S3 bucket name and file key from the event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        document_key = event['Records'][0]['s3']['object']['key']
        
        logger.info(f"Processing file: {document_key} in bucket: {bucket_name}")
    # Call Textract to process the file
        response = textract.analyze_document(
            Document={'S3Object': {'Bucket': bucket_name, 'Name': document_key}},
            FeatureTypes=["FORMS"])
    except Exception as e:
        logger.error(f"Error processing file {document_key}: {e}")
        raise e  
        
    # Extract key-value pairs
    extracted_data = {}
    for item in response["Blocks"]:
        if item["BlockType"] == "KEY_VALUE_SET":
            if 'KEY' in item['EntityTypes']:
                # Find the corresponding value block
                for relationship in item['Relationships']:
                    if relationship['Type'] == 'VALUE':
                        for value_id in relationship['Ids']:
                            value_block = next(block for block in response['Blocks'] if block['Id'] == value_id)
                            key = get_text(item, response['Blocks'])
                            value = get_text(value_block, response['Blocks'])
                            extracted_data[key] = value
    
    print("Extracted data", extracted_data.keys())
    field_mapping = {
        'ACCOUNT REC. NO.': 'ACCOUNT_REC_NO',
        'TRAIN CONSIST': 'ACCOUNT_TRAIN_CONSIST',
        'APPROVED BY :': 'APPROVED_BY',
        "# CONTINUOUS HOURS :": 'CONTINUOUS_HOURS',
        'CHARGE (JOB) NO.': 'CHARGE_JOB_NO',
        "TRAIN CONSIST*": 'ChargeJobTRAIN_CONSIST',
        'CONTRACT': 'CONTRACT',
        'CONTRACTOR': 'CONTRACTOR',
        "Date:": 'DATE',
        "# DAYS / NIGHT :": 'DAYS_OR_NIGHT',
        "'GENERAL ORDER # :": 'GENERAL_ORDER_NUMBER',
        'LINE': 'LINE',
        "LOAD DATES & TIME :": 'LOAD_DATE_AND_TIME',
        "YARD :": 'LOAD_YARD',
        "PIGGYBACK WITH": 'PIGGYBACK_WITH',
        "REQUESTED :": 'REQUESTED',
        "SERVICE PLAN#": 'SERVICE_PLAN',
        "SPECIAL INSTRUCTIONS :": 'SPECIAL_INSTRUCTIONS',
        "SUBMITTED BY :": 'SUBMITTED_BY',
        "Tel:": 'TEL',
        "TRACK :": 'TRACK',
        "UNLOAD DATES & TIME :": 'UPLOAD_DATE_AND_TIME',
        'YARD': 'UPLOAD_YARD',
        'WORK DATES :': 'WORK_DATES',
        'WORK DAYS :': 'WORK_DAYS',
        'WORK HOURS :': 'WORK_HOURS',
        'WORK HOURS': 'WORK_HOURS',
        'WORK LOCATION :': 'WORK_LOCATION',
        'WORK TRAIN CONSIST :': 'WORK_TRAIN_CONSIST',
        'WORK TRAIN REQUEST NUMBER': 'WORK_TRAIN_REQUEST_NO'
           
    }
    
    # Transform the extracted data to match the DynamoDB schema
    transformed_data = {field_mapping.get(key, key): value for key, value in extracted_data.items()}
    print(transformed_data)
    unique_id = str(uuid.uuid4())
    
    #call llm
    workdays= transformed_data.get('WORK_DAYS')
    LOAD_DATE_AND_TIME= transformed_data.get('LOAD_DATE_AND_TIME')
    WORK_DATES= transformed_data.get('WORK_DATES')
    WORK_HOURS= transformed_data.get('WORK_HOURS')
    UPLOAD_DATE_AND_TIME= transformed_data.get('UPLOAD_DATE_AND_TIME')
    DATE= transformed_data.get('DATE')
    #work hours and 
    
    prompt= f"Now can you tell total number of working days for this {workdays} Give me just the working days a single value. Now this is a second prompt, You are aware of AWS datatypes, AWSDateTime is one if the data type.I want the load date and time in AWSDateTime. Give me load date and time for this {LOAD_DATE_AND_TIME} Give me just the load date and time a single value. This is the third prompt, You are aware of AWS datatypes, AWSDate is one if the data type. The given WORKDATES is in this format<WORKDATES>1/5/24 to 1/8/24'</WORKDATES> , I want the WORKDATES in AWSDate. There any be many dates given but just consider the start date <WORKDATES>1/5/24 to 1/8/24'</WORKDATES> date is 2024-01-05. Notice I have considered the start date only. Give me date for this {WORK_DATES} Give me just the work date a single value. This is the forth prompt, Total work hours for this is <WORKHOURS>2200to0500</WORKHOURS> 7 hours a day. The work starts from 22:00 and ends at 5:00, so from 22:00 to 5:00 it’s 7 hours. Give me work hours for this {WORK_HOURS}. The Work hours should be in integer format. I just want the total hours worked. This is the next prompt, same like LOADDATES&TIME convert UNLOADDATES&TIME into AWSDateTime. Give me upload date and time for this {UPLOAD_DATE_AND_TIME}. This is the next prompt, same like WORKDATES convert DATE into AWSDate. Give me date for this {DATE}"
    
    
    
    value= handle_with_llm(prompt)
    lst= value.split()
    
    transformed_data['WORK_DAYS']= lst[0]
    if len(lst)>4:
        transformed_data['LOAD_DATE_AND_TIME']= lst[1]
        transformed_data['WORK_DATES']= lst[2]
        transformed_data['WORK_HOURS']= lst[3]
        transformed_data['UPLOAD_DATE_AND_TIME']= lst[4]
        transformed_data['DATE']= lst[5]
        
    
    
   
    
    
    
    print(transformed_data)
    #Store transformed data in DynamoDB
    if transformed_data:
        response = table.put_item(
            Item={
                'id': unique_id,  # primary key in DynamoDB
                **transformed_data  # Transformed fields
            }
        )
        return {
            'statusCode': 200,
            'body': json.dumps('Transformed data stored in DynamoDB')
        }
    else:
        return {
            'statusCode': 400,
            'body': json.dumps('No data to store in DynamoDB')
        }