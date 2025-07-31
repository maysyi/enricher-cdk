import json
import boto3
from slugify import slugify
import urllib
from bs4 import BeautifulSoup
import datetime
import ssl
import time
import os

s3_id = os.environ['S3_ID']
db_id = os.environ['DB_ID']

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(db_id)
ssl_context = ssl._create_unverified_context()

class redirect_handler(urllib.request.HTTPRedirectHandler):
    def __init__(self):
        self.redirect_history = []

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        print(f"{code}: Redirecting to {newurl}")
        self.redirect_history.append((code, newurl))
        return super().redirect_request(req, fp, code, msg, headers, newurl)

def update_error(UploadFileName, TimeStamp, e, log_info):
    table.update_item(
        Key={
            'UploadFileName': UploadFileName,
            'TimeStamp': TimeStamp
        },
        UpdateExpression='SET html_status = :status, html_info = :info',
        ExpressionAttributeValues={
            ':status': str(e),
            ':info': {
                'html_log_info': log_info
            }
        }
    )

def lambda_handler(event, context):
    log_info = {
        'log_stream_name': context.log_stream_name,
        'log_group_name': context.log_group_name,
        'aws_request_id': context.aws_request_id
    }
    for record in event['Records']:
        sqs_payload = json.loads(record['body'])
        new_image = sqs_payload.get('MessageAttributes')
        UploadFileName = new_image['UploadFileName']['Value']
        TimeStamp = new_image['TimeStamp']['Value']
        if 'ip_address' in new_image:
            ip_or_domain = new_image['ip_address']['Value']
        elif 'domain' in new_image:
            ip_or_domain = new_image['domain']['Value']
        else:
            print("HTML/JS/APK unsuccessful (No IP or domain found)")
            continue
        
        if ip_or_domain.endswith(".apk"):
            print(f"HTML/JS/APK unsuccessful (is .apk file): {ip_or_domain}")
            continue
        
        foldername = slugify(ip_or_domain) + '_' + TimeStamp
        
        print(f"Starting HTML/JS/APK parse: {ip_or_domain}")
        current_time = time.time()
        success = 0
        html_file_location = []
        redirect_history = []
        for protocol in ["http", "https"]:
            filename = slugify(protocol + "://" + ip_or_domain) + '_' + TimeStamp
            query = f"{protocol}://{ip_or_domain}"
            print(f"Query: {query}")
            
            for tries in range(2):
                try:
                    time.sleep(2) # To prevent overwhelming server
                    redirects = redirect_handler()
                    opener = urllib.request.build_opener(
                        redirects,
                        urllib.request.HTTPSHandler(context=ssl_context)
                    )
                    req = urllib.request.Request(
                        url=query,
                        headers={
                            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
                        }
                    )
                    response = opener.open(req, timeout=5)
                    status_code = response.getcode()

                    if status_code >= 200 and status_code <= 299:
                        ### --- HTML --- ###
                        print(f"Compiled redirect history: {redirects.redirect_history}")
                        redirect_history.append(redirects.redirect_history)

                        headers = response.getheaders()
                        body = response.read()

                        with open("/tmp/output.html", "wb") as file:
                            for header, value in headers:
                                header_line = f"{header}: {value}\n"
                                file.write(header_line.encode())
                            file.write(b"\n") # Gap between headers and html
                            file.write(body)
                        
                        html_location = f"{UploadFileName}/html/{foldername}/{protocol}/{filename}.html"

                        s3.upload_file(
                            "/tmp/output.html",
                            s3_id,
                            html_location
                        )

                        print(f"HTML successful: Stored to enricher-prototype-s3/{html_location}")

                        table.update_item(
                            Key={
                                'UploadFileName': UploadFileName,
                                'TimeStamp': TimeStamp
                            },
                            UpdateExpression="SET html_status = :val1",
                            ExpressionAttributeValues={
                                ':val1': str(status_code)
                            },
                        )

                        success = 1
                        html_file_location.append(f"{s3_id}/{html_location}")

                        ### --- JS --- ###
                        html = BeautifulSoup(body, "html.parser")

                        js_files_link = []
                        js_filename = []
                        for script in html.find_all('script'):
                            if script.attrs.get("src"):
                                url = script.attrs.get("src")
                                if url[0:4] == "http": ## Also accounts for https
                                    js_files_link.append(url)
                                elif url[0:2] == "//":  ## e.g. //g.alicdn.com/alilog/mlog/aplus_v2.js
                                    js_files_link.append(protocol + ":"+ url)
                                elif url[0] in ['/','\\']:
                                    js_files_link.append(query+url)
                                elif url[0] not in ['/','\\']:
                                    js_files_link.append(query+ "/" + url)

                                filepath = slugify(url.split('.js')[0])
                                js_filename.append(filepath + '_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S%f'))

                        js_file_counter = 0
                        js_files_link = list(dict.fromkeys(js_files_link)) # Removes duplicates
                        js_filename = list(dict.fromkeys(js_filename))
                        print("All JS file links: ", js_files_link)

                        for each_js_link in js_files_link:
                            print("Starting GET for JS link: ", each_js_link)
                            js_location = f"{UploadFileName}/html/{foldername}/{protocol}/{js_filename[js_file_counter]}.js"
                            failed_js_files = []
                            for attempt in range(2):
                                try:
                                    js_req = urllib.request.Request(
                                        url=each_js_link, 
                                        headers={'User-Agent': 'Mozilla/5.0'}
                                    )
                                    each_js = urllib.request.urlopen(js_req, context=ssl_context, timeout=5)
                                    js_status_code = each_js.getcode()
                                    if js_status_code >= 200 and js_status_code <= 299:
                                        s3.put_object(
                                            Body=each_js.read(),
                                            Bucket=s3_id,
                                            Key=js_location,
                                            ContentType="application/javascript"
                                        )
                                        print(f"JS successful: Stored to {s3_id}/{js_location}")
                                        js_file_counter += 1
                                        break
                                    else:
                                        raise Exception
                                except Exception as e:
                                    if attempt == 1:
                                        print(f"JS unsuccessful: {ip_or_domain}---{TimeStamp} \n {e}")
                                        failed_js_files.append(each_js_link)
                                        break
                                    else:
                                        print(f"JS unsuccessful (Attempt {attempt+1} of 2)")
                                        continue
                            
                            if js_file_counter > 0:
                                table.update_item(
                                    Key={
                                        'UploadFileName': UploadFileName,
                                        'TimeStamp': TimeStamp
                                    },
                                    UpdateExpression="SET js_file_location = :val1",
                                    ExpressionAttributeValues={
                                        ':val1': f"{s3_id}/{UploadFileName}/html/{foldername}/"
                                    },
                                )

                            if failed_js_files:
                                log_info['failed_js'] = failed_js_files
                            
                        ### --- APK (Not tested) ---### 
                        apk_duplicate_check = []
                        for link in html.find_all('a', href=True):
                            is_apk = False
                            href = link.get('href')
                            if '.apk' in href:
                                if href.endswith(".apk"):
                                    is_apk = True
                                elif "?" in href:
                                    href_splitted = href.split('?')
                                    before_query = href_splitted[0]
                                    if before_query.endswith(".apk"):
                                        is_apk = True
                                
                                if (is_apk):
                                    absolute_url = urllib.parse.urljoin(query, href)
                                    if absolute_url in apk_duplicate_check:
                                        continue
                                    else:
                                        print("APK found: ", absolute_url)
                                        apk_duplicate_check.append(absolute_url)

                        if apk_duplicate_check:
                            apk_location = f"{UploadFileName}/html/{foldername}/{protocol}/{filename}.txt"
                            apk_body = str(apk_duplicate_check).encode('utf-8')
                            s3.put_object(
                                Body=apk_body,
                                Bucket=s3_id,
                                Key=apk_location
                            )
                            table.update_item(
                                    Key={
                                        'UploadFileName': UploadFileName,
                                        'TimeStamp': TimeStamp
                                    },
                                    UpdateExpression="SET apk_file_location = :val1",
                                    ExpressionAttributeValues={
                                        ':val1': f"{s3_id}/{apk_location}"
                                    },
                                )
                        else:
                            print("No APK found")
                    
                    else: # If status_code is not 2xx, raise Exception
                        raise Exception
                    
                    break # Stop retrying
                
                except Exception as e:
                    if tries == 1:
                        print(f"HTML/JS/APK unsuccessful: {filename}---{TimeStamp} \n {e}")
                        html_file_location.append(str(e))
                        log_info['duration'] = int(time.time() - current_time)
                        if success == 0:
                            if "522" in str(e):
                                update_error(UploadFileName, TimeStamp, "522", log_info)
                            elif "403" in str(e):
                                update_error(UploadFileName, TimeStamp, "403", log_info)
                            elif "404" in str(e):
                                update_error(UploadFileName, TimeStamp, "404", log_info)
                            elif "[Errno 16]" in str(e):
                                update_error(UploadFileName, TimeStamp, "16", log_info)
                            else:
                                update_error(UploadFileName, TimeStamp, e, log_info)
                    else: 
                        print(f"HTML/JS/APK unsuccessful (Attempt {tries+1} of 2)")
                        continue

        log_info['duration'] = int(time.time() - current_time)
        table.update_item(
            Key={
                'UploadFileName': UploadFileName,
                'TimeStamp': TimeStamp
            },
            UpdateExpression="SET html_info = :val1",
            ExpressionAttributeValues={
                ':val1': {
                    'html_file_location': html_file_location,
                    'redirect_history': redirect_history,
                    'html_log_info': log_info
                }
            }
        )
        
    return {
        'statusCode': 200,
        'body': json.dumps('HTML/JS/APK successful')
    }
