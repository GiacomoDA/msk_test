import requests
import threading
import time

# URL format
base_url = "http://3.76.221.107:8080/?topic=test_topic1&value={value}&request_id={request_id}"

num_requests = 10000
# time between requests in milliseconds
interval = 2

first_reply_time = None
last_reply_time = None
replies_received = 0
timeouts = 0
lock = threading.Lock()

def send_request(i):
    global first_reply_time, last_reply_time, replies_received, timeouts
    
    value = f"{i:03}"
    request_id = f"{i:03}"
    
    url = base_url.format(value=value, request_id=request_id)
    
    try:
        send_time = time.time()
        response = requests.get(url)
        request_time = (time.time() - send_time) * 1000
        
        print(f"Request {i:03}: Status Code {response.status_code}, value {value}, Time: {request_time:.0f}ms, Response: {response.text}")


        with lock:
            if response.status_code == 504:
                timeouts += 1
            replies_received += 1
            if replies_received == 1:
                first_reply_time = time.time()
            last_reply_time = time.time()
            
    except Exception as e:
        print(f"Request {i} failed: {e}")
threads = []
for i in range(1, num_requests + 1):
    thread = threading.Thread(target=send_request, args=(i,))
    threads.append(thread)
    thread.start()
    
    time.sleep(interval * 0.001)

for thread in threads:
    thread.join()

if first_reply_time is not None and last_reply_time is not None:
    total_time = last_reply_time - first_reply_time
    print(f"Total time between the first and last replies: {total_time:.3f} seconds")
    print(f"Total timeouts: {timeouts}")
else:
    print("No replies received.")
