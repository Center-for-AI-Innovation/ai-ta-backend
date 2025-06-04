from rmqueue import Queue

active_queue = Queue()
test_data = {
    "course_name": "test_course",
    "s3_paths": ["s3://uiuc-chat-rabbitmq-test/1.txt"],
    "readable_filename": "1.txt",
    "groups": ["group1"]
}
result = active_queue.addJobToIngestQueue(test_data)
print(result)
