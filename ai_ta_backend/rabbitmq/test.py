from queue import Queue

active_queue = Queue()
test_data = {
    "course_name": "test_course",
    "s3_paths": ["s3://example-bucket/test_file.txt"],
    "readable_filename": "test_file.txt",
    "url": "http://example.com/test_file.txt",
    "groups": ["group1", "group2"]
}
result = active_queue.addJobToIngestQueue(test_data)
print(result)
