import requests

def test_warm_ai():
    url = "http://localhost:8000/api/chat-api/warm-ai"
    
    user_query = input("Enter your query: ")
    
    response = requests.post(url, json={"query": user_query})
        
    try:
        if response.status_code == 200:
            print("\n" + "="*50)
            print("User Query:", response.json()['response']['results']['input'])
            print("-"*50)
            print("Response:", response.json()['response']['results']['output'])
            print("="*50 + "\n")
        else:
            print(f"Error Status Code: {response.status_code}")
            print("Response Text:", response.text)
    except requests.exceptions.JSONDecodeError:
        print(f"Failed to decode JSON. Status Code: {response.status_code}")
        print("Raw Response:", response.text)
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    test_warm_ai()