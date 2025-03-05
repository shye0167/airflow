import requests

client_id = '92be748625ab9e04bb0b828bf523b656' #REST API
redirect_uri = 'https://example.com/oauth' 
authorize_code = 'kudZJnXyXg2WjGcbE5cSwJnivQzIV_ETiVLtQ-UoEr42RGGGYOFolgAAAAQKKiWOAAABlUTBF5SoblpFv_zasg'

token_url = 'https://kauth.kakao.com/oauth/token'
data = {
    'grant_type': 'authorization_code',
    'client_id': client_id,
    'redirect_uri': redirect_uri,
    'code': authorize_code,    
}

response = requests.post(token_url, data=data)

try:
    tokens = response.json()
    print(tokens)
except requests.exceptions.JSONDecodeError:
    print("Error: Response is not in JSON format")
    print(response.text)
