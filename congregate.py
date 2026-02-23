import os, requests
url = "https://api.box.com/oauth2/token"
data = {
    'grant_type': 'client_credentials',
    'client_id': os.environ.get('BOX_CLIENT_ID'),
    'client_secret': os.environ.get('BOX_CLIENT_SECRET'),
    'box_subject_type': 'enterprise',
    'box_subject_id': '1444288525' # Ensure this matches your Enterprise ID
}
token = requests.post(url, data=data).json().get('access_token')
me = requests.get("https://api.box.com/2.0/users/me", headers={'Authorization': f'Bearer {token}'}).json()
print(f"I am logged in as: {me.get('login')}")
