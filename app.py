import requests
import json
directory = r"/Users/zhangxiangxian/Desktop/test"
data = {'grant_type':"client_credentials",
        'resource':"https://graph.microsoft.com",
        'client_id':'ac8add5d-f910-4465-b620-849caebe99ed',
        'client_secret':'XXXXX'}
URL = "https://login.microsoftonline.com/95ce6199-ca36-4820-87c3-d540c860270c/oauth2/v2.0/token"
r = requests.post(url = URL, data = data)
j = json.loads(r.text)
TOKEN = j["access_token"]
URL = "https://graph.microsoft.com/v1.0/users/b07303058@ntu.edu.tw/drive/root:/fotos/HouseHistory"
headers={'Authorization': "Bearer " + TOKEN}
r = requests.get(URL, headers=headers)
j = json.loads(r.text)
print("Uploading file(s) to "+URL)
for root, dirs, files in os.walk(directory):
    for filename in files:
        filepath = os.path.join(root,filename)
        print("Uploading "+filename+"....")
        fileHandle = open(filepath, 'rb')
        r = requests.put(URL+"/"+filename+":/content", data=fileHandle, headers=headers)
        fileHandle.close()
        if r.status_code == 200 or r.status_code == 201:
            #remove folder contents
            print("succeeded, removing original file...")
            os.remove(os.path.join(root, filename))
print("Script completed")
raise SystemExit