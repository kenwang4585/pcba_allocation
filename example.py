#!/usr/bin/python3
from keystoneauth1 import session
from keystoneauth1.identity import v3
from swiftclient import Connection
from keystoneclient.v3 import client
from settings import cec_pw


authurl_='https://cloud-alln-1.cisco.com:5000/v3'
storageurl='https://cloud-alln-1-storage.cisco.com/swift/v1/c28a56e2e4f84c21bda26bc131e558fb'

auth = v3.Password(auth_url=authurl_,
                            username='kwang2',
                            user_domain_name='Default',
                            password=cec_pw,
                            project_name='Default',
                            project_domain_name='Default')

# Create session
session = session.Session(auth=auth)

# Create swiftclient Connection
conn = Connection(session=auth)

# Print containers
resp_headers, containers = conn.get_account()
print("Response headers: %s" % resp_headers)
for container in containers:
    print(container)
