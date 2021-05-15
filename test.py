#!/usr/bin/python3
from keystoneauth1 import session
from keystoneauth1.identity import v3
from swiftclient import Connection


#authurl_='https://cloud-alln-1-storage.cisco.com/swift/v1/c28a56e2e4f84c21bda26bc131e558fb'
authurl_='https://cloud-alln-1.cisco.com:5000/v3'
storageurl='https://cloud-alln-1-storage.cisco.com/swift/v1/c28a56e2e4f84c21bda26bc131e558fb'

# Create a password auth plugin
auth = v3.Password(auth_url=authurl_,
                   username='kwang2',
                   password='ZIYIZIJUNyijun45852892!',
                   user_domain_name='Default',
                   project_name='Default',
                   project_domain_name='Default')

# Create session
keystone_session = session.Session(auth=auth)

# Create swiftclient Connection
swift_conn = Connection(session=keystone_session)

resp_headers, containers = swift_conn.get_account()
print("Response headers: %s" % resp_headers)
for container in containers:
    print(container)