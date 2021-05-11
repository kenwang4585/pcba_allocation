from keystoneauth1 import session
from keystoneauth1.identity import v3
from swiftclient import Connection

# Create a password auth plugin
auth = v3.Password(auth_url='https://cloud-alln-1-storage.cisco.com/swift/v1/c28a56e2e4f84c21bda26bc131e558fb/kw_openstack_sortage_container',
                   username='kwang2',
                   password='ZIYIZIJUNyijun45852892!',
                   user_domain_name='Default',
                   project_name='Default',
                   project_domain_name='Default')


# Create session
#keystone_session = session.Session(auth=auth)
# Create swiftclient Connection
#swift_conn = Connection(session=keystone_session)


conn = Connection(
                authurl='https://cloud-alln-1-storage.cisco.com/swift/v1/c28a56e2e4f84c21bda26bc131e558fb',
                user='kwang2',
                key='ZIYIZIJUNyijun45852892!',
                #tenant_name='ken_wang',
                auth_version='3',
                os_options={
                    'user_domain_name': 'Default',
                    'project_domain_name': 'Default',
                    'project_name': 'Default'
                            }
                )


resp_headers, containers = conn.get_account()
print("Response headers: %s" % resp_headers)
for container in containers:
    print(container)