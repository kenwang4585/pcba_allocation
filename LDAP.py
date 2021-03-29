from sqlalchemy import create_engine

engine = create_engine("ldap:///?User=CMERO&Password=rocme&Server=10.0.1.1&Port=389")