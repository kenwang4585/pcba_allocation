FROM python:3

MAINTAINER Kenwang

WORKDIR /pcba_allocation

#COPY Pipfile /pcba_allocation
COPY Pipfile.lock ./
COPY gunicorn.conf ./
COPY .env /pcba_allocation
#COPY ["Pipfile.lock","gunicorn.conf",".env"]
RUN pip install pipenv
RUN pipenv install --system --deploy --ignore-pipfile

COPY . .

CMD ["gunicorn", "wsgi:app", "-c", "gunicorn.conf"]

#CMD [ "python", "./wsgi.py" ]