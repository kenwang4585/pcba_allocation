FROM python:3

LABEL maintainer="Kenwang"

#WORKDIR /pcba_allocation

COPY Pipfile .
COPY Pipfile.lock .
#COPY gunicorn.conf .
COPY .env .
#COPY [".env",".env"]
RUN pip install pipenv
RUN pipenv install --system --deploy --ignore-pipfile

COPY . .

EXPOSE 8083

#CMD ["pipenv", "run","gunicorn","wsgi:app","-c", "gunicorn.conf"]
CMD ["pipenv", "run","gunicorn", "-w", "2", "-b", "127.0.0.1:8083", "wsgi:app"]
