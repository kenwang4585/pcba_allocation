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
CMD ["pipenv", "run","gunicorn", "-w", "2", "-b", "0.0.0.0:8083", "wsgi:app"]

# use below command to build this image
# docker build -t pcba_allocation .
# use below command to create and run the container based on this image. below map 8083 to 80
# docker run -p 80:8083/tcp pcba_allocation
