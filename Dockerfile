FROM python:3.9

RUN mkdir /opt/app
WORKDIR /opt/app

COPY requirements.txt /opt/app/requirements.txt
RUN pip install -r requirements.txt

COPY ./src /opt/app

CMD ["python", "toponim.py", "--path_from", "/opt/app/messages", "--path_to", "/opt/app/messages"]
