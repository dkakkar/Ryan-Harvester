FROM python:2.7.11

WORKDIR /usr/local/src/ryan_geotweet_harvester

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt



# Do this last; most likely to change
COPY src/Ryan_Harvester_modified.py ./

CMD ["python2", "Ryan_Harvester_modified.py"]
