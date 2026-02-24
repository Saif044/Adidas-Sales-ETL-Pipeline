FROM astrocrpublic.azurecr.io/runtime:3.1-12

COPY requirements.txt .
RUN pip install -r requirements.txt