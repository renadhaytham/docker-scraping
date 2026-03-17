FROM python:3.11.9
WORKDIR /app-SCRAPING
COPY requirements.txt /app-SCRAPING

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app-SCRAPING

CMD [ "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "800" ]