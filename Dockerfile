FROM python:3.10-slim-buster

WORKDIR /app

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY . .

# Create output directory and mark it as a volume
VOLUME /app/output_files

EXPOSE 4000

CMD [ "flask", "run", "--host=0.0.0.0", "--port=4000"]