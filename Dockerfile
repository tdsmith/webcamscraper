FROM python:3.11
WORKDIR /usr/src/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENTRYPOINT ["python", "src/webcamscraper/__init__.py", "config.toml", "secrets.toml"]
