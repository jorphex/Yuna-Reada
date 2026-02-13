# specify base image
FROM python:3.12-slim-bookworm

# set working directory
WORKDIR /app

# copy requirements file
COPY requirements.txt .

# install dependencies
RUN pip install --no-cache-dir --disable-pip-version-check --no-compile -r requirements.txt

# Copy the bot code into the container
COPY yunareada.py .

CMD [ "python", "yunareada.py" ]
