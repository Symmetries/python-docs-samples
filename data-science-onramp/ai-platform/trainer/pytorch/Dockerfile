# Dockerfile
FROM python:3.7-buster

# Create working directory
WORKDIR /root/pytorch

# Install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Install source files
COPY *.py .

# Set model trainer as entrypoint
ENTRYPOINT ["python3", "task.py"]
