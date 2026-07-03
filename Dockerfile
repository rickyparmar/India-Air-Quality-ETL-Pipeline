FROM apache/airflow:2.9.1

# Copy the requirements file into the image
COPY requirements.txt .

# Install dependencies using the airflow user
RUN pip install --no-cache-dir -r requirements.txt
