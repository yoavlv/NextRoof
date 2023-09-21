# Use an official Python runtime as the parent image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Define environment variable if needed (optional)
# ENV NAME World

# Command to run the script
# Here we use "sleep infinity" because we'll run the main script using a scheduling mechanism
CMD ["sleep", "infinity"]