FROM python:3.9-slim


# Install necessary tools
RUN apt update && \
    apt install -y curl && \
    rm -rf /var/lib/apt/lists/*

ADD https://storage.googleapis.com/google-code-archive-downloads/v2/code.google.com/badwordslist/badwords.txt /opt/bad_words.txt

WORKDIR /app


    
RUN ls -la /opt/bad_words.txt

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Expose Streamlit's default port
EXPOSE 8501


# Run the Streamlit app
CMD ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]