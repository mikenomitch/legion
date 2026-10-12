FROM public.ecr.aws/docker/library/python:3.12-alpine
RUN pip install --no-cache-dir pg8000==1.31.2 redis==5.2.1
COPY app.py /app/app.py
ENV PYTHONUNBUFFERED=1
CMD ["python3", "/app/app.py"]
