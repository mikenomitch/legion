FROM public.ecr.aws/docker/library/python:3.12-alpine
COPY app.py /app/app.py
# Legion sets LEGION_PORT_HTTP; PORT is the fallback for `docker run`.
ENV PORT=8080 PYTHONUNBUFFERED=1
CMD ["python3", "/app/app.py"]
