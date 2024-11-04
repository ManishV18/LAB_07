FROM xserrat/facebook-demucs:latest
RUN  pip3 install --upgrade redis requests minio
COPY worker.py worker.py
COPY docker-facebook-demucs docker-facebook-demucs
ENTRYPOINT ["python3", "worker.py"]