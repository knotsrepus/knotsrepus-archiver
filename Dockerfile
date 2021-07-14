FROM rayproject/ray:1.4.0-py38
USER root

COPY . /app
WORKDIR /app

VOLUME /app/out

RUN ["python", "-m", "pip", "install", "-r", "requirements.txt"]

EXPOSE 8265

ENTRYPOINT ["./run.sh"]
CMD ["--local", "out/", "-a", "2021-05-31T23:00:00Z", "-b", "2021-06-01T00:00:00Z", "-c", "2", "-m", "2"]