FROM public.ecr.aws/amazonlinux/amazonlinux:latest

RUN amazon-linux-extras enable python3.8 && \
    yum install -y python3.8 python38-devel gcc

COPY submission-finder/main.py /app/main.py
COPY common /app/src/common
COPY requirements.txt /app/requirements.txt

WORKDIR /app
RUN python3.8 -m pip install -r requirements.txt

CMD ["python3.8", "main.py"]
