FROM julia:1.4-buster

RUN apt install -y python3 python3-pip && \
    pip3 install pyarrow==0.17.0 pandas==1.0.3
