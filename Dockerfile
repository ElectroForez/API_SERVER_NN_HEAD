FROM ubuntu

ENV TZ=Europe/Moscow
ENV BASICSR_EXT=True
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt update && apt upgrade -y
RUN apt install python3 python3-pip git ffmpeg libsm6 libxext6 tzdata -y
WORKDIR /usr/src/
RUN git clone https://github.com/ElectroForez/server_nn-head.git
RUN git clone https://github.com/ElectroForez/video_nn.git
RUN pip install -r server_nn-head/requirments.txt -r video_nn/requirments.txt
ENV IS_DOCKER=1
WORKDIR /usr/src/server_nn-head
ENTRYPOINT ["python3", "server_head.py"]
