FROM ubuntu

RUN apt update && apt upgrade -y
RUN apt install python3 python3-pip git -y
WORKDIR /usr/src/
RUN git clone https://github.com/ElectroForez/API_server_nn_head.git
RUN git clone https://github.com/ElectroForez/video_nn.git
RUN pip install -r API_server_nn_head/requirments.txt -r video_nn/requirments.txt
ENV TZ=Europe/Moscow
RUN pip install opencv-python-headless
WORKDIR /usr/src/API_server_nn_head



