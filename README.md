# server_nn-head
This is a program that sends frames from a video to remote servers of the [server_nn-api](https://github.com/ElectroForez/server_nn-api.git) for their further processing and receives them back.
Based on [video_nn](https://github.com/ElectroForez/video_nn.git)

[Docker](https://hub.docker.com/repository/docker/forez/server_nn-head)
# requirments
requests==2.22.0

opencv-python==4.5.5.62

moviepy==1.0.3

ffmpeg
# install
```
git clone https://github.com/ElectroForez/server_nn-head.git
git clone https://github.com/ElectroForez/video_nn.git
pip install -r server_nn-head/requirments.txt -r video_nn/requirments.txt
sudo apt install ffmpeg
```
# usage
```
export PASS_HEAD=YOUR_PASSWORD_FOR_API_SERVERS
$ python3 server_head.py --help
usage: Server API [-h] -i INPUT [-o OUTPUT] [-r REALSR ARGS]

Head for server nn. Improve video on remote servers

options:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        Input path for video
  -o OUTPUT, --output OUTPUT
                        Output path for video. Temporary files will be stored in the same path.
  -r REALSR ARGS, --realsr REALSR ARGS
```
# example
```
export PASS_HEAD=password
$ python3 server_head.py -i /home/vladt/video_proc/upbar.mp4 -o /home/vladt/video_proc/UpdBar/updated_video.mp4 -r "-s 4"
```
