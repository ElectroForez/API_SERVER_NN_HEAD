import cv2
import os
import sys
import subprocess
import glob
from datetime import datetime
from moviepy.editor import AudioFileClip


def print_timecost(func):
    def wrapper(*args, **kwargs):
        t1 = datetime.now()
        statuscode = func(*args, **kwargs)
        t2 = datetime.now()
        print(f'time cost {func.__name__} =', t2 - t1)
        return statuscode
    return wrapper


@print_timecost
def improve_video(videofile, upd_videofile='untitled.avi', *args_realsr, func_upscale):
    """
    improve quality of video using frame-by-frame processing.
    func_upscale processes frames
    """
    if not os.path.exists(videofile):
        print(f'File {videofile} not found')
        return -1
    if os.path.isdir(videofile):
        print(f'{videofile} it is a directory')
        return -1
    filename = videofile.split('/')[-1]  # take filename
    if upd_videofile.split('/')[-1].count('.') == 0:  # check path that it's directory
        if not os.path.exists(upd_videofile):
            os.mkdir(upd_videofile)
        directory = upd_videofile
        upd_videofile += 'untitled.avi'  # it's path for a future file
    else:
        directory = videofile.split(filename)[0]
    if not directory.endswith('/'):
        directory += '/'

    fragments_path = filename.replace('.', '-') + '_fragments/'
    upd_fragments_path = 'Updated_' + fragments_path
    upd_videofile_WOA = 'UWOA_' + filename  # UWOA - Updated without audio

    if directory:
        fragments_path = directory + fragments_path
        upd_fragments_path = directory + upd_fragments_path
        upd_videofile_WOA = directory + upd_videofile_WOA

    for path in [fragments_path, upd_fragments_path]:
        if not os.path.exists(path):
            os.mkdir(path)

    if video_to_fragments(videofile, fragments_path) != 0:
        print('Error on function video to fragments')
        return -1

    # copy info and audio of video to path with updated fragments
    subprocess.run(['cp', fragments_path + 'info.txt', fragments_path + 'audio.mp3', upd_fragments_path])

    return_code_upscale = func_upscale(fragments_path, upd_fragments_path, *args_realsr)
    if return_code_upscale != 0:
        print('Error on upscaling frames')
        return -1

    return_code_glue = glue_frames(upd_fragments_path, upd_videofile_WOA)
    if return_code_glue != 0:
        print('Error on glue frames')
        return -1

    return_code_audio = add_audio(upd_videofile_WOA, fragments_path + 'audio.mp3', upd_videofile)
    if return_code_audio != 0:
        print('Error on adding audio')
        return -1
    return 0


@print_timecost
def use_realsr(input_path, output_path, *args_realsr, realsr_path='./realsr-ncnn-vulkan/realsr-ncnn-vulkan'):
    """up-scaling frames"""
    finish = subprocess.run([realsr_path, '-i', input_path, '-o', output_path, *args_realsr])
    return finish.returncode


@print_timecost
def video_to_fragments(video_path, output_path=None):
    """split video into frames and audio, & write some info in info.txt"""
    # check paths
    if output_path.endswith('/'):
        output_path = output_path[:-1]

    if not os.path.exists(video_path):
        print(video_path + " not exists")
        return -1
    elif os.path.isdir(video_path):
        print(video_path + " is not file")
        return -1
    elif output_path and output_path.split('/')[-1].count('.') > 0:
        print(output_path + " is not directory")
        return - 1

    output_path += '/'
    filename = video_path.split('/')[-1]

    if not output_path:
        output_path = filename.replace('.', '-') + "_fragments"
    if not os.path.exists(output_path):
        os.mkdir(output_path)
    if os.listdir(output_path):
        print(f'WARNING!!! Path for fragments {output_path} is not empty. Files with the same name will be overwritten')

    videoCapture = cv2.VideoCapture()
    videoCapture.open(video_path)
    fps = videoCapture.get(cv2.CAP_PROP_FPS)
    frames = videoCapture.get(cv2.CAP_PROP_FRAME_COUNT)
    print("fps=", int(fps), "frames=", int(frames))

    count_frames = 0  # sometimes the wrong number of frames is displayed. Recalculation just in case
    for i in range(int(frames)):
        ret, frame = videoCapture.read()
        if ret:
            count_frames += 1
            cv2.imwrite("%s/%d.png" % (output_path, i), frame)
    if count_frames != frames:
        frames = count_frames

    # create a txt file with additional info for processing by other programs
    with open(output_path + 'info.txt', 'w') as infoFile:
        infoFile.write(str(int(fps)) + '\n')  # fps
        infoFile.write(filename + '\n')  # filename
        infoFile.write(str(int(frames)))  # frames

    # catching audio
    try:
        audioclip = AudioFileClip(video_path)
        audioclip.write_audiofile(output_path + 'audio.mp3')
        audioclip.close()
    except IndexError:
        print('video without audio')

    return 0


def glue_frames(src_path, videofile='untitled.avi', codec='h264', fps=30, *args_ffmpeg, photo_extenstion='png'):
    frames = glob.glob(src_path + '/*.' + photo_extenstion)
    if len(frames) == 0:
        print(f'Frames with extension {photo_extenstion} not found')
        return
    filename = 'untitled.avi'

    if os.path.exists(src_path + '/info.txt'):
        with open(src_path + '/info.txt', 'r') as infoFile:
            try:
                fps = int(infoFile.readline())
                filename = 'UpdWOA_' + infoFile.readline().strip()  # Updated without audio
                count_frames = int(infoFile.readline())
                if count_frames != len(frames):
                    print('The number of files in info.txt does not match the actual')
            except ValueError:
                print("Bad info.txt")
    else:
        print("WARNING!!! info.txt not exists. It's true path?")

    if videofile.split('/')[-1].count('.') == 0:
        videofile += '/' + filename

    finish = subprocess.run(
        ['ffmpeg', '-start_number', '1', '-r', str(fps), '-i', src_path + '/%d.png', '-vcodec', codec, '-y',
         *args_ffmpeg, videofile])

    return finish.returncode


@print_timecost
def add_audio(videofile, audio_path, new_name=None):
    if not new_name:
        new_name = 'UPDATED_' + videofile.split('/')[-1]
    if videofile in (new_name, audio_path):
        print('Files with same name')
        return
    finish = subprocess.run(['ffmpeg', '-i', audio_path, '-i', videofile, '-codec', 'copy', '-y', new_name])
    return finish.returncode


if __name__ == '__main__':
    improve_video(*sys.argv[1:], func_upscale=use_realsr)
