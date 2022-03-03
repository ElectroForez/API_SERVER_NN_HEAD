import cv2
import os
import sys
import subprocess
import glob
from datetime import datetime
from moviepy.editor import AudioFileClip


def improve_video(videofile, upd_videofile='untitled.avi', *args_realsr):  # Function to improve the video
    if not os.path.exists(videofile):
        print(f'File {videofile} not found')
        return -1
    if os.path.isdir(videofile):
        print(f'{videofile} it is a directory')
        return -1
    filename = videofile.split('/')[-1]  # take filename
    if upd_videofile.split('/')[-1].count('.') == 0:  # check path it's file or directory
        if not os.path.exists(upd_videofile):
            os.mkdir(upd_videofile)
        directory = upd_videofile
        upd_videofile += '/untitled.avi'  # it's path for a future file
    else:
        directory = videofile.split(filename)[0]
    if not directory.endswith('/'):
        directory += '/'
    fragments_path = filename.replace('.', '-') + '_fragments'
    upd_fragments_path = filename.replace('.', '-') + '_updated_fragments'
    upd_videofile_WOA = 'UWOA_' + filename  # UWOA - Updated without audio

    if directory:
        fragments_path = directory + fragments_path
        upd_fragments_path = directory  + upd_fragments_path
        upd_videofile_WOA = directory + upd_videofile_WOA

    for path in [fragments_path, upd_fragments_path]:
        if not os.path.exists(path):
            os.mkdir(path)
    if video_to_fragments(videofile, fragments_path) != 0:
        print('Error on function video to fragments')
        return -1

    subprocess.run(['cp', fragments_path + '/info.txt', fragments_path + '/audio.mp3', upd_fragments_path])

    finish_returncode = use_realsr(fragments_path, upd_fragments_path, *args_realsr)

    if finish_returncode != 0:
        print('Error on upscaling frames')
        return -1

    if glue_frames(upd_fragments_path, upd_videofile_WOA) != 0:
        print('Error on glue frames')
        return -1
    if add_audio(upd_videofile_WOA, fragments_path + '/audio.mp3', directory + upd_videofile) != 0:
        print('Error on adding audio')
        return -1

    return 0


def use_realsr(input_path, output_path, *args_realsr, realsr_path='./realsr-ncnn-vulkan/realsr-ncnn-vulkan'):
    t1 = datetime.now()
    finish = subprocess.run([realsr_path, '-i', input_path, '-o', output_path, *args_realsr])
    t2 = datetime.now()
    print('time cost realsr =', t2 - t1)
    return finish.returncode


def video_to_fragments(path, output_path=None):
    # check paths
    if not os.path.exists(path):
        print(path + " not exists")
        return
    elif os.path.isdir(path):
        print(path + " is not file")
        return
    elif output_path and output_path.split('/')[-1].count('.') > 0:
        print(output_path + " is not directory")
        return

    filename = path.split('/')[-1]

    if not output_path:
        output_path = filename.replace('.', '-') + "_fragments"
    if not os.path.exists(output_path):
        os.mkdir(output_path)
    if os.listdir(output_path):
        print('WARNING!!! Path for fragments is not empty. Files with the same name will be overwritten')

    t1 = datetime.now()
    videoCapture = cv2.VideoCapture()
    videoCapture.open(path)
    fps = videoCapture.get(cv2.CAP_PROP_FPS)
    frames = videoCapture.get(cv2.CAP_PROP_FRAME_COUNT)
    print("fps=", int(fps), "frames=", int(frames))

    count_frames = 0  # sometimes the wrong number of frames is displayed. Recalculation just in case
    for i in range(int(frames)):
        ret, frame = videoCapture.read()
        if ret:
            count_frames += 1
            cv2.imwrite("%s/%d.png" % (output_path, i), frame)
    t2 = datetime.now()
    if count_frames != frames:
        frames = count_frames
    print('time cost  = ', t2 - t1)

    # create a txt file with additional info for processing by other programs
    with open(output_path + '/info.txt', 'w') as infoFile:
        infoFile.write(str(int(fps)) + '\n')  # fps
        infoFile.write(filename + '\n')  # filename
        infoFile.write(str(int(frames)))  # frames
    # catching audio
    try:
        audioclip = AudioFileClip(path)
        audioclip.write_audiofile(output_path + '/audio.mp3')
        audioclip.close()
    except:
        print('video without audio')

    return 0


def glue_frames(src_path, videofile='untitled.avi', codec='h264', fps=30, *args_ffmpeg, photo_extenstion='png'):
    frames = glob.glob(src_path + '/*.' + photo_extenstion)
    if len(frames) == 0:
        print(f'Frames with extension {photo_extenstion} not found')
        return
    # frameSize = cv2.imread(frames[0]).shape[1::-1]
    filename = 'untitled.avi'

    if os.path.exists(src_path + '/info.txt'):
        with open(src_path + '/info.txt', 'r') as infoFile:
            try:
                fps = int(infoFile.readline())
                filename = 'UpdWOA_' + infoFile.readline().strip()  # Updated without audio
                count_frames = int(infoFile.readline())
                if count_frames != len(frames):
                    print('The number of files in info.txt does not match the actual')
            except:
                print("Bad info.txt")
    else:
        print("WARNING!!! info.txt not exists. It's true path?")

    if videofile.split('/')[-1].count('.') == 0:
        videofile += '/' + filename

    t1 = datetime.now()

    finish = subprocess.run(
        ['ffmpeg', '-start_number', '1', '-r', str(fps), '-i', src_path + '/%d.png', '-vcodec', codec, '-y',
         *args_ffmpeg, videofile])

    t2 = datetime.now()
    print('time cost qlue =', t2 - t1)

    return finish.returncode


def add_audio(videofile, audio_path, new_name=None):
    if not new_name:
        new_name = 'UPDATED_' + videofile.split('/')[-1]
    if videofile in (new_name, audio_path):
        print('Files with same name')
        return
    t1 = datetime.now()
    finish = subprocess.run(['ffmpeg', '-i', audio_path, '-i', videofile, '-codec', 'copy', '-y', new_name])
    t2 = datetime.now()
    print('time cost add audio = ', t2 - t1)
    return finish.returncode


if __name__ == '__main__':
    improve_video(*sys.argv[1:])