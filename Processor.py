import json
from Uploader import Uploader
import copy
import datetime
import logging
import os
import shutil
import subprocess
from itertools import groupby
from typing import Dict, List, Tuple

import ffmpeg
import jsonlines

import utils
from BiliLive import BiliLive


def parse_danmu(dir_name):
    danmu_list = []
    if os.path.exists(os.path.join(dir_name, 'danmu.jsonl')):
        with jsonlines.open(os.path.join(dir_name, 'danmu.jsonl')) as reader:
            for obj in reader:
                danmu_list.append({
                    "text": obj['text'],
                    "time": obj['properties']['time']//1000
                })
    if os.path.exists(os.path.join(dir_name, 'superchat.jsonl')):
        with jsonlines.open(os.path.join(dir_name, 'superchat.jsonl')) as reader:
            for obj in reader:
                danmu_list.append({
                    "text": obj['text'],
                    "time": obj['time']
                })
    danmu_list = sorted(danmu_list, key=lambda x: x['time'])
    return danmu_list


def get_cut_points(time_dict: Dict[datetime.datetime, List[str]], up_ratio: float = 2, down_ratio: float = 0.75, topK: int = 5) -> List[Tuple[datetime.datetime, datetime.datetime, List[str]]]:
    status = 0
    cut_points = []
    prev_num = None
    start_time = None
    temp_texts = []
    for time, texts in time_dict.items():
        if prev_num is None:
            start_time = time
            temp_texts = copy.copy(texts)
        elif status == 0 and len(texts) >= prev_num*up_ratio:
            status = 1
            temp_texts.extend(texts)
        elif status == 1 and len(texts) < prev_num*down_ratio:
            tags = utils.get_words("。".join(texts), topK=topK)
            cut_points.append((start_time, time, tags))
            status = 0
            start_time = time
            temp_texts = copy.copy(texts)
        elif status == 0:
            start_time = time
            temp_texts = copy.copy(texts)
        prev_num = len(texts)
    return cut_points


def get_true_timestamp(video_times: List[Tuple[datetime.datetime, float]], point: datetime.datetime) -> float:
    time_passed = 0
    for t, d in video_times:
        if point < t:
            return time_passed
        elif point - t <= datetime.timedelta(seconds=d):
            return time_passed + (point - t).total_seconds()
        else:
            time_passed += d
    return time_passed


def count(danmu_list: List, live_start: datetime.datetime, live_duration: float, interval: int = 60) -> Dict[datetime.datetime, List[str]]:
    start_timestamp = int(live_start.timestamp())
    return_dict = {}
    for k, g in groupby(danmu_list, key=lambda x: (x['time']-start_timestamp)//interval):
        return_dict[datetime.datetime.fromtimestamp(
            k*interval+start_timestamp)] = []
        for o in list(g):
            return_dict[datetime.datetime.fromtimestamp(
                k*interval+start_timestamp)].append(o['text'])
    return return_dict


def flv2ts(input_file: str, output_file: str, ffmpeg_logfile_hander) -> subprocess.CompletedProcess:
    ret = subprocess.run(f"ffmpeg -y -fflags +discardcorrupt -i {input_file} -c copy -bsf:v h264_mp4toannexb -f mpegts {output_file}",
                         shell=True, check=True, stdout=ffmpeg_logfile_hander, stderr=ffmpeg_logfile_hander)
    return ret


def concat(merge_conf_path: str, merged_file_path: str, ffmpeg_logfile_hander) -> subprocess.CompletedProcess:
    ret = subprocess.run(f"ffmpeg -y -f concat -safe 0 -i {merge_conf_path} -c copy -fflags +igndts -avoid_negative_ts make_zero {merged_file_path}",
                         shell=True, check=True, stdout=ffmpeg_logfile_hander, stderr=ffmpeg_logfile_hander)
    return ret


def get_start_time(filename: str) -> datetime.datetime:
    base = os.path.splitext(filename)[0]
    return datetime.datetime.strptime(
        " ".join(base.split("_")[1:3]), '%Y-%m-%d %H-%M-%S')


class Processor(BiliLive):
    def __init__(self, config: dict, global_start: datetime.datetime):
        super().__init__(config)
        self.config = config
        self.global_start = global_start
        self.record_dir = utils.init_record_dir(
            self.room_id, self.global_start, config['root']['data_path'])
        self.danmu_path = utils.init_danmu_log_dir(
            self.room_id, self.global_start, config['root']['data_path'])
        self.merge_conf_path = utils.get_merge_conf_path(
            self.room_id, self.global_start, config['root']['data_path'])
        self.merged_file_path = utils.get_merged_filename(
            self.room_id, self.global_start, config['root']['data_path'])
        self.outputs_dir = utils.init_outputs_dir(
            self.room_id, self.global_start, config['root']['data_path'])
        self.splits_dir = utils.init_splits_dir(
            self.room_id, self.global_start, config['root']['data_path'])
        self.times = []
        self.live_start = self.global_start
        self.live_duration = 0
        self.ffmpeg_logfile = os.path.join(config['root']['logger']['log_path'], "FFMpeg_"+datetime.datetime.now(
        ).strftime('%Y-%m-%d_%H-%M-%S')+'.log')
        self.ffmpeg_logfile_hander = open(
            self.ffmpeg_logfile, mode="a", encoding="utf-8")

    def pre_concat(self) -> None:
        filelist = os.listdir(self.record_dir)
        with open(self.merge_conf_path, "w", encoding="utf-8") as f:
            for filename in filelist:
                file_path = os.path.join(self.record_dir, filename)
                if os.path.splitext(file_path)[1] == ".flv" and os.path.getsize(file_path) > 1024*1024:
                    ts_path = os.path.splitext(file_path)[0]+".ts"
                    _ = flv2ts(file_path, ts_path, self.ffmpeg_logfile_hander)
                    if not self.config['spec']['recorder']['keep_raw_record']:
                        os.remove(file_path)
                    # ts_path = os.path.join(self.record_dir, filename)
                    duration = float(ffmpeg.probe(ts_path)[
                                     'format']['duration'])
                    start_time = get_start_time(filename)
                    self.times.append((start_time, duration))
                    f.write(
                        f"file '{os.path.abspath(ts_path)}'\n")
        _ = concat(self.merge_conf_path, self.merged_file_path,
                   self.ffmpeg_logfile_hander)
        self.times.sort(key=lambda x: x[0])
        self.live_start = self.times[0][0]
        self.live_duration = (
            self.times[-1][0]-self.times[0][0]).total_seconds()+self.times[-1][1]

    def __cut_video(self, outhint: List[str], start_time: int, delta: int) -> subprocess.CompletedProcess:
        output_file = os.path.join(
            self.outputs_dir, f"{self.room_id}_{self.global_start.strftime('%Y-%m-%d_%H-%M-%S')}_{start_time:012}_{outhint}.mp4")
        cmd = f'ffmpeg -y -ss {start_time} -t {delta} -accurate_seek -i "{self.merged_file_path}" -c copy -avoid_negative_ts 1 "{output_file}"'
        ret = subprocess.run(cmd, shell=True, check=True,
                             stdout=self.ffmpeg_logfile_hander)
        return ret

    def cut(self, cut_points: List[Tuple[datetime.datetime, datetime.datetime, List[str]]], min_length: int = 60) -> None:
        duration = float(ffmpeg.probe(self.merged_file_path)
                         ['format']['duration'])
        for cut_start, cut_end, tags in cut_points:
            start = get_true_timestamp(self.times,
                                       cut_start) + self.config['spec']['clipper']['start_offset']
            end = min(get_true_timestamp(self.times,
                                         cut_end) + self.config['spec']['clipper']['end_offset'], duration)
            delta = end-start
            outhint = " ".join(tags)
            if delta >= min_length:
                self.__cut_video(outhint, max(
                    0, int(start)), int(delta))

    def split(self, split_interval: int = 3600) -> None:
        if split_interval <= 0:
            shutil.copy2(self.merged_file_path, os.path.join(
                self.splits_dir, f"{self.room_id}_{self.global_start.strftime('%Y-%m-%d_%H-%M-%S')}_0000.mp4"))
            return

        duration = float(ffmpeg.probe(self.merged_file_path)
                         ['format']['duration'])
        num_splits = int(duration) // split_interval + 1
        for i in range(num_splits):
            output_file = os.path.join(self.splits_dir, f"{i}.mp4")
            cmd = f'ffmpeg -y -ss {i*split_interval} -t {split_interval} -accurate_seek -i "{self.merged_file_path}" -c copy -avoid_negative_ts 1 "{output_file}"'
            _ = subprocess.run(cmd, shell=True, check=True,
                               stdout=self.ffmpeg_logfile_hander, stderr=self.ffmpeg_logfile_hander)

    def run(self) -> None:
        logging.basicConfig(level=utils.get_log_level(self.config),
                            format='%(asctime)s %(thread)d %(threadName)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=os.path.join(self.config['root']['logger']['log_path'], "Processor_"+datetime.datetime.now(
                            ).strftime('%Y-%m-%d_%H-%M-%S')+'.log'),
                            filemode='a')
        try:
            self.pre_concat()
            if not self.config['spec']['recorder']['keep_raw_record']:
                if os.path.exists(self.merged_file_path):
                    utils.del_files_and_dir(self.record_dir)
        except Exception as e:
            logging.error("文件转码出现错误："+str(e))
        # duration = float(ffmpeg.probe(self.merged_file_path)[
        #                              'format']['duration'])
        # start_time = get_start_time(self.merged_file_path)
        # self.times.append((start_time, duration))
        # self.live_start = self.times[0][0]
        # self.live_duration = (
        #     self.times[-1][0]-self.times[0][0]).total_seconds()+self.times[-1][1]

        try:
            if self.config['spec']['clipper']['enable_clipper']:
                danmu_list = parse_danmu(self.danmu_path)
                paser_config: dict = self.config['spec']['parser']
                counted_danmu_dict = count(
                    danmu_list, self.live_start, self.live_duration, paser_config['interval'])
                cut_points = get_cut_points(counted_danmu_dict, paser_config['up_ratio'],
                                            paser_config['down_ratio'], paser_config['topK'])
                self.cut(
                    cut_points, self.config['spec']['clipper']['min_length'])
        except Exception as e:
            logging.error("切片出现错误："+str(e))
        try:
            if self.config['spec']['uploader']['record']['upload_record']:
                self.split(self.config['spec']['uploader']
                           ['record']['split_interval'])
        except Exception as e:
            logging.error("文件切分出现错误："+str(e))


if __name__ == "__main__":
    with open("config/config.json", "r", encoding="UTF-8") as f:
        all_config = json.load(f)
    root_config: dict = all_config.get('root', {})
    spec_config = all_config.get('spec', [])[0]
    config = {
        'root': root_config,
        'spec': spec_config,
        'password_path': "config/passwd.json"
    }
    p = Processor(config, "data/records/5561470_2021-11-18_00-18-00",
                  "data/danmu/5561470_2021-11-18_00-18-00")
    p.split(3600)
    u = Uploader(p.outputs_dir, p.splits_dir,
                 config, '【歌杂】唱歌啦~')
    d = u.upload(p.global_start)
