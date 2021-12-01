import datetime
import json
import logging
import os
import sys
import threading
import time
from logging.handlers import RotatingFileHandler
from multiprocessing import freeze_support

from lastversion import lastversion

import utils
from MainRunner import MainThreadRunner

CURRENT_VERSION = "1.1.9.1"


class versionThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        latest_version = lastversion.has_update(
            repo="https://github.com.cnpmjs.org/AsaChiri/DDRecorder", current_version=CURRENT_VERSION)
        if latest_version:
            print('DDRecorder有更新，版本号: {} 请尽快到https://github.com/AsaChiri/DDRecorder/releases 下载最新版'.format(str(latest_version)))
        else:
            print('DDRecorder已是最新版本！')


def initroot(root_config: dict):
    root_config.setdefault('check_interval', 100)
    root_config.setdefault('print_interval', 60)
    root_config.setdefault('data_path', './')
    root_config.setdefault('request_header', {})
    root_config.setdefault('enable_baiduyun', False)

    logger_config: dict = root_config.setdefault('logger', {})
    logger_config.setdefault('log_path', './log')
    logger_config.setdefault('log_level', 'INFO')

    uploader_config: dict = root_config.setdefault('uploader', {})
    uploader_config.setdefault('upload_by_edit', False)
    uploader_config.setdefault('thread_pool_workers', 1)
    uploader_config.setdefault('max_retry', 10)


def initspec(spec_config: dict):
    spec_config.setdefault('room_id', None)
    spec_config.setdefault('backup', False)

    recorder_config: dict = spec_config.setdefault('recorder', {})
    recorder_config.setdefault('keep_raw_record', False)

    parser_config: dict = spec_config.setdefault('parser', {})
    parser_config.setdefault('interval', 30)
    parser_config.setdefault('up_ratio', 2.5)
    parser_config.setdefault('down_ratio', 0.75)
    parser_config.setdefault('topK', 5)

    clipper_config: dict = spec_config.setdefault('clipper', {})
    clipper_config.setdefault('enable_clipper', False)
    clipper_config.setdefault('min_length', 30)
    clipper_config.setdefault('start_offset', -20)
    clipper_config.setdefault('end_offset', 10)

    uploader_config: dict = spec_config.setdefault('uploader', {})
    uploader_config.setdefault('copyright', 2)

    account_config: dict = uploader_config.setdefault('account', {})
    account_config.setdefault('username', '')

    record_record: dict = uploader_config.setdefault('record', {})
    record_record.setdefault('upload_record', True)
    record_record.setdefault('keep_record_after_upload', True)
    record_record.setdefault('split_interval', 3600)
    record_record.setdefault('title', '')
    record_record.setdefault('tid', 27)
    record_record.setdefault('tags', [])
    record_record.setdefault('desc', '')

    clips_record: dict = uploader_config.setdefault('clips', {})
    clips_record.setdefault('upload_clips', False)
    clips_record.setdefault('keep_clips_after_upload', False)
    clips_record.setdefault('title', '')
    clips_record.setdefault('tid', 27)
    clips_record.setdefault('tags', [])
    clips_record.setdefault('desc', '')


def run(all_config: dict, logfile_name: str, runner_dict: dict):
    old_config = all_config
    try:
        if len(sys.argv) > 1:
            all_config_filename = sys.argv[1]
            with open(all_config_filename, "r", encoding="UTF-8") as f:
                all_config = json.load(f)
        else:
            with open("config.json", "r", encoding="UTF-8") as f:
                all_config = json.load(f)
    except Exception as e:
        print("解析配置文件时出现错误，请检查配置文件！已使用最后一次正确的配置")
        print("错误详情："+str(e))
        all_config = old_config
    root_config: dict = all_config.get('root', {})
    initroot(root_config)
    utils.check_and_create_dir(root_config['data_path'])
    utils.check_and_create_dir(root_config['logger']['log_path'])
    logging.basicConfig(level=utils.get_log_level(root_config['logger']['log_level']),
                        format='%(asctime)s %(thread)d %(threadName)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        handlers=[RotatingFileHandler(os.path.join(root_config['logger']['log_path'], logfile_name), maxBytes=100*1024*1024, backupCount=5, mode="a", encoding="utf-8")])
    utils.init_data_dirs(root_config['data_path'])
    for spec_config in all_config.get('spec', []):
        initspec(spec_config)
        config = {
            'root': root_config,
            'spec': spec_config,
            'password_path': sys.argv[2]
        }
        room_id = spec_config['room_id']
        if room_id in runner_dict:
            tr: MainThreadRunner = runner_dict[room_id]
            tr.mr.config = config
        else:
            tr = MainThreadRunner(config)
            tr.setDaemon(True)
            runner_dict[room_id] = tr
            tr.start()
    utils.print_log(runner_dict)
    time.sleep(root_config['print_interval'])


if __name__ == "__main__":
    freeze_support()
    vt = versionThread()
    vt.start()

    if utils.is_windows():
        utils.add_path("./ffmpeg/bin")
    logfile_name = "Main_"+datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')+'.log'
    runner_dict = {}
    all_config = {}
    while True:
        run(all_config, logfile_name, runner_dict)
