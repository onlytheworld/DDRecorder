import json
import os
import datetime
import logging
import threading
import time
import traceback
from multiprocessing import Process, Value

import utils
from BiliLive import BiliLive
from BiliLiveRecorder import BiliLiveRecorder
from BiliVideoChecker import BiliVideoChecker
from DanmuRecorder import BiliDanmuRecorder
from Processor import Processor
from Uploader import Uploader


class MainRunner():
    def __init__(self, config: dict):
        self.config = config
        self.prev_live_status = False
        self.current_state = Value(
            'i', int(utils.state.WAITING_FOR_LIVE_START))
        self.state_change_time = Value('f', time.time())
        if config['root']['enable_baiduyun']:
            from bypy import ByPy
            _ = ByPy()
        self.bl = BiliLive(config)
        self.blr = None
        self.bdr = None
        self.logger = utils.get_logger(config, "MainRunner")

        # logging.basicConfig(level=utils.get_log_level(self.config['root']['logger']['log_level']),
        #                     format='%(asctime)s %(thread)d %(threadName)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
        #                     datefmt='%a, %d %b %Y %H:%M:%S',
        #                     handlers=[logging.FileHandler(os.path.join(self.config['root']['logger']['log_path'], "MainRunner_"+datetime.datetime.now(
        #                     ).strftime('%Y-%m-%d_%H-%M-%S')+'.log'), "a", encoding="utf-8")])

    def proc(self) -> None:
        p = Processor(self.config, self.blr.record_dir, self.bdr.danmu_dir)
        p.run()

        uploader_config = self.config['spec']['uploader']
        if uploader_config['record']['upload_record'] or uploader_config['clips']['upload_clips']:
            self.current_state.value = int(utils.state.UPLOADING_TO_BILIBILI)
            self.state_change_time.value = time.time()
            u = Uploader(p.outputs_dir, p.splits_dir,
                         self.config, self.roomname)
            d = u.upload(p.global_start)
            if not uploader_config['record']['keep_record_after_upload'] and d.get("record", None) is not None and not self.config['root']['uploader']['upload_by_edit']:
                rc = BiliVideoChecker(d['record']['bvid'],
                                      p.splits_dir, self.config)
                rc.start()
            if not uploader_config['clips']['keep_clips_after_upload'] and d.get("clips", None) is not None and not self.config['root']['uploader']['upload_by_edit']:
                cc = BiliVideoChecker(d['clips']['bvid'],
                                      p.outputs_dir, self.config)
                cc.start()

        try:
            if self.config['root']['enable_baiduyun'] and self.config['spec']['backup']:
                self.current_state.value = int(
                    utils.state.UPLOADING_TO_BAIDUYUN)
                self.state_change_time.value = time.time()
                from bypy import ByPy
                bp = ByPy()
                bp.upload(p.merged_file_path, remotepath="/L_archives/")
                bp.upload(p.danmu_path, remotepath="/L_archives/")
        except Exception as e:
            self.logger.error('Error when uploading to Baiduyun:' +
                              str(e)+traceback.format_exc())

        if self.current_state.value != int(utils.state.LIVE_STARTED):
            self.current_state.value = int(utils.state.WAITING_FOR_LIVE_START)
            self.state_change_time.value = time.time()

    def run(self):
        proc_process = None
        try:
            while True:
                if not self.prev_live_status and self.bl.live_status:
                    start = datetime.datetime.now()
                    self.blr = BiliLiveRecorder(self.config, start)
                    self.bdr = BiliDanmuRecorder(self.config, start)
                    record_process = Process(target=self.blr.run)
                    danmu_process = Process(target=self.bdr.run)
                    danmu_process.start()
                    record_process.start()

                    self.current_state.value = int(utils.state.LIVE_STARTED)
                    self.state_change_time.value = time.time()
                    self.prev_live_status = True
                    self.roomname = self.bl.get_room_info()['roomname']

                    record_process.join()
                    danmu_process.join()

                    self.current_state.value = int(
                        utils.state.PROCESSING_RECORDS)
                    self.state_change_time.value = time.time()

                    self.prev_live_status = False
                    proc_process = Process(target=self.proc)
                    proc_process.start()
                else:
                    time.sleep(self.config['root']['check_interval'])
        except KeyboardInterrupt:
            return
        except Exception as e:
            self.logger.error('Error in Mainrunner:' +
                              str(e)+traceback.format_exc())


class MainThreadRunner(threading.Thread):
    def __init__(self, config: dict):
        threading.Thread.__init__(self)
        self.mr = MainRunner(config)

    def run(self):
        self.mr.run()


if __name__ == "__main__":
    if utils.is_windows():
        utils.add_path("./ffmpeg/bin")
    with open("config/config.json", "r", encoding="UTF-8") as f:
        all_config = json.load(f)
    root_config: dict = all_config.get('root', {})
    spec_config = all_config.get('spec', [])[0]
    config = {
        'root': root_config,
        'spec': spec_config,
        'password_path': "config/passwd.json"
    }
    mr = MainRunner(config)
    start = datetime.datetime(2021, 11, 28, 22, 31, 32)
    mr.blr = BiliLiveRecorder(mr.config, start)
    mr.bdr = BiliDanmuRecorder(mr.config, start)
    mr.roomname = '游戏时间！'
    mr.proc()
    print("end")
