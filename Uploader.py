import datetime
import json
import logging
import os
import traceback

from bilibiliuploader.bilibiliuploader import BilibiliUploader
from bilibiliuploader.core import VideoPart

import utils
from BiliLive import BiliLive


def upload(uploader: BilibiliUploader, parts: list, cr: int, title: str, tid: int, tags: list, desc: str, source: str, thread_pool_workers: int = 1, max_retry: int = 3, upload_by_edit: bool = False) -> tuple:
    bvid = None
    if upload_by_edit:
        while bvid is None:
            avid, bvid = uploader.upload(
                parts=[parts[0]],
                copyright=cr,
                title=title,
                tid=tid,
                tag=",".join(tags),
                desc=desc,
                source=source,
                thread_pool_workers=thread_pool_workers,
                max_retry=max_retry,
            )
        for i in range(1, len(parts)):
            uploader.edit(
                bvid=bvid,
                parts=[parts[i]],
                max_retry=max_retry,
                thread_pool_workers=thread_pool_workers
            )
    else:
        while bvid is None:
            avid, bvid = uploader.upload(
                parts=parts,
                copyright=cr,
                title=title,
                tid=tid,
                tag=",".join(tags),
                desc=desc,
                source=source,
                thread_pool_workers=thread_pool_workers,
                max_retry=max_retry,
            )
            print(avid, bvid)
    return avid, bvid


class Uploader(BiliLive):
    def __init__(self, output_dir: str, splits_dir: str, config: dict, roomname: str):
        super().__init__(config)
        self.config = config
        self.roomname = roomname
        self.output_dir = output_dir
        self.splits_dir = splits_dir
        self.uploader = BilibiliUploader()
        logging.basicConfig(level=utils.get_log_level(config['root']['logger']['log_level']),
                            format='%(asctime)s %(thread)d %(threadName)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=os.path.join(config['root']['logger']['log_path'], "Uploader_"+datetime.datetime.now(
                            ).strftime('%Y-%m-%d_%H-%M-%S')+'.log'),
                            filemode='a')
        try:
            with open(config['password_path'], "r", encoding="UTF-8") as f:
                pw_config = json.load(f)
            username = config['spec']['uploader']['account']['username']
            passwd = pw_config.get(username, None)
            self.uploader.login(username, passwd)
        except Exception as e:
            logging.error("解析密码文件时出现错误，请用户名密码是否正确")
            logging.error("错误详情："+str(e))

    def upload(self, global_start: datetime.datetime) -> dict:

        return_dict = {}
        try:
            if self.config['spec']['uploader']['clips']['upload_clips']:
                output_parts = []
                datestr = global_start.strftime(
                    '%Y{y}%m{m}%d{d}').format(y='年', m='月', d='日')
                filelists = os.listdir(self.output_dir)
                for filename in filelists:
                    if os.path.getsize(os.path.join(self.output_dir, filename)) < 1024*1024:
                        continue
                    title = os.path.splitext(filename)[0].split("_")[-1]
                    output_parts.append(VideoPart(
                        path=os.path.join(self.output_dir, filename),
                        title=title,
                        desc=self.config['spec']['uploader']['clips']['desc'].format(
                            date=datestr, title=self.roomname),
                    ))

                avid, bvid = upload(self.uploader, output_parts,
                                    cr=self.config['spec']['uploader']['copyright'],
                                    title=self.config['spec']['uploader']['clips']['title'].format(
                                        date=datestr, title=self.roomname),
                                    tid=self.config['spec']['uploader']['clips']['tid'],
                                    tags=self.config['spec']['uploader']['clips']['tags'],
                                    desc=self.config['spec']['uploader']['clips']['desc'].format(
                                        date=datestr, title=self.roomname),
                                    source="https://live.bilibili.com/"+self.room_id,
                                    thread_pool_workers=self.config['root']['uploader']['thread_pool_workers'],
                                    max_retry=self.config['root']['uploader']['max_retry'],
                                    upload_by_edit=self.config['root']['uploader']['upload_by_edit'])
                return_dict["clips"] = {
                    "avid": avid,
                    "bvid": bvid
                }
            if self.config['spec']['uploader']['record']['upload_record']:
                splits_parts = []
                datestr = global_start.strftime(
                    '%Y{y}%m{m}%d{d}').format(y='年', m='月', d='日')
                filelists = os.listdir(self.splits_dir)
                for filename in filelists:
                    if os.path.getsize(os.path.join(self.splits_dir, filename)) < 1024*1024:
                        continue
                    title = filename
                    splits_parts.append(VideoPart(
                        path=os.path.join(self.splits_dir, filename),
                        title=title,
                        desc=self.config['spec']['uploader']['record']['desc'].format(
                            date=datestr, title=self.roomname),
                    ))

                avid, bvid = upload(self.uploader, splits_parts,
                                    cr=self.config['spec']['uploader']['copyright'],
                                    title=self.config['spec']['uploader']['record']['title'].format(
                                        date=datestr, title=self.roomname),
                                    tid=self.config['spec']['uploader']['record']['tid'],
                                    tags=self.config['spec']['uploader']['record']['tags'],
                                    desc=self.config['spec']['uploader']['record']['desc'].format(
                                        date=datestr, title=self.roomname),
                                    source="https://live.bilibili.com/"+self.room_id,
                                    thread_pool_workers=self.config['root']['uploader']['thread_pool_workers'],
                                    max_retry=self.config['root']['uploader']['max_retry'],
                                    upload_by_edit=self.config['root']['uploader']['upload_by_edit'])
                return_dict["record"] = {
                    "avid": avid,
                    "bvid": bvid
                }
        except Exception as e:
            logging.error(self.generate_log(
                'Error while uploading:' + str(e)+traceback.format_exc()))
        return return_dict


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
    global_start = datetime.datetime(2021, 11, 18, 23, 37, 43)
    u = Uploader("data/outputs/5561470_2021-11-18_23-37-43", "data/splits/5561470_2021-11-18_23-37-43",
                         config, '【歌杂】唱歌啦~')
    d = u.upload(global_start)
    print("success")
