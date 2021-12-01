import ctypes
import datetime
import logging
import os
import platform
import threading
from collections import Counter
from enum import Enum

import jieba
import prettytable as pt


def is_windows() -> bool:
    plat_sys = platform.system()
    return plat_sys == "Windows"


if is_windows():
    import winreg


def get_log_level(log_level: str) -> int:
    if log_level == 'DEBUG':
        return logging.DEBUG
    if log_level == 'INFO':
        return logging.INFO
    if log_level == 'WARN':
        return logging.WARN
    if log_level == 'ERROR':
        return logging.ERROR
    return logging.INFO


def get_logger(config: dict, logname: str):
    logger = logging.getLogger(logname)
    logger.setLevel(get_log_level(config['root']['logger']['log_level']))
    handler = logging.FileHandler(os.path.join(config['root']['logger']['log_path'],
                                               logname+"_"+datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')+'.log'),
                                  mode="a", encoding="utf-8")
    handler.setFormatter(logging.Formatter(
        '%(asctime)s %(thread)d %(threadName)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
        '%a, %d %b %Y %H:%M:%S'))
    logger.addHandler(handler)
    return logger


def check_and_create_dir(dirs: str) -> None:
    if not os.path.exists(dirs):
        os.mkdir(dirs)


def init_data_dirs(root_dir: str = os.getcwd()) -> None:
    check_and_create_dir(os.path.join(root_dir, 'data'))
    check_and_create_dir(os.path.join(root_dir, 'data', 'records'))
    check_and_create_dir(os.path.join(root_dir, 'data', 'merged'))
    check_and_create_dir(os.path.join(root_dir, 'data', 'merge_confs'))
    check_and_create_dir(os.path.join(root_dir, 'data', 'danmu'))
    check_and_create_dir(os.path.join(root_dir, 'data', 'outputs'))
    check_and_create_dir(os.path.join(root_dir, 'data', 'splits'))


def init_record_dir(room_id: str, global_start: datetime.datetime, root_dir: str = os.getcwd()) -> str:
    dirs = os.path.join(root_dir, 'data', 'records',
                        f"{room_id}_{global_start.strftime('%Y-%m-%d_%H-%M-%S')}")
    check_and_create_dir(dirs)
    return dirs


def init_danmu_log_dir(room_id: str, global_start: datetime.datetime, root_dir: str = os.getcwd()) -> str:
    log_dir = os.path.join(
        root_dir, 'data', 'danmu', f"{room_id}_{global_start.strftime('%Y-%m-%d_%H-%M-%S')}")
    check_and_create_dir(log_dir)
    return log_dir


def generate_filename(room_id: str) -> str:
    return f"{room_id}_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.flv"


def get_global_start_from_records(record_dir: str) -> datetime.datetime:
    base = os.path.basename(record_dir)
    return datetime.datetime.strptime(" ".join(base.split("_")[1:3]), '%Y-%m-%d %H-%M-%S')


def get_merged_filename(room_id: str, global_start: datetime.datetime, root_dir: str = os.getcwd()) -> str:
    filename = os.path.join(root_dir, 'data', 'merged',
                            f"{room_id}_{global_start.strftime('%Y-%m-%d_%H-%M-%S')}_merged.mp4")
    return filename


def init_outputs_dir(room_id: str, global_start: datetime.datetime, root_dir: str = os.getcwd()) -> str:
    dirs = os.path.join(root_dir, 'data', 'outputs',
                        f"{room_id}_{global_start.strftime('%Y-%m-%d_%H-%M-%S')}")
    check_and_create_dir(dirs)
    return dirs


def init_splits_dir(room_id: str, global_start: datetime.datetime, root_dir: str = os.getcwd()) -> str:
    dirs = os.path.join(root_dir, 'data', 'splits',
                        f"{room_id}_{global_start.strftime('%Y-%m-%d_%H-%M-%S')}")
    check_and_create_dir(dirs)
    return dirs


def get_merge_conf_path(room_id: str, global_start: datetime.datetime, root_dir: str = os.getcwd()) -> str:
    filename = os.path.join(root_dir, 'data', 'merge_confs',
                            f"{room_id}_{global_start.strftime('%Y-%m-%d_%H-%M-%S')}_merge_conf.txt")
    return filename


def del_files_and_dir(dirs: str) -> None:
    for filename in os.listdir(dirs):
        os.remove(os.path.join(dirs, filename))
    os.rmdir(dirs)


def refresh_reg() -> None:
    HWND_BROADCAST = 0xFFFF
    WM_SETTINGCHANGE = 0x1A

    SMTO_ABORTIFHUNG = 0x0002

    result = ctypes.c_long()
    SendMessageTimeoutW = ctypes.windll.user32.SendMessageTimeoutW
    SendMessageTimeoutW(HWND_BROADCAST, WM_SETTINGCHANGE, 0,
                        u'Environment', SMTO_ABORTIFHUNG, 5000, ctypes.byref(result))


def add_path(path: str) -> None:
    abs_path = os.path.abspath(path)
    path_key = winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                              'Environment', 0, winreg.KEY_ALL_ACCESS)
    path_value = winreg.QueryValueEx(path_key, 'Path')
    if path_value[0].find(abs_path) == -1:
        winreg.SetValueEx(path_key, "Path", 0,
                          winreg.REG_EXPAND_SZ, path_value[0]+(";" if path_value[0][-1] != ";" else "")+abs_path+";")
        refresh_reg()


class state(Enum):
    ERROR = -1
    WAITING_FOR_LIVE_START = 0
    LIVE_STARTED = 1
    PROCESSING_RECORDS = 2
    UPLOADING_TO_BILIBILI = 3
    UPLOADING_TO_BAIDUYUN = 4

    def __str__(self):
        if self.value == self.ERROR.value:
            return "错误！"
        if self.value == self.WAITING_FOR_LIVE_START.value:
            return "摸鱼中"
        if self.value == self.LIVE_STARTED.value:
            return "正在录制"
        if self.value == self.PROCESSING_RECORDS.value:
            return "正在处理视频"
        if self.value == self.UPLOADING_TO_BILIBILI.value:
            return "正在上传至Bilibili"
        if self.value == self.UPLOADING_TO_BAIDUYUN.value:
            return "正在上传至百度网盘"

    def __int__(self):
        return self.value


def print_log(runner_list: list) -> str:
    tb = pt.PrettyTable()
    tb.field_names = ["TID", "平台", "房间号", "直播状态", "程序状态", "状态变化时间"]
    for runner in runner_list.values():
        tb.add_row([runner.name, runner.mr.bl.site_name, runner.mr.bl.room_id, "是" if runner.mr.bl.live_status else "否",
                    str(state(runner.mr.current_state.value)), datetime.datetime.fromtimestamp(runner.mr.state_change_time.value)])
    logging.info(f"正在工作线程数：{threading.activeCount()}\n{tb}\n")
    # logging.info(tb)
    # logging.info("\n")


def get_words(txt, topK=5):
    seg_list = jieba.cut(txt)  # 对文本进行分词
    c = Counter()
    for x in seg_list:  # 进行词频统计
        if len(x) > 1 and x != '\r\n':
            c[x] += 1
    try:
        return list(list(zip(*c.most_common(topK)))[0])
    except IndexError:
        return []
