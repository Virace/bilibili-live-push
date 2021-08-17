# -*- coding: utf-8 -*-
# @Author  : Virace
# @Email   : Virace@aliyun.com
# @Site    : x-item.com
# @Software: PyCharm
# @Create  : 2021/2/19 2:13
# @Update  : 2021/8/17 14:19
# @Detail  : B站直播推送，其实和斗鱼推送一样

import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from database import Flag
from push import Message, push_plus, cool_push, wxpusher_push
from common import Request, check_time

logging.getLogger('urllib3.connectionpool').setLevel(logging.INFO)
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

LEANCLOUD_APP_ID = os.environ.get('LEANCLOUD_APP_ID')
LEANCLOUD_APP_KEY = os.environ.get('LEANCLOUD_APP_KEY')
assert LEANCLOUD_APP_ID, 'LEANCLOUD_APP_ID不能为空'
assert LEANCLOUD_APP_KEY, 'LEANCLOUD_APP_KEY不能为空'

flag = Flag(LEANCLOUD_APP_ID, LEANCLOUD_APP_KEY)
# 设置时区
if sys.platform != "win32":
    os.environ['TZ'] = 'Asia/Shanghai'
    time.tzset()


@check_time
def notification_push_concurrent(msg: Message, extra: dict = None):
    """
    多线程推送, 解决通信不及时问题
    :param msg:
    :param extra:
    :return:
    """
    if extra is None:
        extra = {}

    push_plus_token = os.environ.get('PUSH_PLUS_TOKEN')
    cool_push_token = os.environ.get('COOL_PUSH_TOKEN')
    wxpusher_token = os.environ.get('WXPUSHER_TOKEN')
    with ThreadPoolExecutor() as e:
        fs = []
        if push_plus_token:
            fs.append(e.submit(push_plus, push_plus_token, msg, extra.get('push_plus_topic', ''),
                               extra.get('push_plus_template', 'html')))
        else:
            log.info('未提供 PUSH_PLUS_TOKEN.')

        if cool_push_token:
            fs.append(e.submit(cool_push, push_plus_token, msg, extra.get('cool_push_type', 0),
                               extra.get('cool_push_specific', None)))
        else:
            log.info('未提供 COOL_PUSH_TOKEN.')

        if wxpusher_token:
            fs.append(e.submit(wxpusher_push, wxpusher_token, msg, extra.get('wxpusher_type', 1),
                               extra.get('wxpusher_topicids', None), extra.get('wxpusher_url', None)))
        else:
            log.info('未提供 WXPUSHER_TOKEN.')

        for future in as_completed(iter(fs)):
            try:
                future.result()
            except Exception as exc:
                log.warning('generated an exception: %s' % exc)

    if not (push_plus_token or cool_push_token or wxpusher_token):
        raise Exception('未提供任何推送token')


def notification_push(msg: Message, extra: dict = None):
    """
    消息推送
    :param msg: 消息主题
    :param extra: 额外参数:
    {
        # push+ 群组推送ID
        "push_plus_topic": '',
        # push+ 推送模板
        "push_plus_template": '',

        # coolpush 推送类型(私人推送或群组推送)
        "cool_push_type": '',
        # coolpush 指定推送ID, userId/groupId
        "cool_push_specific": '',

        # WxPusher 推送类型(格式)
        "wxpusher_type" : '',
        # WxPusher 推送主题ID[]
        "wxpusher_topicids" : [],
        # WxPusher 消息底部链接
        "wxpusher_url" : '',
    }
    :return:
    """
    if extra is None:
        extra = {}

    push_plus_token = os.environ.get('PUSH_PLUS_TOKEN')
    cool_push_token = os.environ.get('COOL_PUSH_TOKEN')
    wxpusher_token = os.environ.get('WXPUSHER_TOKEN')

    if push_plus_token:
        try:
            push_plus(
                push_plus_token,
                msg,
                topic=extra.get('push_plus_topic', ''),
                template=extra.get('push_plus_template', 'html')
            )
        except Exception as e:
            log.warning(e)

    if cool_push_token:
        try:
            cool_push(
                push_plus_token,
                msg,
                _type=extra.get('cool_push_type', 0),
                extra=extra.get('cool_push_specific', None))
        except Exception as e:
            log.warning(e)

    if wxpusher_token:
        try:
            wxpusher_push(
                wxpusher_token,
                msg,
                _type=extra.get('wxpusher_type', 1),
                topic_ids=extra.get('wxpusher_topicids', None),
                url=extra.get('wxpusher_url', None)
            )
        except Exception as e:
            log.warning(e)

    if not (push_plus_token or cool_push_token or wxpusher_token):
        raise Exception('未提供任何推送token')


@check_time
def get_status(rid: str) -> tuple:
    """
    通过B站 网页端直播接口 get_info_by_id 获取直播信息, 支持短ID
    :param rid: 直播间ID
    :return: 返回元组格式(状态, 源)
    """
    url = f'https://api.live.bilibili.com/room/v1/Room/get_info_by_id?ids[]={rid}'
    request = Request()
    response = request.get(url, timeout=5)
    response.raise_for_status()
    data = response.json()['data']
    log.debug(data)
    for _id, item in data.items():
        if _id == rid or item['short_id'] == rid:
            return item['live_status'] == 1, item
    else:
        return None, None


def monitor_and_notify(rid: str, extra: dict = None):
    """
    监测并推送
    :param rid: 直播间ID
    :param extra: 额外参数:
    {
        # push+ 群组推送ID
        "push_plus_topic": '',
        # push+ 推送模板
        "push_plus_template": '',

        # coolpush 推送类型(私人推送或群组推送)
        "cool_push_type": '',
        # coolpush 指定推送ID, userId/groupId
        "cool_push_specific": ''
        ....
        # 详见notification_push函数文档

        # leancloud结构化数据中, 数据行objectId, 一个直播间对应一个数据行进行判断
        "leancloud_oid": '',
    }
    :return:
    """
    status, data = get_status(rid)
    log.info(f'{rid} 直播状态: {status}')

    oid = extra.get('leancloud_oid', None)
    assert oid, '缺少关键性参数, leancloud_oid'

    other_msg = extra.get('OTHER_MSG', None)

    if status:
        # 下面一句代码也许会出现问题
        new = time.mktime(time.strptime(data["live_time"], '%Y-%m-%d %H:%M:%S'))
        try:
            old = flag.get_time(oid)
            if old == new:
                return

            else:
                flag.update_time(oid, new)
        except Exception as e:
            log.warning(e)
            return
        else:

            content = f'开播时间: {new}<br>' \
                      f'<img src={data["cover"]}>'
            if other_msg:
                content = f'{content}<br>{other_msg}'
            notification_push_concurrent(
                Message(title=f'您关注的主播 {data["uname"]}:{data["roomid"]} 正在直播!',
                        content=content),
                extra
            )


def main_handler(event, context):
    """
    云函数调用函数
    :param event:
    :param context:
    :return:
    """
    if event and 'Message' in event:
        try:
            data = json.loads(event['Message'].strip())
        except Exception as e:
            raise Exception('触发器格式不正确', e)
        else:
            for item in data.items():
                monitor_and_notify(item[0], item[1])
    else:
        raise Exception('请配置触发器参数')
