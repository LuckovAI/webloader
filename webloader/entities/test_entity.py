# -*- coding: utf-8 -*-
"""
@author Lukashov_ai
@python 3.6
"""
from pika import BasicProperties


def fheader_step1():
    return {}


def fheader_step2():
    return {}


def fheader_step3():
    return {}


async def fimport_step1(entity, db, rebbit, file_log, pkg_path, session):
    send_event(entity, db, rebbit, file_log, pkg_path, session)
    return True


async def fimport_step2(entity, db, rebbit, file_log, pkg_path, session):
    return True


async def fimport_step3(entity, db, rebbit, file_log, pkg_path, session):
    return True


def send_event(entity, db, rebbit, file_log, pkg_path, session):
    channel = rebbit.get_channel()
    channel.basic_qos(prefetch_count=1)
    channel.exchange_declare(exchange=entity,
                             exchange_type='direct')
    # в будущем могут быть несколько типов сообщений для 1 entity, поэтому "direct"
    channel.basic_publish(exchange=entity,
                          routing_key=entity,
                          body=entity + '_ready_for_udate_bet',
                          properties=BasicProperties(
                              delivery_mode=2,  # make message persistent
                          ))
