# -*- coding: utf-8 -*-
"""
@author Lukashov_ai
@python 3.6
"""
import os
import sys
import fcntl
from webloader.impl import TaskImpl
from webloader.utils import load_config, chunkify
import asyncio
from multiprocessing import Pool
from time import sleep

CONF = load_config()


def __run(lfrun):
    while (True):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(asyncio.gather(*[fr() for fr in lfrun]))
        loop.close()
        sleep(int(CONF['interval_sec']))


def run():
    try:
        # Можно запускать только один экземпляр
        lock_path = os.path.join(CONF['tmp_store'], 'webloader.lock')
    except Exception as exc:
        msg = u'Ошибка обработки файла конфига. %s' % str(exc)
        print(str(exc))
        sys.stderr.write(msg)
        sys.exit(1)

    try:
        # Если не сделать переменную fp глобальной,
        # сборщик мусора закрывает файл
        global fp
        fp = open(lock_path, 'w')
        fcntl.flock(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except Exception as exc:
        msg = u'Другой экземпляр webloader уже запущен.'
        print(msg)
        sys.stderr.write(msg)
        sys.exit(0)

    # Инициализируем статические члены класса
    [TaskImpl.fdimport.update({a: b}) for a, b in zip(TaskImpl.steps, TaskImpl.flimport)]
    try:
        # разбиваем список сущностей по процессам и потокам(асинхронным)
        # Сначала инициализируем объекты заданий которые передадим в отдельные процессы
        # Инициализация происходит в синхронном режиме в главном процессе
        entity_list = chunkify(CONF['entity_list'], int(CONF['thread_count_in_process']))
        lfruns = []
        for entity_list_thread in entity_list:
            lfruns_thread = []
            for entity in entity_list_thread:
                # подготавливаем задания; copy - чтоб избежать синхронизации общих переменных между процессами
                app = TaskImpl(entity, CONF.copy())
                lfruns_thread.append(app.run)
            lfruns.append(lfruns_thread)
        # Многопроцессный вариант
        with Pool(int(CONF['process_count'])) as p:
            # для каждой пачки сущностей(бирж которые опрашивают) отдельный процесс
            # для каждой сущности асинхронный поток
            procs = p.map_async(__run, lfruns, 1)
            # ждать пока все выполнятся, иначе просто завершится главный процесс, а дочерние прервутся
            # нужно чтоб пока выполняются дочерние процессы можно было работать в главном процессе
            procs.wait()
            res = procs.get()

            # Однопроцессный вариант(для тестирования)
            # while(True):
            # [__run(lf) for lf in lfruns]
            # sleep(int(conf['interval_sec']))
    except Exception as exc:
        msg = u'Ошибка выполнения. %s.' % str(exc)
        print(msg)
        sys.stderr.write(msg)
        sys.exit(1)
