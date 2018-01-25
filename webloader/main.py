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
import aiohttp
from time import sleep


def __run(lfrun):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.gather(*[fr() for fr in lfrun]))
    loop.close()
        
        
def run():
    try:
        conf = load_config()
    except Exception as exc:
        msg = u'Ошибка обработки файла конфига. %s' % str(exc)
        print(str(exc))
        sys.stderr.write(msg)
        sys.exit(1)
        
    try:
        # Если не сделать переменную fp глобальной,
        # сборщик мусора закрывает файл
        # Можно запускать только один экземпляр
        lock_path = os.path.join(conf['tmp_store'], 'webloader.lock')
        global fp
        fp = open(lock_path, 'w')
        fcntl.flock(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except Exception as exc:
        msg = u'Другой экземпляр webloader уже запущен.'
        print(msg)
        sys.stderr.write(msg)
        sys.exit(0)
        
    # Инициализируем статические члены класса
    [TaskImpl.fdimport.update({a:b}) for a, b in zip(TaskImpl.steps, TaskImpl.flimport)]
    try:
        #разбиваем список сущностей по процессам и потокам(асинхронным)
        #Сначала инициализируем объекты заданий которые передадим в отдельные процессы
        #Инициализация происходит в синхронном режиме в главном процессе
        entity_list = chunkify(conf['entity_list'], int(conf['thread_count_in_process']))
        lfruns = []
        for entity_list_thread in entity_list:
            lfruns_thread = []
            for entity in entity_list_thread:
                # подготавливаем задания;
                app = TaskImpl(entity, conf)
                lfruns_thread.append(app.run)
            lfruns.append(lfruns_thread)
        #Многопроцессный вариант
        with Pool(int(conf['process_count'])) as p:
            # для каждой пачки сущностей(бирж которые опрашивают) отдельный процесс
            # для каждой сущности асинхронный поток
            procs = p.map_async(__run, lfruns, 1)
            # ждать пока все выполнятся, иначе просто завершится главный процесс, а дочерние прервутся
            # нужно чтоб пока выполняются дочерние процессы можно было работать в главном процессе
            procs.wait()
            res = procs.get()
            
        #Однопроцессный вариант(для тестирования)
        #[__run(lf) for lf in lfruns]            
        #sleep(int(conf['interval_sec']))
    except Exception as exc:
        msg = u'Ошибка выполнения. %s.' % str(exc)
        print(msg)
        sys.stderr.write(msg)
        sys.exit(1)

