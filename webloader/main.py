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


def __run(lent_conf):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.gather(*[TaskImpl(*ent_conf).run() for ent_conf in lent_conf]))
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
    TaskImpl.init_cls()
    try:
        #разбиваем список сущностей по процессам и потокам(асинхронным)
        #Сначала инициализируем объекты заданий которые передадим в отдельные процессы
        #Инициализация происходит в синхронном режиме в главном процессе
        entity_list = chunkify(conf['entity_list'], int(conf['thread_count_in_process']))
        lent_confs = []
        for entity_list_thread in entity_list:
            lent_confs_thread = []
            for entity in entity_list_thread:
                # подготавливаем задания;
                lent_confs_thread.append((entity, conf))
            lent_confs.append(lent_confs_thread)
        #Многопроцессный вариант
        with Pool(int(conf['process_count'])) as p:
            # для каждой пачки сущностей(бирж которые опрашивают) отдельный процесс
            # для каждой сущности асинхронный поток
            procs = p.map_async(__run, lent_confs, 1)
            # ждать пока все выполнятся, иначе просто завершится главный процесс, а дочерние прервутся
            # нужно чтоб пока выполняются дочерние процессы можно было работать в главном процессе
            procs.wait()
            res = procs.get()
            
        #Однопроцессный вариант(для тестирования)
        #[__run(lf) for lf in lent_confs]            
        #sleep(int(conf['interval_sec']))
    except Exception as exc:
        msg = u'Ошибка выполнения. %s.' % str(exc)
        print(msg)
        sys.stderr.write(msg)
        sys.exit(1)

