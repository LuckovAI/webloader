# -*- coding: utf-8 -*-
"""
@author Lukashov_ai
@python 3.6
"""

import os
import sys
import logging
import logging.handlers
from webloader.utils import Database, Service, load_config_entity, gen_guid, Rebbit
from importlib import import_module
import aiofiles
import asyncio
from datetime import datetime
import json
import zipfile
import shutil


class TaskImpl(object):
    fdimport = {}
    steps = ['step1', 'step2', 'step3']
    flheader = ['fheader_step1', 'fheader_step2', 'fheader_step3']
    flimport = ['fimport_step1', 'fimport_step2', 'fimport_step3']
    
    @classmethod
    def init_cls(cls):
        [cls.fdimport.update({a:b}) for a, b in zip(cls.steps, cls.flimport)]
    
    def __init__(self, entity, conf):
        self._conf = conf
        self._entity = entity
        self._srv = {}
        self._spec_module_entity = import_module('webloader.entities.' + entity)
        self._init_log(entity)
        self._conf_entity = load_config_entity(entity, self._file_log)
        self._create_dirs(entity)
        self._init_db()
        self._init_srv(entity)
        self._init_rebbit()
        # сессия одного главного цикла(для передачи специфичных данных от шага к шагу)
        self._session = {}
        
    def _init_rebbit(self):
        self._rebbit = Rebbit(logger=self._file_log,
                           host=self._conf['rebbitmq_host'],
                           port=self._conf['rebbitmq_port'],
                           virtual_host=self._conf['rebbitmq_virtual_host'],
                           user=self._conf['rebbitmq_user'],
                           password=self._conf['rebbitmq_password'],
                           ssl=self._conf['rebbitmq_ssl'])
        
    async def fstep(self, step):
        """
        Загружаем и импортируем, каждый активный для сущности шаг
        """
        if int(self._conf_entity[step]['active_webload_step']): await self.load_data(step)
        if int(self._conf_entity[step]['active_import_step']): await self.import_data(step)
            
    async def run(self):
        """
        Точка входа. Обычно для обработки любой сущности хватает 2,3-х шагов
        """
        while(True):
            [await self.fstep(i) for i in TaskImpl.steps]
            await asyncio.sleep(int(self._conf['interval_sec']))

    async def import_data(self, step):
        """
        Составляет список файлов для импорта и запускает
        метод import_file для каждого из них
        """
        start = datetime.now()
        self._file_log.info(u'import_data %s' % step)
        # Формируем список импорта
        implist = list()
        inbox = os.path.join(self._conf['tmp_store'], self._entity, step, 'inbox')
        for pkg in os.listdir(inbox):
            pkg_path = os.path.join(inbox, pkg)
            self._file_log.info(u'pkg_path %s: %s' % (step, pkg_path))
            implist.append(pkg_path)
            
        self._file_log.info(u'implist %s: %s' % (step, implist))
        # Импортируем пакеты
        [await self.import_file(f, step) for f in implist]
        self._file_log.info(u'Все пакеты %s импортированы.' % step)
        diff = (datetime.now() - start).total_seconds()
        self._file_log.info(u'Общеее время импорта %s данных %s' % (step, diff))

    async def import_file(self, pkg_path, step):
        self._file_log.info(u'import_file(%s)' % pkg_path)
        garbage = os.path.join(self._conf['tmp_store'], self._entity, step, 'garbage')
        res = await getattr(self._spec_module_entity, TaskImpl.fdimport[step])(self._entity, self._db, self._rebbit, \
                                                                               self._file_log, pkg_path, self._session)
        self._file_log.info(u'Результат импорта файла: %s' % res)
        # Манипулируем с файликами для хранения истории и очистки
        basename = os.path.basename(pkg_path)
        res_path = os.path.join(garbage, basename)
        self._file_log.info(u'Перемещаем файл %s' % pkg_path)
        self._file_log.info(u'Результирующая директория: %s' % res_path)
        if os.path.exists(res_path):
            self._file_log.info(u'Такой файл есть. Удаляем %s' % res_path)
            os.remove(res_path)
        shutil.move(pkg_path, res_path)
        self._file_log.info(u'Файл(сущность) %s обработана' % pkg_path)
        return
        
    async def load_data(self, step):
        """
        Загружает архив с сервера и сохраняет во временную директорию
        Распаковывает содержимое
        """
        start = datetime.now()
        self._file_log.info(u'OpImpl::load_data. Загружаем данные.')

        for i in range(1, int(self._conf_entity[step]['retries']) + 1):
            try:
                resp = None
                resp = await (self._srv[step]).send_get(self._session.get('params'))
                if resp: break
            except Exception as exc:
                msg = u'Попытка N%s получения не удалась. %s' % (i, exc)
                self._file_log.info(msg)
                await asyncio.sleep(int(self._conf_entity[step]['sleep']))
        
        if not resp:
            self._file_log.info(u'Get запроса %s ничего не вернул' % step)
            return
        
        if not resp.get('result'):
            self._file_log.info(u'Get запроса %s ничего не вернул' % step)
            return

        fname = gen_guid()
        path = os.path.join(self._conf['tmp_store'], self._entity, step, 'tmp', fname)
        async with aiofiles.open(path, 'wb') as hndl:
            await hndl.write(json.dumps(resp['result']).encode())
        self._file_log.info(u'Ответ Get запроса %s загружен. %s' % (step, path))
        
        inbox = os.path.join(self._conf['tmp_store'], self._entity, step, 'inbox')
        try:
            # Распаковываем
            zipfile.ZipFile(path).extractall(inbox)
        except zipfile.BadZipfile as exc:
            # Файл не архив, значит просто перемещаем готовый
            shutil.copy(path, inbox)
        # Отправляем в хлам
        garbage = os.path.join(self._conf['tmp_store'], self._entity, step, 'garbage')
        shutil.move(path, garbage)
        self._file_log.info(u'Обработан ответ %s. %s' % (step, path))
        diff = (datetime.now() - start).total_seconds()
        self._file_log.info(u'Общеее время получения данных %s %s' % (step, diff))
        return
        
    def _init_srv(self, entity):
        async def get_cookie():
            await self._db.start_transaction()
            res = await self._db.fetch("select cookie from bs where code = $1", (self._entity,))
            await self._db.commit()
            dres = dict(res[0])
            cookie = {'Cookie':dres['cookie']} if dres['cookie'] else {}
            return cookie
        async def set_cookie(cookie):
            if not cookie: return
            await self._db.start_transaction()
            await self._db.execute("update bs set cookie = $1 where code = $2", (cookie, self._entity))
            await self._db.commit()
            
        #имя модуля с специфичной реализацией для сущности совпадает с наименованием сущности
        srv1 = Service(logger=self._file_log,
                       get_cookie=get_cookie,
                       set_cookie=set_cookie,
                       host=self._conf_entity['step1']['host'],
                       port=self._conf_entity['step1']['port'],
                       headers=getattr(self._spec_module_entity, TaskImpl.flheader[0])(),
                       params=self._conf_entity['step1']['params'],
                       ssl=self._conf_entity['step1']['ssl'])
        srv2 = Service(logger=self._file_log,
                       get_cookie=get_cookie,
                       set_cookie=set_cookie,
                       host=self._conf_entity['step2']['host'],
                       port=self._conf_entity['step2']['port'],
                       headers=getattr(self._spec_module_entity, TaskImpl.flheader[1])(),
                       params=self._conf_entity['step2']['params'],
                       ssl=self._conf_entity['step2']['ssl'])
        srv3 = Service(logger=self._file_log,
                       get_cookie=get_cookie,
                       set_cookie=set_cookie,
                       host=self._conf_entity['step3']['host'],
                       port=self._conf_entity['step3']['port'],
                       headers=getattr(self._spec_module_entity, TaskImpl.flheader[2])(),
                       params=self._conf_entity['step3']['params'],
                       ssl=self._conf_entity['step3']['ssl'])
        self._srv.update({'step1':srv1, 'step2':srv2, 'step3':srv3})
        
    def _init_db(self):
        self._db = Database(logger=self._file_log,
                           host=self._conf['host'],
                           port=self._conf['port'],
                           dbname=self._conf['dbname'],
                           user=self._conf['user'],
                           password=self._conf['password']
                           )

    def _create_dirs(self, entity):
        """
        Создает временные диертории при их отсутствии
        """
        self._file_log.info(u'OpImpl::_create_dirs')
        dirs = ['tmp', 'inbox', 'receips', 'garbage']
        path = os.path.join(self._conf['tmp_store'], entity)
        step1 = os.path.join(self._conf['tmp_store'], entity, 'step1')
        step2 = os.path.join(self._conf['tmp_store'], entity, 'step2')
        step3 = os.path.join(self._conf['tmp_store'], entity, 'step3')

        if not os.path.isdir(path):
            os.mkdir(path)
        if not os.path.isdir(step1):
            os.mkdir(step1)
        if not os.path.isdir(step2):
            os.mkdir(step2)
        if not os.path.isdir(step3):
            os.mkdir(step3)

        for name in dirs:
            step1 = os.path.join(self._conf['tmp_store'], entity, 'step1', name)
            if not os.path.isdir(step1):
                os.mkdir(step1)
        for name in dirs:
            step2 = os.path.join(self._conf['tmp_store'], entity, 'step2', name)
            if not os.path.isdir(step2):
                os.mkdir(step2)
        for name in dirs:
            step3 = os.path.join(self._conf['tmp_store'], entity, 'step3', name)
            if not os.path.isdir(step3):
                os.mkdir(step3)
    
    def _init_log(self, name):
        """
        Инициализация лог файла
        Пока нормальных асинхронных библиотек не найдено, оставляем синхронный вариант
        """
        self._path_log = os.path.join(self._conf['log_file'], name + '.log')
        self._file_log = logging.getLogger(name)
        logging_level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        self._file_log.setLevel(logging_level_map[self._conf['log_level']])
        log_form = logging.Formatter(self._conf['format'], self._conf['dateformat'])
        rotation_handler = logging.handlers.RotatingFileHandler( \
            self._path_log, 'ab', maxBytes=self._conf['maxBytes'], \
            backupCount=self._conf['backupCount'], encoding='utf8')
        rotation_handler.setFormatter(log_form)
        self._file_log.addHandler(rotation_handler)
        self._file_log.info('TaskImpl::_init_log')
        return True
