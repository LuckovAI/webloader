# -*- coding: utf-8 -*-
"""
@author Lukashov_ai
@python 3.6
"""

import os
import uuid
import asyncpg
import configparser
import aiohttp
from datetime import datetime
import pika


def chunkify(lst, n):
    """
    Делит на равные более менее части список
    """
    return [lst[i::n] for i in range(n)]
    # entity_list = map(lambda lst,n: [lst[i::n] for i in range(n)], el, cip)[0]


def load_config_entity(entity, file_log):
    """
    Загружает конфигурационный файл сущности
    """
    conf = dict()
    conf_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'entities', entity + '.conf')

    if not os.path.exists(conf_file):
        conf_file = os.path.join('/etc/webloader', entity + '.conf')
    if not os.path.exists(conf_file):
        msg = u'Ошибка чтения файла конфигурации. Файл %s не найден.' % conf_file
        print(msg)
        file_log.error(msg)
        sys.exit(1)

    cfg_adapter = configparser.RawConfigParser()
    try:
        cfg_adapter.read(conf_file)
    except configparser.Error as ce:
        msg = u'Ошибка чтения %s файла конфигурации сущностей. %s.' % (conf_file, str(ce))
        print(msg)
        file_log.error(msg)
        sys.exit(1)

    if not cfg_adapter.has_section(entity):
        msg = u'Ошибка чтения секций файла конфигурации. %s.' % conf_file
        print(msg)
        file_log.error(msg)
        sys.exit(1)

    try:
        # Настройки соединения 
        step = {}
        step['host'] = cfg_adapter.get(entity, 'host_step1')
        step['port'] = cfg_adapter.get(entity, 'port_step1')
        step['params'] = cfg_adapter.get(entity, 'params_step1')
        step['ssl'] = cfg_adapter.get(entity, 'ssl_step1')
        step['sleep'] = cfg_adapter.get(entity, 'sleep_step1')
        step['retries'] = cfg_adapter.get(entity, 'retries_step1')
        step['active_webload_step'] = cfg_adapter.get(entity, 'active_webload_step1')
        step['active_import_step'] = cfg_adapter.get(entity, 'active_import_step1')
        conf['step1'] = step
        step = {}
        step['host'] = cfg_adapter.get(entity, 'host_step2')
        step['port'] = cfg_adapter.get(entity, 'port_step2')
        step['params'] = cfg_adapter.get(entity, 'params_step2')
        step['ssl'] = cfg_adapter.get(entity, 'ssl_step2')
        step['sleep'] = cfg_adapter.get(entity, 'sleep_step2')
        step['retries'] = cfg_adapter.get(entity, 'retries_step2')
        step['active_webload_step'] = cfg_adapter.get(entity, 'active_webload_step2')
        step['active_import_step'] = cfg_adapter.get(entity, 'active_import_step2')
        conf['step2'] = step
        step = {}
        step['host'] = cfg_adapter.get(entity, 'host_step3')
        step['port'] = cfg_adapter.get(entity, 'port_step3')
        step['params'] = cfg_adapter.get(entity, 'params_step3')
        step['ssl'] = cfg_adapter.get(entity, 'ssl_step3')
        step['sleep'] = cfg_adapter.get(entity, 'sleep_step3')
        step['retries'] = cfg_adapter.get(entity, 'retries_step3')
        step['active_webload_step'] = cfg_adapter.get(entity, 'active_webload_step3')
        step['active_import_step'] = cfg_adapter.get(entity, 'active_import_step3')
        conf['step3'] = step
    except (ConfigParser.Error, TypeError) as cpex:
        msg = u'Некорректные настройки %s. %s' % (conf_file, cpex)
        print(msg)
        file_log.error(msg)
        sys.exit(1)
    return conf


def load_config():
    """
    Загружает конфигурационные файлы
    """
    conf = dict()
    conf_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'webloader.conf')
    if not os.path.exists(conf_file):
        conf_file = '/etc/webloader/webloader.conf'
    if not os.path.exists(conf_file):
        msg = u'Ошибка чтения файла конфигурации. Файл %s не найден' % conf_file
        print(msg)
        sys.exit(0)

    cfg = configparser.RawConfigParser()
    try:
        cfg.read(conf_file)
    except configparser.Error as ce:
        msg = u'Ошибка чтения файла конфигурации. %s.'
        msg = msg % conf_file
        print(msg)
        sys.exit(0)

    if not cfg.has_section('log') or \
            not cfg.has_section('app') or \
            not cfg.has_section('db'):
        msg = u'Ошибка чтения секций файла конфигурации. %s.'
        msg = msg % conf_file
        print(msg)
        sys.exit(0)

    try:
        # Настройки приложения
        conf['tmp_store'] = cfg.get('app', 'tmp_store')
        conf['process_count'] = cfg.get('app', 'process_count')
        conf['thread_count_in_process'] = cfg.get('app', 'thread_count_in_process')
        conf['entity_list'] = cfg.get('app', 'entity_list').split(';')
        conf['interval_sec'] = cfg.get('app', 'interval_sec')

        # rebbitmq
        conf['rebbitmq_host'] = cfg.get('rebbitmq', 'host')
        conf['rebbitmq_port'] = cfg.get('rebbitmq', 'port')
        conf['rebbitmq_user'] = cfg.get('rebbitmq', 'user')
        conf['rebbitmq_password'] = cfg.get('rebbitmq', 'password')
        conf['rebbitmq_virtual_host'] = cfg.get('rebbitmq', 'virtual_host')
        conf['rebbitmq_ssl'] = cfg.get('rebbitmq', 'ssl')

        # Настройки логирования
        conf['log_file'] = cfg.get('log', 'log_file')
        conf['format'] = cfg.get('log', 'format')
        conf['log_level'] = cfg.get('log', 'level')
        conf['dateformat'] = cfg.get('log', 'dateformat')
        conf['maxBytes'] = cfg.getint('log', 'maxBytes')
        conf['backupCount'] = cfg.getint('log', 'backupCount')

        # База
        conf['dbname'] = cfg.get('db', 'dbname')
        conf['host'] = cfg.get('db', 'host')
        conf['port'] = cfg.get('db', 'port')
        conf['user'] = cfg.get('db', 'user')
        conf['password'] = cfg.get('db', 'password')
        conf['ssl'] = cfg.get('db', 'ssl')

    except (ConfigParser.Error, TypeError) as cpex:
        msg = u'Некорректные настройки %s. %s' % (conf_file, cpex)
        print(msg)
        sys.exit(0)

    if len(conf['entity_list']) < int(conf['thread_count_in_process']):
        msg = u'Количество сущностей меньше количества потоков в процессе!'
        print(msg)
        sys.exit(0)

    return conf


class Async_obj(object):
    async def __new__(cls, *a, **kw):
        instance = super().__new__(cls)
        await instance.__init__(*a, **kw)
        return instance


class Service(object):
    def __init__(self, logger, get_cookie, set_cookie, host, port, headers, params, ssl):
        logger.info(u'Service::__init__')
        self._file_log = logger
        self._get_cookie = get_cookie
        self._set_cookie = set_cookie
        self._host = host
        self._port = port
        self._headers = headers
        self._params = params
        self._ssl = ssl

    async def send_post(self):
        """
        Выполняет HTTP POST запрос
        """
        pass

    async def send_get(self, params):
        """
        Выполняет HTTP GET запрос
        """
        self._file_log.info('Service::send_get')
        self._headers.update(await self._get_cookie())
        vparams = params if params else self._params
        try:
            url = 'http://' + self._host if self._ssl != '1' else 'https://' + self._host
            self._file_log.info(u'Выполнение GET-запроса. url: %s,headers: %s' % (url, self._headers))
            start_get = datetime.now()
            async with aiohttp.ClientSession() as ses:
                resp = await ses.get(url, headers=self._headers, params=vparams, timeout=5)
        except Exception as exc:
            msg = u'Ошибка: %s.' % str(exc)
            print(msg)
            return False

        # обрабатываем ответ
        if resp.status == 200:
            result = await resp.text()
            resp_headers = resp.headers
        elif resp.status == 204:
            msg = u'Нет данных для загрузки. %s %s'
            self._file_log.info(msg % (resp.status, resp.text()))
            result = resp_headers = None
        else:
            msg = u'Внутренняя ошибка сервиса. %s %s'
            self._file_log.info(msg % (resp.status, resp.text()))
            result = resp_headers = None

        diff = (datetime.now() - start_get).total_seconds()
        self._file_log.info(u'Время выполнения GET %s: %s. status - %s' % (url, diff, resp.status))
        if resp_headers: await self._set_cookie(resp_headers.get('set-cookie'))
        return {'result': result, 'headers': resp_headers, 'status': resp.status}


class Rebbit(object):
    def __init__(self, logger, host, port, virtual_host, user, password, ssl):
        logger.info(u'Rebbit::__init__')
        self._file_log = logger
        self._host = host
        self._port = port
        self._virtual_host = virtual_host
        self._user = user
        self._password = password
        self._ssl = bool(int(ssl))
        self._con = None
        self._channel = None
        self._credentials = pika.PlainCredentials(self._user, self._password)
        self._parameters = pika.ConnectionParameters(host=self._host,
                                                     port=self._port,
                                                     virtual_host=self._virtual_host,
                                                     ssl=self._ssl,
                                                     credentials=self._credentials)

    def _rebbit_connect(self):
        self._file_log.info('Rebbit::_rebbit_connect')
        self._con = pika.BlockingConnection(self._parameters)

    def _rebbit_channel(self):
        self._file_log.info('Rebbit::_rebbit_channel')
        self._channel = self._con.channel()

    def get_channel(self):
        self._file_log.info(u'Rebbit::get_channel')
        # Если нет соединения поднимаем
        if not self._con:
            self._rebbit_connect()
        if not self._channel:
            self._rebbit_channel()
        return self._channel


class Database(object):
    def __init__(self, logger, host, port, dbname, user, password):
        logger.info(u'Database::__init__')
        self._file_log = logger
        self._dbname = dbname
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._con = None
        self._tr = None

    async def _db_connect(self):
        self._file_log.info('Database::_db_connect')
        self._con = await asyncpg.connect(host=self._host, port=self._port, database=self._dbname, user=self._user,
                                          password=self._password)
        """def _encoder(value):
            return json.dumps(value)
        def _decoder(value):
            return json.loads(value)
        await con.set_type_codec('json', encoder=_encoder, decoder=_decoder)"""

    async def _db_disconnect(self):
        self._file_log.info('Database::_db_disconnect')
        if self._con:
            await self._con.close()
            self._con = None

    async def start_transaction(self):
        self._file_log.info('Database::begin')
        if not self._con:
            await self._db_connect()
        self._tr = self._con.transaction()
        await self._tr.start()

    async def commit(self):
        self._file_log.info('Database::commit')
        await self._tr.commit()
        await self._db_disconnect()
        self._tr = None

    async def rollback(self):
        self._file_log.info('Database::rollback')
        await self._tr.rollback()
        await self._db_disconnect()
        self._tr = None

    async def callproc(self, procname, params=tuple(), isReturn=None):
        # 'UPDATE tbl SET info=$2 WHERE id=$1', id_, new)
        # select event_1xbet_i(%(code_bs)s, %(type_sport)s, %(node_id)s, %(player1)s, \
        #       await con.execute(
        #             'UPDATE tbl SET info=$2 WHERE id=$1', id_, new)
        # row = await conn.fetchrow(
        #        'SELECT * FROM users WHERE name = $1', 'Bob')
        # cur.execute("SELECT * FROM GET_USERS();")
        # types = await con.fetch('SELECT * FROM pg_type')


        self._file_log.info(u'Database::callproc')
        # Если нет соединения поднимаем
        if not self._con:
            await self._db_connect()
        # Составляем запрос
        if isReturn is None:
            prefix = 'select %s'
        else:
            prefix = 'select * from %s'
        sql = prefix % procname
        if len(params) > 0:
            sql += '(%s)' % ','.join(('%s',) * len(params))
            sql = sql % params

        self._file_log.info(u'-' * 100)
        self._file_log.info(u'SQL: %s' % sql)
        self._file_log.info(u'-' * 100)
        result = await self._con.fetch(sql)
        return result

    async def execute(self, sql, params=tuple()):
        self._file_log.info(u'Database::execute')
        # Если нет соединения поднимаем
        if not self._con:
            await self._db_connect()
        self._file_log.info(u'SQL: %s' % sql)
        self._file_log.info(u'Params: %s' % str(params))
        await self._con.execute(sql, *params)
        self._file_log.info(u'Database::execute end')

    async def fetch(self, sql, params=tuple()):
        self._file_log.info(u'Database::fetch')
        # Если нет соединения поднимаем
        if not self._con:
            await self._db_connect()
        self._file_log.info(u'SQL: %s' % sql)
        self._file_log.info(u'Params: %s' % str(params))
        rows = await self._con.fetch(sql, *params)
        self._file_log.info(u'Database::fetch end')
        return rows

    async def executemany(self, sql, params):
        self._file_log.info(u'Database::executemany')
        # Если нет соединения поднимаем
        if not self._con:
            await self._db_connect()
        self._file_log.info(u'SQL: %s' % sql)
        self._file_log.info(u'Params: %s' % str(params))
        await self._con.executemany(sql, *params)
        self._file_log.info(u'Database::executemany end')


def gen_guid():
    return str(uuid.uuid4()).replace('-', '').upper()
