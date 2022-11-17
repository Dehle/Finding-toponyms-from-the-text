# -*- coding: utf-8 -*-
"""
Created on Thu Nov 17 18:26:14 2022
@author: https://github.com/vik1109/
"""
import os
import pandas as pd
import argparse
import glob
from datetime import datetime
import logging
from toponim_parser_yargo import ToponimParserYargo
import re

_logger = logging.getLogger(__name__)


# Валидация пути прописанного в аргументах
def validate_path(path: str) -> str:
    if (not os.path.isabs(path)) or (not os.path.exists(path)):
        raise argparse.ArgumentTypeError(f'Absolute and exist path required, got "{path}"')
    return os.path.normpath(path)

def validate_file_csv(file_name: str):
    if (not os.path.exists(file_name)):
        raise argparse.ArgumentTypeError(f'File required, got "{file_name}"')
    return file_name

def setup_logging(logfile=None, loglevel="INFO"):
    """

    :param logfile:
    :param loglevel:
    :return:
    """
    if logfile is None:
        logfile = os.path.join(os.path.dirname(__file__), "parser.log")

    loglevel = getattr(logging, loglevel)

    logger = logging.getLogger()
    logger.setLevel(loglevel)
    fmt = '%(asctime)s: %(levelname)s: %(filename)s: ' + \
          '%(funcName)s(): %(lineno)d: %(message)s'
    formatter = logging.Formatter(fmt)

    fh = logging.FileHandler(filename=logfile)
    fh.setLevel(loglevel)
    fh.setFormatter(formatter)

    ch = logging.StreamHandler()
    ch.setLevel(loglevel)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)


if __name__ == "__main__":
    #шаблон для чистки сообщений
    BAD_PART = re.compile(r'^\[\S+|S+\],\s?')
    
    # парсим коммандную строку. Путь по умолчанию - папка из которой запущен скрипт.
    descriptions = '''Required csv file name with full path.\n
    Required full path to save report\n
    Required full path to save log files'''
    try:
        parser = argparse.ArgumentParser(description = descriptions, exit_on_error=False)
        #парсинг имени файла
        parser.add_argument(
            '--file_csv',
            metavar='csv_file_name',
            type=validate_file_csv,
            help='Folder with *.txt files for reading',
            default=os.path.join(os.path.abspath(os.curdir), 'comments.csv')
        )
        #парсинг папки для хранения отчетов
        parser.add_argument(
            '--path_to',
            metavar='PATH',
            type=validate_path,
            help='Folder for recording a report',
            default=os.path.abspath(os.curdir)
        )
        #парсинг папки для хранения логов
        parser.add_argument(
            "--log_path",
            metavar="PATH",
            type=validate_path,
            help="Folder for log file",
            default=os.path.abspath(os.curdir)
        )
        args = parser.parse_args()
    
        setup_logging(os.path.join(args.log_path, "parser.log"), "INFO")
      
        #читаем сообщения
        try:
            comments = pd.read_csv(args.file_csv)
        except IOError as err:
            _logger.error("File read error: %s, %s", args.file_csv, repr(err))
            exit()
        #чистим пропуски в сообщениях
        comments = comments.dropna().reset_index(drop = True)
        #переименование столбца с сообщениями
        comments = comments.rename(columns = {'text': 'msg'})
        #чистка сообщений от технических моментов
        comments['msg'] = comments['msg'].apply(lambda x: BAD_PART.sub('', x, 1).strip())
    
        if (len(comments)>0):
            #парсим топонимы
            toponims = ToponimParserYargo(comments[['datetime', 'msg']])
            toponims.pars_all()
            
            # название отчета формируется из слова report_ и текущуй даты и текущего времени.
            report = os.path.join(args.path_to, 'report_' + str(datetime.now().strftime("%d_%m_%Y_%H_%M_%S")) + '.csv')
            toponims.to_csv(report)
    except SystemExit as err:
        print(err)
        exit()
