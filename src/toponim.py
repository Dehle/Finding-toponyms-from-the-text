# -*- coding: utf-8 -*-
"""
Created on Thu Aug  22 16:20:52 2022
@author: https://github.com/vik1109/
"""
import os
import pandas as pd
import argparse
import glob
from datetime import datetime
import logging
from toponim_parser_yargo import ToponimParserYargo

_logger = logging.getLogger(__name__)


# Валидация пути прописанного в аргументах
def validate_path(path: str) -> str:
    if (not os.path.isabs(path)) or (not os.path.exists(path)):
        raise argparse.ArgumentTypeError(f'Absolute and exist path required, got "{path}"')
    return os.path.normpath(path)


# преобразовываем текстовый файл в DataFrame со столбцами: ['date', 'user', 'msg']
def convert_file(msg) -> pd.DataFrame:
    listed_data = []  # будущий список сообщений
    tmp_msg = ''  # временная переменная для сообщения
    tmp_user = ''  # временная переменная для имени пользователя
    tmp_date = ''  # временная переменная для даты
    quota = 0  # переменная для хранения индекса запятой

    # интересующие нас сообщения хранятся в последнем элементе списка first_split
    # еще раз разбиваем список
    second_split = msg[-1].split('\n\n________________\n\n')

    # цикл поиска запятой и символа '\n'
    for item in second_split:
        for i in range(len(item)):
            # запомним индекс запятой
            if item[i] == ',':
                quota = i
            # найдем первый \n
            if item[i] == '\n':
                # подстрока с сообщением. от символа \n до конца строки
                tmp_msg = item[(i + 1):].strip()
                if tmp_msg.find(']') != -1:
                    tmp_msg = tmp_msg[(tmp_msg.find(']') + 3):]
                # подстрока с датой от начала до запятой
                tmp_date = item[:(quota)]
                tmp_date = ' '.join(tmp_date.split(' ')[:-1]).strip()  # оставим только дату
                # подстрока с именем пользователя от запятой до \n
                tmp_user = item[(quota + 1):(i)].strip()
                # заканчиваем цикл досрочно
                break
        # делаем список [tmp_date, tmp_user, tmp_msg] и закидиваем в списое сообщений
        listed_data.append([tmp_date, tmp_user, tmp_msg])
        # не обязательно, но мне кажется так лучше, что бы случайностей не было
        tmp_msg = ''
        tmp_user = ''
        tmp_date = ''
        quota = 0
    # из списка делаем датафрейм
    df = pd.DataFrame(listed_data, columns=['date', 'user', 'msg'])

    # удалим строки, где поле msg пустое
    df = df.drop(df[(df['msg'] == '')].index).reset_index(drop=True)
    return df


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
    # парсим коммандную строку. Путь по умолчанию - папка из которой запущен скрипт.
    parser = argparse.ArgumentParser(description='My example explanation')
    parser.add_argument(
        '--path_from',
        metavar='PATH',
        type=validate_path,
        help='Folder with *.txt files for reading',
        default=os.path.abspath(os.curdir)
    )
    parser.add_argument(
        '--path_to',
        metavar='PATH',
        type=validate_path,
        help='Folder for recording a report',
        default=os.path.abspath(os.curdir)
    )
    parser.add_argument(
        "--log_path",
        metavar="PATH",
        type=validate_path,
        help="Folder for log file",
        default=os.path.abspath(os.curdir)

    )
    args = parser.parse_args()

    setup_logging(os.path.join(args.log_path, "parser.log"), "INFO")

    # создаем DataFrame для хранения сообщений
    df = pd.DataFrame(columns=['date', 'user', 'msg'])

    # цикл чтения всех файлов. В случае не соблюдения формата файлов - ловим ошибку
    for filename in glob.glob(os.path.join(args.path_from, '*.txt')):
        print(os.path.join(args.path_from, filename))
        file_path = os.path.join(args.path_from, filename)
        try:
            with open(file_path, 'r', encoding='utf8') as f:
                df = pd.concat(
                    [convert_file(f.read().split('\n--------------------------\n')),
                     df]
                )
        except IOError as err:
            _logger.error("File read error: %s, %s", file_path, repr(err))
            exit()

    toponims = ToponimParserYargo(df)
    toponims.pars_all()
    # название отчета формируется из слова report_ и текущуй даты и текущего времени.
    report = os.path.join(args.path_to, 'report_' + str(datetime.now().strftime("%d_%m_%Y_%H_%M_%S")) + '.csv')
    toponims.to_csv(report)
