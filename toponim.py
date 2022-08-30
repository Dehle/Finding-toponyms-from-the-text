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
from toponim_parser_yargo import ToponimParserYargo


# Валидация пути прописанного в аргументах
def validate_path(path: str) -> str:
    if (not os.path.isabs(path)) or (not os.path.exists(path)):
        raise argparse.ArgumentTypeError(f'Absolute and exist path required, got "{path}"')

    return os.path.normpath(path)


# преобразовываем текстовый файл в DataFrame со столбцами: ['date', 'user', 'msg']
def base_create(msg) -> pd.DataFrame:
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
    paths = parser.parse_args()

    # создаем DataFrame для хранения сообщений
    df = pd.DataFrame(columns=['date', 'user', 'msg'])

    # цикл чтения всех файлов. В случае не соблюдения формата файлов - ловим ошибку
    try:
        for filename in glob.glob(paths.path_from + '\*.txt'):
            print(os.path.join(paths.path_from, filename))
            with open(os.path.join(paths.path_from, filename), 'r', encoding='utf8') as f:
                df = pd.concat(
                    [base_create(f.read().split('\n--------------------------\n')),
                     df]
                )

    except:
        print("ошибка чтения файла")
        exit()
    toponims = ToponimParserYargo(df)
    toponims.pars_all()
    print(toponims.df.head())
    # название отчета формируется из слова report_ и текущуй даты и текущего времени.
    report = os.path.join(paths.path_to, 'report_' + str(datetime.now().strftime("%d_%m_%Y_%H_%M_%S")) + '.csv')
    toponims.to_csv(report)
