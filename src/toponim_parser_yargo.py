# -*- coding: utf-8 -*-
"""
Получаем на вход выгрузку от сервиса https://vk.barkov.net/comments.aspx
отбираем топонимы
выгружаем в папку DB подготовленный файл 'msg.cvs', sep = ';', encoding='utf-8-sig' 
"""

# imports
import pandas as pd
from yargo_rules import CITY, RESP_RULE, KRAI_RULE, OBL_RULE, OKRUG_RULE, MSK_R_RULE, RAION_RULE
from yargy import Parser
import os
import logging

_logger = logging.getLogger(__name__)


class ToponimParserYargo:

    def __init__(self, msg_df):
        """
        
        """
        # инициализация всех столбцов
        self.df = msg_df.copy()
        self.df['city'] = ''
        self.df['resp'] = ''
        self.df['krai'] = ''
        self.df['obl'] = ''
        self.df['okr'] = ''
        self.df['msk_r'] = ''
        self.df['raion'] = ''
        self.df["drugs"] = ""
        self.df["symptoms"] = ""
        self.df["allergens"] = ""

        # загрузка классификаторов
        file_path = os.path.dirname(__file__)
        read_params = {"sep": ";", "encoding": "utf-8"}
        self.cities_classifier = pd.read_csv(os.path.join(file_path, "dict", "cities_classifier.csv"), **read_params)
        self.regions_classifier = pd.read_csv(os.path.join(file_path, "dict", "regions.csv"), **read_params)
        self.krai_classifier = pd.read_csv(os.path.join(file_path, "dict", "krai.csv"), **read_params)
        self.resp_classifier = pd.read_csv(os.path.join(file_path, "dict", "resp.csv"), **read_params)
        self.okrug_classifier = pd.read_csv(os.path.join(file_path, "dict", "okrug.csv"), **read_params)

    def pars_all(self):
        self.toponim_parser()

        self.msk_r_to_city()
        self.obl_to_city()
        self.krai_to_city()
        self.okrug_to_city()
        self.pesp_to_city()
        self.city_id()

    def to_csv(self, name):
        self.df.to_csv(name, sep=';', encoding='utf-8-sig')
        _logger.info("Output file is saved to %s", name)

    def toponim_parser(self):
        _logger.info('start')
        parser = Parser(CITY)
        self.df['city'] = self.df.msg.apply(lambda x: ', '.join([match.fact.name for match in parser.findall(x)]))
        _logger.info('CITY')
        parser = Parser(RESP_RULE)
        self.df['resp'] = self.df.msg.apply(lambda x: ', '.join([match.fact.name for match in parser.findall(x)]))
        _logger.info('RESP_RULE')
        parser = Parser(KRAI_RULE)
        self.df['krai'] = self.df.msg.apply(lambda x: ', '.join([match.fact.name for match in parser.findall(x)]))
        _logger.info('KRAI_RULE')
        parser = Parser(OBL_RULE)
        self.df['obl'] = self.df.msg.apply(lambda x: ', '.join([match.fact.name for match in parser.findall(x)]))
        _logger.info('OBL_RULE')
        parser = Parser(OKRUG_RULE)
        self.df['okr'] = self.df.msg.apply(lambda x: ', '.join([match.fact.name for match in parser.findall(x)]))
        _logger.info('OKRUG_RULE')
        parser = Parser(MSK_R_RULE)
        self.df['msk_r'] = self.df.msg.apply(lambda x: ', '.join([match.fact.name for match in parser.findall(x)]))
        _logger.info('MSK_R_RULE')
        parser = Parser(RAION_RULE)
        self.df['raion'] = self.df.msg.apply(lambda x: ', '.join([match.fact.name for match in parser.findall(x)]))
        _logger.info('RAION_RULE')

        _logger.info('msg done')

    def city_id(self):
        self.cities_classifier.rename(columns={"id": "city_id"}, inplace=True)
        self.df = self.df.merge(self.cities_classifier, on="city", how="left")
        self.df["city_id"] = self.df["city_id"].fillna(-1).astype(int).apply(lambda x: str(x) if x > 0 else "")
        _logger.info('id done')

    def obl_to_city(self):
        # область в город
        for i in range(len(self.df)):
            try:
                if self.df.iloc[i]['obl']:
                    if not self.df.iloc[i]['city']:
                        self.df.iloc[i]['city'] = str(self.regions_classifier.loc[self.regions_classifier['region'] ==
                                                                                  self.df.iloc[i]['obl'], 'city'])
            except Exception as e:
                _logger.warning("%s", repr(e))
        _logger.info('obl done')

    def msk_r_to_city(self):
        # район москвы в город
        for i in range(len(self.df)):
            try:
                if self.df.iloc[i]['msk_r']:
                    if not self.df.iloc[i]['city']:
                        self.df.iloc[i]['city'] = "Москва"
            except Exception as e:
                _logger.warning("%s", repr(e))
        _logger.info('msk done')

    def krai_to_city(self):
        # край в город
        for i in range(len(self.df)):
            try:
                if self.df.iloc[i]['krai']:
                    if not self.df.iloc[i]['city']:
                        self.df.iloc[i]['city'] = str(
                            self.krai_classifier.loc[self.krai_classifier['krai'] == self.df.iloc[i]['krai'], 'city'].values[0])
            except Exception as e:
                _logger.warning("%s", repr(e))
        _logger.info('krai done')

    def okrug_to_city(self):
        # округ в город
        for i in range(len(self.df)):
            try:
                if self.df.iloc[i]['okr']:
                    if not self.df.iloc[i]['city']:
                        self.df.iloc[i]['city'] = str(self.okrug_classifier.loc[
                                                         self.okrug_classifier['okrug'] == self.df.iloc[i]['okr'], 'city'].values[0])
            except Exception as e:
                _logger.warning("%s", repr(e))
        _logger.info('okrug done')

    def pesp_to_city(self):
        # республика в город
        for i in range(len(self.df)):
            try:
                if self.df.iloc[i]['resp']:
                    if not self.df.iloc[i]['city']:
                        self.df.iloc[i]['city'] = str(
                            self.resp_classifier.loc[self.resp_classifier['resp'] == self.df.iloc[i]['resp'], 'city'].values[0])
            except Exception as e:
                _logger.warning("%s", repr(e))
        _logger.info('resp done')


if __name__ == "__main__":
    pass
