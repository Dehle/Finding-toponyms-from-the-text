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

class ToponimParserYargo:

    def __init__(self, msg_df):
        """
        
        """
        # инициализация всех столбцов
        self.df = msg_df.copy()
        self.df['city'] = ''
        self.df['city_id'] = ''
        self.df['resp'] = ''
        self.df['krai'] = ''
        self.df['obl'] = ''
        self.df['okr'] = ''
        self.df['msk_r'] = ''
        self.df['raion'] = ''

        # загрузка классификаторов
        file_path = os.path.dirname(__file__)
        self.cities_classifier = pd.read_csv(os.path.join(file_path, "dict", "cities_classifier.csv"), sep=';', encoding='utf-8')
        self.regions_classifier = pd.read_csv(os.path.join(file_path, "dict", "regions.csv"), sep=';', encoding='utf-8')
        self.krai_classifier = pd.read_csv(os.path.join(file_path, "dict", "krai.csv"), sep=';', encoding='utf-8')
        self.resp_classifier = pd.read_csv(os.path.join(file_path, "dict", "resp.csv"), sep=';', encoding='utf-8')
        self.okrug_classifier = pd.read_csv(os.path.join(file_path, "dict", "okrug.csv"), sep=';', encoding='utf-8')

    def pars_all(self):
        self.toponim_parser()
        print('msg done')
        self.msk_r_to_city()
        print('msk done')
        self.obl_to_city()
        print('obl done')
        self.krai_to_city()
        print('krai done')
        self.okrug_to_city()
        print('okrug done')
        self.pesp_to_city()
        print('resp done')
        self.city_id()
        print('id done')

    def to_csv(self, name='DB/msg.csv'):
        # self.df = self.df.drop(
        #    self.df[(self.df['city'] == '') & (self.df['moscow_district'] == '') &
        #            (self.df['region'] == '') &
        #            (self.df['area'] == '')].index).reset_index(drop=True)
        self.df.to_csv(name, sep=';', encoding='utf-8-sig')
        print('Successful!!')

    def toponim_parser(self):
        print('start')
        parser = Parser(CITY)
        self.df['city'] = self.df.msg.apply(lambda x: ', '.join([match.fact.name for match in parser.findall(x)]))
        print('CITY')
        parser = Parser(RESP_RULE)
        self.df['resp'] = self.df.msg.apply(lambda x: ', '.join([match.fact.name for match in parser.findall(x)]))
        print('RESP_RULE')
        parser = Parser(KRAI_RULE)
        self.df['krai'] = self.df.msg.apply(lambda x: ', '.join([match.fact.name for match in parser.findall(x)]))
        print('KRAI_RULE')
        parser = Parser(OBL_RULE)
        self.df['obl'] = self.df.msg.apply(lambda x: ', '.join([match.fact.name for match in parser.findall(x)]))
        print('OBL_RULE')
        parser = Parser(OKRUG_RULE)
        self.df['okr'] = self.df.msg.apply(lambda x: ', '.join([match.fact.name for match in parser.findall(x)]))
        print('OKRUG_RULE')
        parser = Parser(MSK_R_RULE)
        self.df['msk_r'] = self.df.msg.apply(lambda x: ', '.join([match.fact.name for match in parser.findall(x)]))
        print('MSK_R_RULE')
        parser = Parser(RAION_RULE)
        self.df['raion'] = self.df.msg.apply(lambda x: ', '.join([match.fact.name for match in parser.findall(x)]))
        print('RAION_RULE')

    def city_id(self):
        # добавляем city_id
        for i in range(len(self.df)):
            try:
                if self.df['city'][i]:
                    self.df.loc[i, 'city_id'] = str(
                        self.cities_classifier.loc[self.cities_classifier['city'] == self.df['city'][i], 'id'].values[
                            0])
            except:
                pass

    def obl_to_city(self):
        # область в город
        for i in range(len(self.df)):
            try:
                if self.df['obl'][i]:
                    if not self.df.loc[i, 'city']:
                        self.df.loc[i, 'city'] = str(self.regions_classifier.loc[
                                                         self.regions_classifier['region'] == self.df['obl'][
                                                             i], 'city'].values[0])
            except:
                pass

    def msk_r_to_city(self):
        # район москвы в город
        for i in range(len(self.df)):
            try:
                if self.df['msk_r'][i]:
                    if not self.df.loc[i, 'city']:
                        self.df.loc[i, 'city'] = "Москва"
            except:
                pass

    def krai_to_city(self):
        # край в город
        for i in range(len(self.df)):
            try:
                if self.df['krai'][i]:
                    if not self.df.loc[i, 'city']:
                        self.df.loc[i, 'city'] = str(
                            self.krai_classifier.loc[self.krai_classifier['krai'] == self.df['krai'][i], 'city'].values[
                                0])
            except:
                pass

    def okrug_to_city(self):
        # округ в город
        for i in range(len(self.df)):
            try:
                if self.df['okrug'][i]:
                    if not self.df.loc[i, 'city']:
                        self.df.loc[i, 'city'] = str(self.okrug_classifier.loc[
                                                         self.okrug_classifier['okrug'] == self.df['okr'][
                                                             i], 'city'].values[0])
            except:
                pass

    def pesp_to_city(self):
        # республика в город
        for i in range(len(self.df)):
            try:
                if self.df['resp'][i]:
                    if not self.df.loc[i, 'city']:
                        self.df.loc[i, 'city'] = str(
                            self.resp_classifier.loc[self.resp_classifier['resp'] == self.df['resp'][i], 'city'].values[
                                0])
            except:
                pass


if __name__ == "__main__":
    pass
