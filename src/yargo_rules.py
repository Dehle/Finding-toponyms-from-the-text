# -*- coding: utf-8 -*-

# импорт библиотек Yargy
from yargy import rule, Parser, or_
from yargy.pipelines import morph_pipeline
from yargy.interpretation import fact

from yargy.predicates import (
    eq, lte, gte, gram, in_caseless, dictionary,
    normalized, caseless
)


def printer(value):
    return value.title()


def load_lines(path):
    # загружаем словари по адресу
    with open(path, 'r', encoding='utf-8') as file:
        for line in file:
            yield line.rstrip('\n').strip()


# Заргузка словарей
DICT_CITY1 = set(load_lines('dict/dict_city1.txt'))
DICT_CITY2 = set(load_lines('dict/dict_city2.txt'))
DICT_RESP = set(load_lines('dict/dict_resp.txt'))
DICT_KRAI = set(load_lines('dict/dict_krai.txt'))
DICT_OBL = set(load_lines('dict/dict_obl.txt'))
DICT_OKRUG = set(load_lines('dict/dict_okrug.txt'))
DICT_RAION = set(load_lines('dict/dict_raion.txt'))
DICT_MSK_R = set(load_lines('dict/dict_msk_r.txt'))

# костанты
SPACE = eq(' ')
DASH = eq('-')
DOT = eq('.')

#####################################################
#                                                   #
#                      Город                        #
#                                                   #
#####################################################

# факт область
City = fact(
    'City',
    ['name']
)

COMPLEX = morph_pipeline(DICT_CITY1)

SIMPLE = morph_pipeline(DICT_CITY2)
# любим, зима, чехов, майский, белый, свободный, шали,
# ясный, кола, ковров, радужный, покров, Александров, Владимир, грязи, короча, лесной
SPB = morph_pipeline({
    'спб',
    'питер'
}).interpretation(City.name.const('Санкт-Петербург'))

MSK = morph_pipeline({
    'мск',
    'масква'
}).interpretation(City.name.const('Москва'))

NSK = morph_pipeline({
    'нск',
    'нсиб',
    'новосиб'
}).interpretation(City.name.const('Новосибирск'))

EKB = morph_pipeline({
    'екб',
    'ебург',
    'екат',
    'ект'
}).interpretation(City.name.const('Екатеринбург'))
ZLN = morph_pipeline({
    'зелик',
    'зелек',
    'злн'
}).interpretation(City.name.const('Зеленоград'))
NN = morph_pipeline({
    'НиНо',
    'нино',
    'НН'
}).interpretation(City.name.const('Нижний Новгород'))
RND = morph_pipeline({
    'рнд'
}).interpretation(City.name.const('Ростов-на-Дону'))

# Сим - город, который реагирует на слово сих и сей

GOROD_NAME = or_(
    SIMPLE.interpretation(
        City.name.normalized().custom(printer)
    ),
    COMPLEX.interpretation(
        City.name.normalized().custom(printer)
    ),
    SPB,
    MSK,
    NSK,
    EKB,
    ZLN,
    NN,
    RND
)

CITY = rule(
    GOROD_NAME
).interpretation(
    City
)

#####################################################
#                                                   #
#                      Область                      #
#                                                   #
#####################################################

# факт - область
Oblast = fact(
    'Oblast',
    ['name']
)

#
MOS = or_(normalized('мос'), normalized('моск'))
OBL = or_(normalized('обл'), normalized('область'))
MO = or_(normalized('мо'), normalized('подмосковье'))

MOS_OBL = or_(
    rule(MOS, DOT.optional(), OBL, DOT.optional()),
    rule(MOS, DOT.optional(), SPACE, OBL, DOT.optional()),
    rule(normalized('мо')),
    rule(normalized('мособласть')),
    rule(normalized('подмосковье')),
    rule(normalized('м'), DOT.optional(), normalized('о'))
)
LEN_OBL = or_(
    rule(normalized('лен'), SPACE, OBL, DOT.optional()),
    rule(normalized('лен'), DOT.optional(), OBL, DOT.optional()),
    rule(normalized('ленобласть'))
)
# inflected({'femn'})
OBL_NAME = morph_pipeline(DICT_OBL).interpretation(Oblast.name.normalized().custom(printer))

OBL_RULE = rule(
    or_(
        rule(OBL_NAME),
        rule(MOS_OBL).interpretation(Oblast.name.const('Московская')),
        rule(LEN_OBL).interpretation(Oblast.name.const('Ленинградская'))
    )
).interpretation(Oblast)

#####################################################
#                                                   #
#                      Республика                   #
#                                                   #
#####################################################

Respublika = fact(
    'Respublika',
    ['name']
)

RESP_NAME = morph_pipeline(DICT_RESP).interpretation(Respublika.name.normalized().custom(printer))
RESP_RULE = rule(RESP_NAME).interpretation(Respublika)

#####################################################
#                                                   #
#                      Край                         #
#                                                   #
#####################################################

Krai = fact(
    'Krai',
    ['name']
)

KRAI_NAME = morph_pipeline(DICT_KRAI).interpretation(Krai.name.normalized().custom(printer))
KRAI_RULE = rule(KRAI_NAME).interpretation(Krai)

#####################################################
#                                                   #
#                      Округ                        #
#                                                   #
#####################################################

Okrug = fact(
    'Okrug',
    ['name']
)

OKRUG_NAME = morph_pipeline(DICT_OKRUG).interpretation(Okrug.name.normalized().custom(printer))
OKRUG_RULE = rule(OKRUG_NAME).interpretation(Okrug)

#####################################################
#                                                   #
#                      Район                        #
#                                                   #
#####################################################

Raion = fact(
    'Raion',
    ['name']
)

RAION_NAME = morph_pipeline(DICT_RAION).interpretation(Raion.name.normalized().custom(printer))
RAION_RULE = rule(RAION_NAME).interpretation(Raion)

#####################################################
#                                                   #
#                Районы Москвы                      #
#                                                   #
#####################################################

Msk_raion = fact('Msk_raion', ['name'])

MSK_R_NAME = morph_pipeline(DICT_MSK_R).interpretation(Msk_raion.name.normalized().custom(printer))

MSK_R_RULE = rule(MSK_R_NAME).interpretation(Msk_raion)
