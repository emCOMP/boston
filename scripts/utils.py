import os
from collections import Counter

def write_to_data(path=''):
    result = os.path.join(os.path.dirname(__file__),os.pardir,'data/') + path
    return result

def counter_data(count,gran=False):
    codes = {}

    if gran:
        codes['misinfo'] = count['misinfo']
        codes['speculation'] = count['speculation']
        codes['hedge'] = count['hedge']
        codes['correction'] = count['correction']
        codes['question'] = count['question']
        codes['other'] = count['unrelated'] + count['other/unclear/neutral'] + count['unclear'] + count[''] + count['discussion - justifying'] + count['discussion - question'] + count['other'] + count['discussion']

    else:
        codes['misinfo'] = count['misinfo'] + count['speculation'] + count['hedge']
        codes['correction'] = count['correction'] + count['question']
        codes['other'] = count['unrelated'] + count['other/unclear/neutral'] + count['unclear'] + count[''] + count['discussion - justifying'] + count['discussion - question'] + count['other'] + count['discussion']

    return codes
