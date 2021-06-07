import time
from multiprocessing import pool
import numpy as np
import pandas as pd


def initializare():
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('display.max_rows', 100)
    cf = pd.read_csv('data/lp2_proiecte_optiuni.csv')
    # new atributes
    cf['grupa'] = cf['Echipa'].apply(lambda x: x.split('-')[0])
    cf['domenii'] = cf['Optiuni'].apply(lambda x: x.split(',') if not pd.isna(x) else [])
    cf['d1'] = cf['domenii'].apply(lambda x: x[0].split('-')[0] if len(x) > 0 else '')
    cf['d2'] = cf['domenii'].apply(lambda x: x[1].split('-')[0] if len(x) > 1 else '')
    cf['d3'] = cf['domenii'].apply(lambda x: x[2].split('-')[0] if len(x) > 2 else '')

    # corectare domenii duplicat
    cf['d3'] = cf.apply(lambda x: x['d3'] if x['d3'] not in (x['d1'], x['d2']) else '', axis=1)
    cf['d2'], cf['d3'] = zip(*cf.
                             apply(lambda x: (x['d3'], '') if x['d1'] == x['d2'] else (x['d2'], x['d3']), axis=1))
    return cf


def apply_parallel(df, func):
    # split dataframe
    df_split = np.array_split(df, 6)
    # calculate metrics for each and concatenate
    df = pd.concat(pool.Pool.map(func, df_split))
    return df


def alocare(dff):
    # configs - 139 de utilizatori in cursul de LP2
    seed = 139

    # alocare runda 1
    r1 =apply_parallel(dff[dff['d1'] != ''].groupby(['grupa', 'd1']), lambda x: x.sample(n=1, replace=False, random_state=seed))["Echipa"].reset_index()
    r1_l = list(r1['Echipa'])
    dff['alocare'], dff['runda'] = \
        zip(*apply_parallel(dff,lambda x: (x['d1'], '1') if x['Echipa'] in r1_l else ('', '')))

    # corectie optiune 2, sa nu fie deja alocat domeniul in runda 1
    def cor(x):
        alese = list(dff[dff['grupa'] == x['grupa']]['alocare'])
        if (x['d2'] in alese) & (x['d3'] not in alese):
            return x['d3'], ''
        elif (x['d2'] in alese) & (x['d3'] in alese):
            return '', ''
        else:
            return x['d2'], x['d3']

    dff['d2'], dff['d3'] = zip(*apply_parallel(dff,cor))

    # alocare runda 2
    r2 = apply_parallel(dff[(dff['d2'] != '') & (dff['alocare'] == '')].groupby(['grupa', 'd2']),lambda x: x.sample(n=1, replace=False, random_state=seed))["Echipa"].reset_index()
    r2_l = list(r2['Echipa'])
    dff['alocare'], dff['runda'] = \
        zip(*apply_parallel(dff,lambda x: (x['d2'], '2') if x['Echipa'] in r2_l else (x['alocare'], x['runda'])))

    # corectie optiune 3, sa nu fie deja alocat domeniul in runda 1 sau 2
    dff['d3'] = apply_parallel(dff,
                               lambda x: '' if x['d3'] in list(dff[dff['grupa'] == x['grupa']]['alocare']) else x['d3'])

    # alocare runda 3
    r3 = apply_parallel( dff[(dff['d3'] != '') & (dff['alocare'] == '')].groupby(['grupa', 'd3']),lambda x: x.sample(n=1, replace=False, random_state=seed))

    if not r3.empty:
        r3_l = list(r3['Echipa'].reset_index())
        dff['alocare'], dff['runda'] = \
            zip(*apply_parallel(dff,lambda x: (x['d3'], '3') if x['Echipa'] in r3_l else (x['alocare'], x['runda']), axis=1))

    # tema alocata
    dff['tema_proiect'] = dff.apply(
        lambda x: next((t for t in x['domenii'] if (x['alocare'] != '') & (t.startswith(x['alocare']))),
                       'De alocat manual'), axis=1)

    # return dataframe
    dff.to_csv('data/mda.csv')


if __name__ == '__main__':
    # metrics
    start_time = time.time()
    alocare(initializare())
    print("Total execution time %s" % (time.time() - start_time))
