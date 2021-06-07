import time
import dask.dataframe as dd
import pandas as pd
import pystore
import quandl


def initializare():
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('display.max_rows', 100)

    # dff = pd.read_csv('data/lp2_proiecte_optiuni.csv')
    dff = pd.read_csv('data/lp2_proiecte_optiuni.csv')

    # new atributes
    dff['grupa'] = dff['Echipa'].apply(lambda x: x.split('-')[0])
    dff['domenii'] = dff['Optiuni'].apply(lambda x: x.split(',') if not pd.isna(x) else [])
    dff['d1'] = dff['domenii'].apply(lambda x: x[0].split('-')[0] if len(x) > 0 else '')
    dff['d2'] = dff['domenii'].apply(lambda x: x[1].split('-')[0] if len(x) > 1 else '')
    dff['d3'] = dff['domenii'].apply(lambda x: x[2].split('-')[0] if len(x) > 2 else '')

    # corectare domenii duplicat
    dff['d3'] = dff.apply(lambda x: x['d3'] if x['d3'] not in (x['d1'], x['d2']) else '', axis=1)
    dff['d2'], dff['d3'] = zip(*dff.
                                apply(lambda x: (x['d3'], '') if x['d1'] == x['d2'] else (x['d2'], x['d3']), axis=1))
    return dff


def alocare():
    # configs - 139 de utilizatori in cursul de LP2
    seed = 139
    dff = dd.from_pandas(initializare(), npartitions=9)

    try:
        # alocare runda 1
        r1 = dff[dff['d1'] != ''].groupby(['grupa', 'd1']). \
            apply(lambda x: x.sample(n=1, replace=False, random_state=seed), meta=object)["Echipa"].reset_index()
        r1_l = list(r1['Echipa'])
        dff['alocare'], dff['runda'] = \
            zip(*dff.apply(lambda x: (x['d1'], '1') if x['Echipa'] in r1_l else ('', ''), axis=1))
    except NotImplementedError as e:
        print("eroare ",e)

    # corectie optiune 2, sa nu fie deja alocat domeniul in runda 1
    def cor(x):
        alese = list(dff[dff['grupa'] == x['grupa']]['alocare'])
        if (x['d2'] in alese) & (x['d3'] not in alese):
            return x['d3'], ''
        elif (x['d2'] in alese) & (x['d3'] in alese):
            return '', ''
        else:
            return x['d2'], x['d3']


    try:
        dff['d2'], dff['d3'] = zip(*dff.apply(cor, axis=1))

        # alocare runda 2
        r2 = dff[(dff['d2'] != '') & (dff['alocare'] == '')].groupby(['grupa', 'd2']). \
            apply(lambda x: x.sample(n=1, replace=False, random_state=seed), meta=object)["Echipa"].reset_index()
        r2_l = list(r2['Echipa'])
        dff['alocare'], dff['runda'] = \
            zip(*dff.apply(lambda x: (x['d2'], '2') if x['Echipa'] in r2_l else (x['alocare'], x['runda']) ,axis=1))

        # corectie optiune 3, sa nu fie deja alocat domeniul in runda 1 sau 2
        dff['d3'] = dff. \
            apply(lambda x: '' if x['d3'] in list(dff[dff['grupa'] == x['grupa']]['alocare']) else x['d3'],meta='str', axis=1)

        # alocare runda 3
        r3 = dff[(dff['d3'] != '') & (dff['alocare'] == '')].groupby(['grupa', 'd3']). \
            apply(lambda x: x.sample(n=1, replace=False, random_state=seed), meta=object)
        if not r3.empty:
            r3_l = list(r3['Echipa'].reset_index())
            dff['alocare'], dff['runda'] = \
                zip(*dff.apply(lambda x: (x['d3'], '3') if x['Echipa'] in r3_l else (x['alocare'], x['runda']), axis=1))
            # tema alocata
        dff['tema_proiect'] = dff.apply(
            lambda x: next((t for t in x['domenii'] if (x['alocare'] != '') & (t.startswith(x['alocare']))),
                           'De alocat manual'), axis=1, meta=object)
    except KeyError:
        print("KeyError")

    # return dataframe
    return dff.compute()


# def lista():
#     df = initializare()
#     df1, df2, df3, df4, df5, df6, df7, df8, df9 = df[df['Echipa'].str.startswith('11')].reset_index(), df[ \
#         df['Echipa'].str.startswith('12')].reset_index(), df[df['Echipa'].str.startswith('21')].reset_index(), df[df[ \
#         'Echipa'].str.startswith('22')].reset_index(), df[df['Echipa'].str.startswith('31')].reset_index(), df[ \
#                                                       df['Echipa'].str.startswith(
#                                                           '32')].reset_index(), df[ \
#                                                       df['Echipa'].str.startswith('41')].reset_index(), df[ \
#                                                       df['Echipa'].str.startswith('42')].reset_index(), df[ \
#                                                       df['Echipa'].str.startswith('43')].reset_index()
#     return df1, df2, df3, df4, df5, df6, df7, df8, df9


# def parallelize_dataframe(df, func, n_cores=2):
#     df_split = np.array_split(df, n_cores)
#     pool = Pool(n_cores)
#     df = pd.concat(pool.map(func, df_split))
#     pool.close()
#     pool.join()
#     return df

if __name__ == '__main__':
    # metrics
    start_time = time.time()

    initializare()

    execution_time = time.time() - start_time
    print("Total execution time %s" % execution_time)
