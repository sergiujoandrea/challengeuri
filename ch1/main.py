import time
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


def alocare(dff):
    # configs - 139 de utilizatori in cursul de LP2
    seed = 139

    # alocare runda 1
    r1 = dff[dff['d1'] != ''].groupby(['grupa', 'd1']). \
        apply(lambda x: x.sample(n=1, replace=False, random_state=seed))["Echipa"].reset_index()
    r1_l = list(r1['Echipa'])
    dff['alocare'], dff['runda'] = \
        zip(*dff.apply(lambda x: (x['d1'], '1') if x['Echipa'] in r1_l else ('', ''), axis=1))

    # corectie optiune 2, sa nu fie deja alocat domeniul in runda 1
    def cor(x):
        alese = list(dff[dff['grupa'] == x['grupa']]['alocare'])
        if (x['d2'] in alese) & (x['d3'] not in alese):
            return x['d3'], ''
        elif (x['d2'] in alese) & (x['d3'] in alese):
            return '', ''
        else:
            return x['d2'], x['d3']

    dff['d2'], dff['d3'] = zip(*dff.apply(cor, axis=1))

    # alocare runda 2
    r2 = dff[(dff['d2'] != '') & (dff['alocare'] == '')].groupby(['grupa', 'd2']). \
        apply(lambda x: x.sample(n=1, replace=False, random_state=seed))["Echipa"].reset_index()
    r2_l = list(r2['Echipa'])
    dff['alocare'], dff['runda'] = \
        zip(*dff.apply(lambda x: (x['d2'], '2') if x['Echipa'] in r2_l else (x['alocare'], x['runda']), axis=1))

    # corectie optiune 3, sa nu fie deja alocat domeniul in runda 1 sau 2
    dff['d3'] = dff. \
        apply(lambda x: '' if x['d3'] in list(dff[dff['grupa'] == x['grupa']]['alocare']) else x['d3'], axis=1)

    # alocare runda 3
    r3 = dff[(dff['d3'] != '') & (dff['alocare'] == '')].groupby(['grupa', 'd3']). \
        apply(lambda x: x.sample(n=1, replace=False, random_state=seed))
    if not r3.empty:
        r3_l = list(r3['Echipa'].reset_index())
        dff['alocare'], dff['runda'] = \
            zip(*dff.apply(lambda x: (x['d3'], '3') if x['Echipa'] in r3_l else (x['alocare'], x['runda']), axis=1))

    # tema alocata
    dff['tema_proiect'] = dff.apply(
        lambda x: next((t for t in x['domenii'] if (x['alocare'] != '') & (t.startswith(x['alocare']))),
                       'De alocat manual'), axis=1)

    # return dataframe
    dff.to_csv('data/mda.csv')

# am incercat sa imparte pe subgrupe pentru a aplica programul ca o functie de alocare, dar nu am reusit sa scot timpi
# mai buni
# def lista(): df = pd.read_csv('data/lp2_proiecte_optiuni.csv') df = initializare(df) df1, df2, df3, df4, df5, df6,
# df7, df8, df9 = df[df['Echipa'].str.startswith('11')].reset_index(), df[ \ df['Echipa'].str.startswith(
# '12')].reset_index(), df[df['Echipa'].str.startswith('21')].reset_index(), df[df[ \ 'Echipa'].str.startswith(
# '22')].reset_index(), df[df['Echipa'].str.startswith('31')].reset_index(), df[ \ df['Echipa'].str.startswith(
# '32')].reset_index(), df[ \ df['Echipa'].str.startswith('41')].reset_index(), df[ \ df['Echipa'].str.startswith(
# '42')].reset_index(), df[ \ df['Echipa'].str.startswith('43')].reset_index() return df1, df2, df3, df4, df5, df6,
# df7, df8, df9

if __name__ == '__main__':
    # metrics
    start_time = time.time()

    alocare(
        initializare())  # nu mai am printat dataframe-ul pt ca dureaza cu pana la 0.02s in plus  ceea ce ii aprox
    # 1/6 din timpul total

    s = time.time() - start_time  # salvam timpul inainte pt ca printul mai dureaza ceva timp
    print("Total execution time %s" % s)
    #Total execution time 0.10571837425231934 fata de # Total execution time 0.12392163276672363