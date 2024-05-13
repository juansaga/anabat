import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)

playlist = pd.read_csv('PLAYLIST_SEPARADO.csv', sep = ';')
junto = pd.read_csv('junto2.csv', sep = ';')
codigos = pd.read_csv('codigos2.csv', sep = ';')

junto['PlaylistID'] = junto['PlaylistID'].apply(lambda x: x.replace('-', '_'))
playlist['PlaylistID'] = playlist['PlaylistID'].apply(lambda x: x.replace('-', '_'))

melted_df = playlist.melt(id_vars=['Group', 'ID_leaf', 'PlaylistID'], 
                    value_vars=['L1','L2','L3','L4','SIL','P1','SIL_1','P2','SIL_2','P3','SIL_3','P4','SIL_4','A1','SIL_5','A2','SIL_6','A3','SIL_7','A4'],
                    var_name='segmento', 
                    value_name='codigo_tratamiento')

segmentos = {'START' : 0,
            'L1' : 1.4,
            'L2' : 17.4,
            'L3' : 33.4,
            'L4' : 49.4,
            'SIL' : 65.4,
            'TRAN1' : 70.4,
            'P1' : 71.6,
            'SIL_1' : 87.6,
            'P2' : 103.6,
            'SIL_2' : 119.6,
            'P3' : 135.6,
            'SIL_3' : 151.6,
            'P4' : 167.6,
            'SIL_4' : 183.6,
            'TRAN2' : 199.6,
            'A1' : 200.8,
            'SIL_5' : 216.8,
            'A2' : 232.8,
            'SIL_6' : 248.8,
            'A3' : 264.8,
            'SIL_7' : 280.8,
            'A4' : 296.8,
            'END': 312.8
            }

segmentos_2 = {'START' : 0,
    'P1' : 1.4,
    'SIL_1' : 17.4,
    'P2' : 33.4,
    'SIL_2' : 49.4,
    'P3' : 65.4,
    'SIL_3' : 81.4,
    'P4' : 97.4,
    'SIL_4' : 113.4,
    'TRAN2' : 129.4,
    'A1' : 130.2,
    'SIL_5' : 143.3,
    'A2' : 162.6,
    'SIL_6' : 178.6,
    'A3' : 191.6,
    'SIL_7' : 207.6,
    'A4' : 223.6,
    'END': 238.6
}

def find_segment(time, playlistid):
    if int(playlistid[-1]) == 1 or int(playlistid[-1]) == 2:
        previous_segment = 'START'
        for segment, value in segmentos.items():
            if time < value:
                return previous_segment
            previous_segment = segment

    previous_segment = 'START'
    for segment, value in segmentos_2.items():
        if time < value:
            return previous_segment
        previous_segment = segment
    return previous_segment

junto['segmento'] = junto.apply(lambda row: find_segment(row['Begin Time (s)'] - row['START'], row['PlaylistID']), axis = 1)

junto_tratamiento = junto.merge(melted_df, how = 'left', left_on=['PlaylistID', 'segmento'], right_on = ['PlaylistID','segmento'])

tratamientos = junto_tratamiento.merge(codigos, how = 'left', left_on='codigo_tratamiento', right_on = 'Tratamiento_codigo' )

columnas_tratamiento = ['Audio','TIPO', 'Group_x', 'ID_leaf_x', 'PlaylistID', 'Tratamiento_significado'  ]
tratamientos_columnas_interes = tratamientos[columnas_tratamiento]

def obtener_parentesco (row):
    if type(row['Tratamiento_significado']) == float:
        return row['Tratamiento_significado']
    if '-' in row['Tratamiento_significado']:
        parentesco = row['Tratamiento_significado'].split('-')[0]
        return parentesco
    return row['Tratamiento_significado']

def obtener_asociacion(row):
    if type(row['Tratamiento_significado']) == float:
        return row['Tratamiento_significado']
    if '-' in row['Tratamiento_significado']:
        parentesco = row['Tratamiento_significado'].split('-')[1]
        parentesco = parentesco.split('_')[0]
        return parentesco
    return row['Tratamiento_significado']

def obtener_riesgo(row):
    if type(row['Tratamiento_significado']) == float:
        return row['Tratamiento_significado']
    if '_' not in row['Tratamiento_significado']:

        return np.nan

    if '_' in row['Tratamiento_significado']:
        parentesco = row['Tratamiento_significado'].split('_')[-1]
        if parentesco in '1234567':
            return np.nan
        return parentesco
    return row['Tratamiento_significado']

tratamientos_columnas_interes['parentesco'] = tratamientos_columnas_interes.apply(obtener_parentesco, axis = 1 )
tratamientos_columnas_interes['asociacion'] = tratamientos_columnas_interes.apply(obtener_asociacion, axis = 1 )
tratamientos_columnas_interes['riesgo'] = tratamientos_columnas_interes.apply(obtener_riesgo, axis = 1 )

tratamientos_columnas_interes.to_csv('tratamientos_columnas_interes.csv', sep = ';')


    






