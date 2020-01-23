import utils.data_download as dw
from datetime import datetime
import utils.data_setup as ds
import numpy as np
import datetime

key= ['USD', 'GBP', 'CAD', 'JPY']

# date = ['2020-01-02','2020-01-03']
# print(date[0])
# for i,fr in enumerate(date):
#     a=dw.ECB_rates_extractor(key, date[i])
#     print(a)
# b=np.array(a['OBS_VALUE'])
# print(b)

s = ds.ECB_setup (key,'2020-01-01', '2020-01-04' )
print(s)

# date = '2020-01-02'
# f= datetime.datetime.strptime(date, '%Y-%m-%d') - datetime.timedelta(days = 1)
# print(f)
# s= f.strftime('%Y-%m-%d')
# print(s)

riga = Dataframe['time'][1578009600]

############ AGGIUNGERE CHECK SE NON TROVA LA DATA 

def substitute_finder(broken_array, reference_array, where_to_lookup, position):

    # find the elements of ref array not included in broken array (the one to check)
    missing_item = Diff(reference_array, broken_array)

    # setting position as column name
    header = ['Time', 'Close Price', "Crypto Volume", "Pair Volume"]
    position = header[position]

    weighted_variations = [] #####se sono zero tutti? se più di uno è zero? deve completare al 100% dei casi #####
    volumes = []
    for element in missing_item:

        if element != None:

            today_value = float(where_to_lookup[where_to_lookup['Time'] == element][position])
            yesterday_value = float(where_to_lookup[where_to_lookup['Time'] == element - 86400][position])
            variation = (today_value - yesterday_value) / yesterday_value
            volume = float(where_to_lookup[where_to_lookup['Time'] == element]['Pair Volume'])
            weighted_variation = variation * volume
            weighted_variations.append(weighted_variation)
            volumes.append(volume)
        else:
            weighted_variations.append(0) 
            volumes.append(0)

    volumes = np.array(volumes)
    weighted_variations = np.array(weighted_variations)
    variation_matrix = np.column_stack((missing_item, weighted_variations))
    volume_matrix = np.column_stack((missing_item, volumes))

    return variation_matrix, volume_matrix