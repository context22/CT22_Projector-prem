import pandas as pd
import pdb

df_conv = pd.read_csv('convdata.csv')

print (df_conv)

field_list_conv = df_conv.columns

print (field_list_conv)

breakpoint()

for i in range(len(field_list_conv)):
    print (field_list_conv[i])
    # print (df_conv[field_list_conv[i]])


