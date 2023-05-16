from db import Connect
import pandas as pd
import requests
import numpy as np
import time

db = Connect()
error_list = []

sql = """
select 
    (select COALESCE(emp.CNPJBASE, emp.CPF) from emp where emp.CODIGO = est.EMP_CODIGO) as cnpj_cpf,
    est.SEQCNPJ as sequential,
    est.ENDLOGRADOURO as street_address,
    est.ENDNUMERO as number_address,
    est.BAIRRO as neighborhood_address,
    est.CEP,
    est.MUN_UFD_SIGLA as uf,
    est.MUN_CODIGO as city_code,
    est.IE as state_registration,
    est.MUN_CODIGO as municipal_registration,
    CASE
        WHEN est.MATRIZ = '1' THEN 'True' ELSE 'False'
    END as is_headquarters,
    est.DTINIATIV as start_of_activities
from EST
"""

df = pd.read_sql(sql, con=db.con)

df['IS_HEADQUARTERS'] = df['IS_HEADQUARTERS'].astype(bool).astype(str)
df['START_OF_ACTIVITIES'] = df['START_OF_ACTIVITIES'].astype(str)
df = df.replace('NaT', None)

for i, cnpj in enumerate(df['CNPJ_CPF']):
    url = "http://192.168.1.54:8089/companies/establishments/"
    values = {
        'cnpj_cpf': df.loc[i, 'CNPJ_CPF'],
        'sequential': df.loc[i, 'SEQUENTIAL'],
        'street_address': df.loc[i, 'STREET_ADDRESS'],
        'number_address': df.loc[i, 'NUMBER_ADDRESS'],
        'neighborhood_address': df.loc[i, 'NEIGHBORHOOD_ADDRESS'],
        'cep': df.loc[i, 'CEP'],
        'uf': df.loc[i, 'UF'],
        'city_code': df.loc[i, 'CITY_CODE'],
        'state_registration': df.loc[i, 'STATE_REGISTRATION'],
        'municipal_registration': df.loc[i, 'MUNICIPAL_REGISTRATION'],
        'is_headquarters': df.loc[i, 'IS_HEADQUARTERS'],
        'start_of_activities': df.loc[i, 'START_OF_ACTIVITIES'],
    }
    # print(values)
    response = requests.post(url, json=values)
    if response.status_code != 201:
        error_list.append(
            f"{response}\n{values}\n{response.text}"
        )

    print(f"{response}\n{values}\n")

with open("errors_est.txt", "w") as f:
    for reg in error_list:
        f.write(f"{reg}\n\n")

print(df.head())
print(df.info())
