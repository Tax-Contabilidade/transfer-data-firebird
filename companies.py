import pandas as pd
from db import Connect
import requests

db = Connect()
error_debug = []


def reports_txt(name, obj):
    with open(f'{name}.txt', 'w') as f:
        for item in obj:
            f.write(f"{item}\n\n")


sql_companies = """
    select
        COALESCE(emp.CNPJBASE, emp.CPF) AS cnpj_cpf,
        emp.RAZAOSOCIAL as corporate_name,
        COALESCE(emp.NMFANTASIA, emp.RAZAOSOCIAL) as fantasy_name,
        COALESCE(emp.DTINIATIV, '') as start_of_activities,
        CASE
            WHEN emp.MEI = 1 THEN 'True' ELSE 'False'
        END AS is_mei,
        CASE
            WHEN emp.OPTANTESIMPLES = 'S' THEN 'True' ELSE 'False'
        END AS is_simple_optant,
        CASE
            WHEN emp.DESATIVADA = 0 THEN 'True' ELSE 'False'
        END AS is_activated
    from emp order by is_mei
"""

# sql_companies = """
#     SELECT
#     COALESCE(emp.CNPJBASE, emp.CPF) AS cnpj_cpf,
#     emp.RAZAOSOCIAL AS corporate_name,
#     COALESCE(emp.NMFANTASIA, emp.RAZAOSOCIAL) AS fantasy_name,
#     COALESCE(emp.DTINIATIV, '') AS start_of_activities,
#     CASE
#         WHEN emp.MEI = 1 THEN 'True' ELSE 'False'
#     END AS is_mei,
#     CASE
#         WHEN emp.OPTANTESIMPLES = 'S' THEN 'True' ELSE 'False'
#     END AS is_simple_optant,
#     CASE
#         WHEN emp.DESATIVADA = 0 THEN 'True' ELSE 'False'
#     END AS is_activated,
#     COALESCE(codigos_fortes, '') AS codigos_fortes
# FROM
#     emp
# LEFT JOIN
#     (
#         SELECT
#             COALESCE(emp.CNPJBASE, emp.CPF) AS cnpj_cpf,
#             LIST(emp.CODIGO, ' ') AS codigos_fortes
#         FROM
#             emp
#         GROUP BY
#             emp.CNPJBASE,
#             emp.CPF
#     ) AS fort
# ON
#     fort.cnpj_cpf = COALESCE(emp.CNPJBASE, emp.CPF)
# order by corporate_name;
# """

df = pd.read_sql(sql_companies, con=db.con)
df['IS_MEI'] = df['IS_MEI'].astype(str).str.strip()
df['IS_SIMPLE_OPTANT'] = df['IS_SIMPLE_OPTANT'].astype(str).str.strip()
df['IS_ACTIVATED'] = df['IS_ACTIVATED'].astype(str).str.strip()


for i, company in enumerate(df['CNPJ_CPF']):
    url = "http://localhost:8000/companies/companies/"
    headers = {'Content-Type': 'application/json'}
    values = {
        "cnpj_cpf": df.loc[i, 'CNPJ_CPF'],
        "corporate_name": df.loc[i, 'CORPORATE_NAME'],
        "fantasy_name": df.loc[i, 'FANTASY_NAME'],
        "start_of_activities": df.loc[i, 'START_OF_ACTIVITIES'],
        "is_mei": df.loc[i, 'IS_MEI'],
        "is_simple_optant": df.loc[i, 'IS_SIMPLE_OPTANT'],
        "is_activated": df.loc[i, 'IS_ACTIVATED'],
    }
    # print(values)
    response = requests.post(url, json=values, headers=headers)

    if response.status_code is not 201:
        error_debug.append(f"{response}\n{values}\n{response.text}")

    print(f"\n{response}\n{response.json()}")

reports_txt("erros_companies", error_debug)
