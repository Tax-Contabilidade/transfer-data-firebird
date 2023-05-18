import pandas as pd
import requests
from db import Connect


def reports_txt(name, obj):
    with open(f'{name}.txt', 'w') as f:
        for item in obj:
            f.write(f"{item}\n\n")


def cod_fortes_is_unique(cod: str):
    cod_array = cod.split(" ")
    if len(cod_array) == 1:
        return True

    return False


manual_cod_fortes = []
error_debug = []
db = Connect()
sql = """
    SELECT COALESCE(emp.CNPJBASE, emp.CPF) as cnpj_cpf, LIST(emp.CODIGO, ' ') AS codigos_fortes
    FROM emp
    GROUP BY emp.CNPJBASE, emp.CPF;
"""

df = pd.read_sql(sql, db.con)
# df['CODIGOS_FORTES'] = df['CODIGOS_FORTES'].astype(str)
# df = df.drop_duplicates(subset=['CNPJ_CPF'])
# df = df.reset_index(drop=True)

for i, cnpj in enumerate(df['CNPJ_CPF']):
    cod_fortes = df.loc[i, 'CODIGOS_FORTES']
    if cod_fortes_is_unique(cod_fortes):
        url = "http://localhost:8000/companies/cod/"
        values = {
            "cpf_cnpj": cnpj,
            "cod_pessoal": cod_fortes,
            "cod_fiscal": cod_fortes,
            "cod_contabio": cod_fortes,
        }
        response = requests.post(url, json=values)

        if response.status_code != 201:
            error_debug.append(
                f"{response}\n{values}\n{response.text}"
            )

        print(f"\n{response}\n{response.json()}")
    else:
        manual_cod_fortes.append(
            f"{cnpj} contém mais de um cod_fortes [{cod_fortes}]"
        )

reports_txt("manual_cod_fortes", manual_cod_fortes)
reports_txt("erros_cod_fortes", error_debug)
