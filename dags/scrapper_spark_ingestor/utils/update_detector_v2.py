import requests
import json
from re import compile
from datetime import datetime

re_path = compile(r'[a-zA-Z\|]+DENUE\|Actividad económica\|([^\|]+)$')

url = "https://www.inegi.org.mx/app/api/descarga/descarga/descargamasiva/lista/obtenerarchivos"
base_donwload_path = "https://www.inegi.org.mx/contenidos"

payload = json.dumps({
    "tinfo": "6",
    "ag": "0",
    "prog": "0",
    "cc": "0",
    "subtema": "0",
    "anio": "0",
    "formato": "0",
    "datosAbiertos": "3",
    "titulo": "T3Ryb3N8REVOVUV8QWN0aXZpZGFkIGVjb27Ds21pY2F8",
    "textoBuscar": "",
    "ingles": "0",
    "tipoInfo": "OTROS"
})

headers = {
    'Content-Type': 'application/json',
    'Cookie': 'NSC_MC_bqjt=ffffffff09da7a5845525d5f4f58455e445a4a423660'
}


def _scrap(*args, **kwargs):
    response = requests.request("POST", url, headers=headers, data=payload)

    data = filter(
        lambda e: re_path.match(e.get('titulo')) and \
            'csv' in e.get('formatos'),
        response.json()
    )

    data = map(
        lambda e: {
            "title": re_path.match(e.get('titulo')).groups()[0],
            "date": e.get('anioInfo'),
            "url": base_donwload_path + e.get('pathLogico') + '_csv.zip'
        },
        data
    )

    json_data = json.dumps(list(data))
    file_name = '/tmp/extraction_' + datetime.today().strftime("__%Y_%m_%d") + '.json'

    with open(file_name, 'wt') as fl:
        fl.write(json_data)

    print('File saved successfully.')
    return file_name