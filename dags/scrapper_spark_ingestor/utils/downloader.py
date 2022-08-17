"""Download all the files."""
from pathlib import Path
from json import loads
from requests import get


def _download(row: dict, data_dir):
    data_dir = Path(data_dir)

    if not data_dir.exists():
        data_dir.mkdir(exist_ok=True)

    print('Downloading', row.get('title'), '...')
    file_name = row.get('url').split('/')[-1]
    data = get(row.get('url'))
    with open(data_dir / file_name, 'wb') as fl:
        fl.write(data.content)
