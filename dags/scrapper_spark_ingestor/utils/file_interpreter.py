"""Extract_info from files."""
from pathlib import Path
from json import loads
from zipfile import ZipFile
import datetime


def _file_interpreter(row: str, tmp_dir: str):
    raw_data_dir = Path(tmp_dir)

    print('\n\nReading', row.get('title'), '...')
    file_name = raw_data_dir / row.get('url').split('/')[-1]
    if file_name.exists():
        with ZipFile(file_name, 'r') as zip:
            files = filter(
                lambda e: 'conjunto_de_datos/' in e.filename,
                zip.infolist()
            )

            files = map(
                lambda e: {
                    "file_name": e.filename,
                    "datetime": str(datetime.datetime(*e.date_time))
                },
                files
            )

            row['files'] = list(files)
            for final_file in row['files']:
                ff_data = zip.read(final_file.get('file_name'))

                if not raw_data_dir.exists():
                    raw_data_dir.mkdir()

                with open(raw_data_dir / final_file.get('file_name').split('/')[-1], 'wb') as ff_p:
                    ff_p.write(ff_data)

            print(file_name.name, 'OK.')
            return row
    print('No data found.')
