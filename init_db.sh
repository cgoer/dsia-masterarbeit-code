#/!bin/bash

python3 masterthesis/db/backup-and-restore.py --mode=restore --source=annotationresults_2022-09-29_034444.json
python3 masterthesis/db/backup-and-restore.py --mode=restore --source=newspleasedata_2022-09-29_034444.json
python3 masterthesis/db/backup-and-restore.py --mode=restore --source=newspleaseerrors_2022-09-29_034444.json
python3 masterthesis/db/backup-and-restore.py --mode=restore --source=preprocessed_2022-09-29_034444.json
python3 masterthesis/db/backup-and-restore.py --mode=restore --source=presampled_2022-09-29_034444.json
python3 masterthesis/db/backup-and-restore.py --mode=restore --source=rawdata_2022-09-29_034444.json
python3 masterthesis/db/backup-and-restore.py --mode=restore --source=referenceannotation_2022-09-29_034444.json
python3 masterthesis/db/backup-and-restore.py --mode=restore --source=sample_2022-09-29_034444.json
python3 masterthesis/db/backup-and-restore.py --mode=restore --source=thenewsapidata_2022-09-29_034444.json
