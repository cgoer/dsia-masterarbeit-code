# Code zur Masterarbeit

Dieses Repository enthält den Quellcode zur Masterarbeit "Intelligenz im Kollektiv - Generierung eines Datensatzes zur Sentiment-Analyse deutschsprachiger Nachrichtentexte", eingereicht in  [**Data Science & Intelligent Analytics**](https://www.fh-kufstein.ac.at/eng/Study/master/data-science-intelligent-analytics-pt) an der [FH Kufstein Tirol](https://www.fh-kufstein.ac.at/eng/). 

---
### Installationsanleitung

Der Betrieb der MongoDB erfordert Docker, es wird die Verwendung von Conda empfohlen.

**1. Ausfetzen der Conda-Umgebung**

Erstellen der Umgebung In der Kommandozeile:   
``conda create --name masterarbeit-goerner python=3.10 ``

Aktivierung der Umgebung:  
``conda activate masterarbeit-goerner``

Installation aller benötigter Packages:  
```pip install requirements.txt```

**2. Inbetriebnahme der Datenbank**

In der Kommandozeile im root-ordner:  
```docker-compose up -d```

Zum einspielen der Daten muss zunächst die Datei 
[masterthesis-goerner.zip](https://kufsteinfh-my.sharepoint.com/:u:/g/personal/1910837689_stud_fh-kufstein_ac_at/EfRHw4cgfJxOvp2sOh3SxbYBsyAwTWtI4z6t4uSgW_FKbQ?e=hCp860)
heruntergeladen und im Ordner
`masterthesis/db/backup/`
entpackt werden. Sollte die Datei nicht mehr verfügbar sein stelle ich diese gerne zur Verfügung.

Anschließend können die Daten mit folgendem Befehl in die Datenbank importiert werden:  
```sh init_db.sh``` oder ``bash init_db.sh``

---
### Struktur

```
.
├── README.md
├── docker-compose.yml
├── init_db.sh      
├── masterthesis
│   ├── __init__.py
│   ├── annotation-evaluation
│   │   ├── 3-4-4_analyse-testkampagne.ipynb
│   │   ├── 3-4-4_nachbearbeitung-annotationsergebnis.ipynb
│   │   ├── 3-5_Vergleichsannotation.ipynb
│   │   ├── 3-6_Exploration-annotierter-daten.ipynb
│   │   └── data
│   ├── db
│   │   ├── backup
│   │   ├── backup-and-restore.py
│   │   ├── data
│   │   ├── db-conf.txt
│   │   └── dbconnection.py
│   ├── preprocessing
│   │   ├── 3-4-2_Sampling.ipynb
│   │   ├── pre-sampling.py
│   │   └── preprocessing.py
│   ├── raw-data
│   │   ├── 3-2-3_raw-data-cleaning.ipynb
│   │   ├── 3-2-3_raw-data-eda.ipynb
│   │   ├── news-please-crawler.py
│   │   ├── thenewsapi-conf.txt
│   │   └── thenewsapi-crawler.py
│   └── utils.py
├── masterthesis.egg-info
│   ├── PKG-INFO
│   ├── SOURCES.txt
│   ├── dependency_links.txt
│   └── top_level.txt
├── requirements.txt
└── setup.py
```






