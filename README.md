# WebScrapping
## Ceci est le repos du projet de WebScrapping 


### Liste des fichiers

20231212.export.CSV contient le fichier 2023 de GDELT 1.0, il a été utilisé pour construire un BDD de médias (code dans get_url_list.py)

Analysis_links.ipynb est un notebook pour analyser les robots.txt et sitemap récupérées des donénes GDELT

DataViz_Medias_scrapping.ipynb est un notebook de DataViz des données scrappées du site https://www.abyznewslinks.com/allco.htm

Medias_scapping.py pemet le scrapping de https://www.abyznewslinks.com/allco.htm

get_url.list.py contient les fonctions de scan asynchrones d'URL 

medias_per_countries.csv contient les données scrappées de https://www.abyznewslinks.com/allco.htm

url_500_11.json contient les robots.txt, et sitemap.xml des données GDELT

### venv pour crawling_async.py

Créer un venv, activation , et installation des paquets pip. Le requirement est commun au .py et .ipynb
```bash 
python3 -m venv venv
source env/bin/activate
pip install -r requirements_async_crawl.txt
```


### BDD MongoDB 

Crée un docker volume
```bash
docker volume create MongoDB_scrapping_volume
```

Lance le conteneur docker 
```bash
docker run -d -p 27017:27017 -v MongoDB_scrapping_volume:/data/db --name Mongodb_scrapping mongo:latest
```
Cette commande lance une image mongo en mode détache (-d) avec un forwarding du port 27017 sur le locahost (-p 27017:27017)
Le conteneur porte un nom en l'occurence Mongodb_scrapping

Pour arrêter et supprimer le conteneur Mongodb_scrapping en cours d'éxecution, le docker volume est persistant
```bash
docker ps | grep Mongodb_scrapping | awk '{print $1}' | xargs docker stop | xargs docker rm
```



### Executer code
Si le docker Mongo a été lancée corrrecement et que les installations ont été faites dans le venv
Il n'y a plus qu'à lancer l'importation des données dans la BDD MongoDB 

Pour le premier run, il faudra scrapper les robots.txt 
```python
if __name__=="__main__":

    n_cycles=50  #nombre de cycles de crawling
    crawling_robots=True #initlisation avec le crawling des robots.txt
```

Ensuite, ce ne sera plus nécessaire, la variable crawling_robots sera mise à False
```python
if __name__=="__main__":

    n_cycles=50  #nombre de cycles de crawling
    crawling_robots=False #initlisation avec le crawling des robots.txt
```

Lancer le scraper 
```bash
python crawling_async.py
```


