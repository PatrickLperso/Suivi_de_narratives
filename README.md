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

Créer un venv, activation , et installation des paquets pip
```bash 
python3 -m venv venv
source env/bin/activate
pip install -r requirements_async_crawl.txt
```


### BDD MongoDB 

Pour récupérer et lancer une instance docker. Lancez la commande suivante (elle est amenée à changer ), parce qu'il n'y a pas 
pour l'instant de presistance locale des données à l'aide d'un volume docker 
```bash
docker run -d -p 27017:27017 --name Mongodb_scrapping mongo:latest
```
Cette commande lance une image mongo en mode détache (-d) avec un forwarding du port 27017 sur le locahost (-p 27017:27017)
Le conteneur porte un nom en l'occurence Mongodb_scrapping

Pour voir les conteneurs en cours d'éxecution:
```bash 
docker ps
```
Pour arrêter et supprimer le conteneur Mongodb_scrapping en cours d'éxecution
```bash
docker ps | grep Mongodb_scrapping | awk '{print $1}' | xargs docker stop | xargs docker rm
```

Le script crawling_async.py va automatiquement insérer les médias anglais dans la BDD MongoDB dans le conteneur, s'il n'y a pas de données

