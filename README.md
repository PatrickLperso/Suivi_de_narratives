# WebScrapping
### Le Projet

Le projet consiste à développer un outil permettant de monitorer ce qu'il se dit à propos d'un sujet.
On peut ainsi rechercher le terme climat et voir à quel moment dans les médias la question du climat a été abordé, 
quels ont été les termes employés, qui en a parlé le plus, etc ...

Etant donné le caractère assez universel de cette capacité, il est possible de faire des requêtes sur absolument n'importe quel sujet.


L'application développée est en réalité plusieurs services fonctionnant dans des dockers.
Cette option permet une conteneurisation absolue et une gestion des dépendances.

### Lancer le projet

```bash
docker-compose -f docker-compose.prod.yml up -d --build
``` 
L'ensemble des installations va être automatiquement lancé. Enfin, le scrapper ainsi que l'ensemble des services seront lancés.


### Définir le nombre de cycles de crawling 

Le nombre de cycle de crawling est actuellment à 10 mais il peut-être modifié dans le fichier docker-compose.prod.yml.
Il est possible de mettre 0 si on souhaite uniquement regarder les logs grafana ainsi que le dashboard.
```yaml
  scrapper:
    build:
      context: ./scrappers
      dockerfile: Dockerfile
    container_name: scrapper
    command : [ "python", "crawling_async.py", "mongodb", "10" , "--crawling_robots=0"]
    networks:
      scrapper:
```
Attention, si le nombre de cycles de scrapping définit est important, il faudra penser à indexer la base de données Mongodb pour pouvoir faire des requêtes dans le dashboard. Enfin, le scrapper pourra prendre du temps avant de finir l'ensemble de ces cycles.

### DashBoard & Grafana

Grafana est accessible pour monitorer le scrapper sur l'addresse :  http://localhost:3000
<img src="images/grafana.png" width="700"/>

Un DashBoard est disponible sur l'addresse : http://localhost:8000/
<img src="images/dashboard.png" width="700"/>

### Choix d'architecture

Les données scrappées sont stockées dans une base de données MongoDB. Pour être plus précis, l'ensemble des données est stockée dans un volume docker.
Ces données ne sont donc pas perdues si la machine s'éteint car elles sont stockées sur la machine. C'est aussi le cas pour les données concerant Prometheus et grafana.
Il est donc possible d'arrêter le scrapping et le reprendre plus tard.
Enfin, une api flask, prometheus et grafana sont déployés pour monitorer le scrapper.

### Fonctionnement du scrapper

Le scrapper scanne un liste de médias américains en allant regarder ce qui se trouve dans chaque robots.txt.
Or dans ces fichiers se trouvent régulièrement des urls qui sont les index du site (des urls conteant sitemap).
Notre scrapper va récursivement récupérées ces sitemaps et trouver les liens html contenus dans ces fichiers.
Toutes ces données sont ensuite mises dans la base de donnée MongoDB.

### Exemple avec lemonde.fr
Nous allons montrer le fonctionnement du crawler avec le site lemonde.fr.
Note : Il n'a pas été crawler.

Le robots.txt se trouve à l'addresse : https://www.lemonde.fr/robots.txt__
Il contient des sitemaps :
```xml
Sitemap: https://www.lemonde.fr/sitemap_news.xml
Sitemap: https://www.lemonde.fr/sitemap_index.xml
Sitemap: https://www.lemonde.fr/en/sitemap_news.xml
Sitemap: https://www.lemonde.fr/en/sitemap_index.xml
```

On y découvre en poursuivant le parcours des sitemaps les articles du journal LeMonde :
```xml
<url>
    <loc>
    https://www.lemonde.fr/international/article/2024/01/25/guerre-en-ukraine-questions-apres-le-crash-d-un-avion-russe_6212827_3210.html
    </loc>
    <lastmod>2024-01-25T04:30:07+01:00</lastmod>
    <news:news>
        <news:publication_date>2024-01-25T04:30:07+01:00</news:publication_date>
        <news:title>
        Guerre en Ukraine : questions après le crash d’un avion russe
        </news:title>
        <news:publication>
        <news:name>Le Monde</news:name>
        <news:language>fr</news:language>
        </news:publication>
    </news:news>
    <image:image>
        <image:loc>
            https://img.lemde.fr/2024/01/24/422/0/5105/2552/1440/720/60/0/55d3ce5_2024-01-24t161100z-883304909-rc2do5a7czzq-rtrmadp-3-ukraine-crisis-belgorod-airplane.JPG
        </image:loc>
        <image:caption>
            Des policiers russes montent la garde sur la route menant au site du crash de l’avion de transport militaire russe IL-76, à Yablonov, dans la région de Belgorod (Russie), le 24 janvier 2024.
        </image:caption>
    </image:image>
</url>
```

Les données de date de publication, de l'url sont récupérées, les mots dans l'url sont parsés.
Il en résulte de données enregistrées sous ce format dans une collection de la base de données mongoDB.
```json
{
    "url": "https://www.lemonde.fr/international/article/2024/01/25/guerre-en-ukraine-questions-apres-le-crash-d-un-avion-russe_6212827_3210.html",
    "mots_in_url" : ["international","2024", "01", "25", "guerre", "ukraine", "questions", "apres", "crash", "avion", "russe", "6212827", "3210"], 
    "media_name" : "lemonde.fr",
    "id_media" : 1245,
    "has_been_scrapped" : true,
    "xml_source" : "https://www.lemonde.fr/sitemap_news.xml",
    "date": "2024-01-25T04:30:07+01:00",
    "text": null,
}
```
Note : Le parsing des Urls sous cette forme permet de faire des index et donc des requêtes très rapides.
Des tests très concluants avec 20 millions de données ont été efectués.
Ces indexes ne sont pas encore automatiquement crées. 


Une deuxième collection est mise à jour en parralèle pour rajouter les nouveaux sitemaps et mettre à jour ceux qui ont été scrappés.
Les informations contenues dans le robot.txt y sont aussi stockées.
Les données sont sous cette forme : 
```json
{
    "site_web_url": "https://www.lemonde.fr/", "media_name": "lemonde.fr" , "media_coverage" :  "" ,  "media_diffusion" : "" , 
    "media_location" : "France" , "coverage" : "national" , "true_country" : "FRance", 
    "sitemaps_xml": [ 
                        {"url":"xml1",  "has_been_scrapped" : true ,"is_responding": true,  "parent_xml" : "robots.txt", "depth":0},
                        {"url":"xml2", "has_been_scrapped" : true ,"is_responding": true, "parent_xml" : "xml1", "depth":1}, 
                        {"url":"xml3", "has_been_scrapped" : false ,"is_responding": true, "parent_xml" : "xml1", "depth":1}, 
                        {"url":"xml4", "has_been_scrapped" : true ,"is_responding": true, "parent_xml" : "xml2", "depth":2}, 
                        {"url":"xml5", "has_been_scrapped" : false ,"is_responding": false, "parent_xml" : "xml2", "depth":2}, 
                    ],
    "robots_txt_parsed" :  {"Disallow": ["/synsearch/", "..." ], "Allow": []} ,
    "last_time_scrapped" : "",
    "is_responding" : true,
    "robots_txt": "..."
}
```

### Liste des fichiers

/api_flask contient l'api flask utile pour prometheus
/dash_app contient le dashboard de DataViz
/grafana contient les fichiers de configuation pour grafana 
/scrappers contient les scrappers

### Commandes utiles

Arrêter les services
```bash
docker-compose -f docker-compose.prod.yml down
```

Voir l'état des services
```bash
docker-compose -f docker-compose.prod.yml ps
```

Voir les logs du scrapper
```bash
docker-compose -f docker-compose.prod.yml logs scrapper --follow
```