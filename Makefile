run: 
	@docker-compose -f docker-compose.prod.yml up -d 

down: 
	@docker-compose -f docker-compose.prod.yml down

mongo_localhost: 
	@docker run -d -p 27017:27017 -v webscrapping_mongodb-data:/data/db --name mongo_test mongo:latest

del_mongo_localhost: 
	@docker ps | grep mongo_test | awk '{print $$1}' | xargs docker stop  | xargs docker rm

logs_scrapper: 
	@docker-compose -f docker-compose.prod.yml logs scrapper --follow