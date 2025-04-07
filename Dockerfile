FROM openjdk:11-jre-slim

# Copier le jar compilé dans le conteneur
COPY ./target/scala-project-1.3.3-jar-with-dependencies.jar /app/scala-app.jar

# Créer un dossier pour les données dans le conteneur
RUN mkdir /data

# Définir les variables d’environnement
ENV MASTER_URL="local[*]"
ENV SRC_PATH="/data/credits.csv"
ENV DST_PATH="/data/Outputs"
ENV REPORTS="report1,report2,report3"

# Lancer l’application avec les variables (on les passe à la commande java)
ENTRYPOINT /bin/sh -c "java -jar /app/scala-app.jar ${MASTER_URL} ${SRC_PATH} ${DST_PATH} ${REPORTS}"
