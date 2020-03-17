# spark-essentials

This is a followup code for the course Spark-Essentials from udemy.com by rockthejvm

Some of the important guidelines to run the code on your machine:

1. Download IDE of your choice with latest version of Scala and Spark
2. Download Docker.
3. Navigate to the current working directory and run following command your new terminal window
   - docker-compose up 
3. In a new terminal window navigate to same working directory and run command './psql.sh'
4. In a new terminal window, Navigate to folder spark-cluster and run command 
   - chmod +x build-images.sh (making executable for the first time)
   - ./build-images.sh (making executable for the first time)
   - docker-compose up --scale spark-worker=3


To stop postgres anytime execute following command
   - brew services stop postgres
