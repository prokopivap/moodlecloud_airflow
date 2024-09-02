Hi.

A couple of words about my dag:
1. For instalation Airflow I used docker (actually I used "astro" for full installation what in adition include a lot of different containers)
   I did it in such way because in parallel I wathed video from udemy ("Master Apache Airflow from A to Z. Hands-on videos on Airflow with AWS, Kubernetes, Docker and more" provided my Marc Lambert),
   and I did it in the way how it was proposed in this video.

2. Data base I have created in Postgres, but the table I have created directly from Airflow.

3. I have issues with copy file in to the docker as I do not know in what container I should copy this file, so I have copied the file into all containers
   using command ($docker cp C:/localMachineSourceFolder/someFile.txt containerId:/containerDestinationFolder) and then I was able to manipulate with this file
   but I have not possibility to copy it back (from docker to local machine) due to permissions issues. I am going to rise this question on QA session.

4.  All the transformations were comoleted succesfuly and data unloaded into postgres.

5. All the validations have been passed success.

6. I have doubts about Branching and wrting result into log file (I do not know how to add into branching two validation tasks and write exceptions into file from both validation tasks).
   So I add branching only from one validation task.
