# AirFlowBasics
Useful - https://www.youtube.com/playlist?list=PLYizQ5FvN6pvIOcOd6dFZu3lQqc6zBGp2
https://marclamberti.com/blog/airflow-dag-creating-your-first-dag-in-5-minutes/
https://www.astronomer.io/guides/airflow-ui  

Establish docker image

1) docker pull puckel/docker-airflow

2) docker run -d -p 8080:8080 -v /path/to/dags/on/your/local/machine/:/usr/local/airflow/dags  puckel/docker-airflow webserver

3) open http://localhost:8080/admin/
