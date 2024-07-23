1. Before running this code, I instantiated a mysql database instance using podman
2. Create a database bandham and a table PERSON manually in mysql.


Scripts:
1. pull docker image: `podman pull mysql:latest`
2. start mysql database: `podman run -d --name test-mysql -e MYSQL_ROOT_PASSWORD=strong_password -p 3306:3306 mysql`
3. Create table:
    `
    CREATE TABLE bandham.PERSON (
    ID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    NAME VARCHAR(255) NOT NULL,
    AGE INT NOT NULL
    );
    `


# Start Airflow in Ubuntu in Windows 11
1. launch Ubuntu
2. update and upgrade: `sudo apt update && sudo apt upgrade`
3. #Create virtual environment
    python3 -m venv airflow-env

4. #Activate the virtual environment
    source airflow-env/bin/activate
5. Set the $AIRFLOW_HOME Parameter
    nano ~/.bashrc

    #Type the following
    AIRFLOW_HOME=/c/Users/bandham/airflow

    #Press Ctrl+S and Ctrl+X to exit the editor

6. `pip install apache-airflow`
7. `airflow db init`
8. `airflow users create \
    --username admin \
    --password admin  \
    --firstname MANIKANTA \
    --lastname BANDHAM \
    --role Admin \
    --email bandhammanikanta@gmail.com`

9. Run the Web Server and Scheduler in seperate consoles.
    `airflow webserver --port 8080
    airflow scheduler`
10. The Airflow web server will be accessible at http://localhost:8080 in your web browser and log in using the above-created User.

Detailed steps: https://vivekjadhavr.medium.com/how-to-easily-install-apache-airflow-on-windows-6f041c9c80d2


# Find the mysql ip address: mysql running in podman
podman inspect <container_id_or_name> | findstr "IPAddress"

# ubuntu password: 7787
# ubuntu username: bandham

# install mysql client in ubuntu:
sudo apt install mysql-client

mysql -h 127.0.0.1 -P 3306 -u root -p


jar tf airflow/mysql-connector-j-8.2.0.jar | grep com/mysql/cj/jdbc/Driver.class
sudo apt-get install python3-pip
sudo apt install openjdk-11-jdk
sudo apt install telnet



Main files:
- dags/pyspark_mysql_dag.py
- pyspark_mysql_write.py
- log4j2.properites


# submit job:
- spark-submit --jars /home/bandham/airflow/mysql-connector-j-8.2.0.jar /home/bandham/airflow/pyspark_mysql_write.py