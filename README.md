# Studying airflow with docker

### you need to do this:
- install requirements.txt
    - pip install -r requirements.txt

- install virtual env
    - python3 -m venv airflow_env

- write Module/secret.json
    - like this:
    - {
    -    "naver_api_id": "open api, you can change if you want. ref Module/Crawling.py",
    -    "naver_api_pw": "open api, you can change if you want. ref Module/Crawling.py",
    -    "postgresDB_password": "postgresql local db account pw",
    -    "host": ""postgresql local db host"",
    -    "dbname": ""postgresql local dbname that you're using"",
    -    "user": "postgresql user name",
    -    "port": "postgresql db port that you're using"
    - }

- teminal command execute
    - cd ../autotraining/airflow
          --> (maybe exists difference path of each user,
             the path '/autotraining/airflow' is important)
    - export AIRFLOW_HOME="$(pwd)"