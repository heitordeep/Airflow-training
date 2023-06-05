# Airflow-training

# Description:
This project is only for study. The goal is to learn how to create a pipeline template in Yaml. <br>
I used government data that can be found at the link: [dados.gov.br](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj)<br>
It's possible to create a pipeline with **TaskGroup**, dividing it into two steps. I created a pipeline with two environments: dev and prod to use airflow's **taskGroup** method. 

# Commands and airflow access:
- Container image:
  - ```shell
    $ docker compose up -d
    ```
  - Image version: 2.5  
- Credentials:
  - User: airflow
  - Password: airflow 
- Access:
  - Link: http://0.0.0.0:8080/  

# Yaml file structure:
```yaml
PIPELINE:
  ENV:
    DEV:    
      PIPELINE_NAME: DADOS_DEV
      INPUT_PATH: /opt/airflow/dev/raw
      OUTPUT_PATH: /opt/airflow/dev/trusted
      INPUT_FILE_NAME: gov.csv
      OUTPUT_FILE_NAME: gov_dev_{today}.csv
    PRD:    
      PIPELINE_NAME: DADOS_PRD
      INPUT_PATH: /opt/airflow/prd/raw
      OUTPUT_PATH: /opt/airflow/prd/trusted
      INPUT_FILE_NAME: gov.csv
      OUTPUT_FILE_NAME: gov_prd_{today}.csv
```
These paths are created in the docker-compose file
```yaml
volumes:
  - ./dags:/opt/airflow/dags
  - ./logs:/opt/airflow/logs
  - ./dev/raw:/opt/airflow/dev/raw
  - ./dev/trusted:/opt/airflow/dev/trusted
  - ./prd/raw:/opt/airflow/prd/raw
  - ./prd/trusted:/opt/airflow/prd/trusted
  - ./configs:/opt/airflow/configs
```


# Pipeline result:
![image](https://github.com/heitordeep/Airflow-training/assets/17969551/b5fbd936-8f54-4197-9535-26aff0c01aa7)
