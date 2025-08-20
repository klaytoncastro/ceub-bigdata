#!/bin/bash

# Verifica se o arquivo fernet_key.txt foi gerado corretamente
if [ ! -f "fernet_key.txt" ]; then
    echo "Erro: Chave Fernet não foi gerada corretamente!"
    exit 1
fi

# Lê a chave Fernet do arquivo fernet_key.txt e remove qualquer quebra de linha
FERNET_KEY=$(cat fernet_key.txt | tr -d '\n')

# Substitui a string #CHAVEFERNET pela chave Fernet no docker-compose.yml
#sed -i "s|#CHAVEFERNET|AIRFLOW__CORE__FERNET_KEY=${FERNET_KEY}|g" docker-compose.yml

# Substitui a string #CHAVEFERNET pela chave Fernet no docker-compose.yml, com o hífen no início
sed -i "s|#CHAVEFERNET|- AIRFLOW__CORE__FERNET_KEY=${FERNET_KEY}|g" docker-compose.yml

echo "Chave Fernet inserida com sucesso no arquivo docker-compose.yml."
