# Projeto: Apache Spark com Delta Lake e Apache Iceberg

Este repositório apresenta um ambiente configurado com Apache Spark, Delta Lake e Apache Iceberg, utilizando PySpark e Jupyter Lab. O projeto foi desenvolvido como parte da disciplina de Engenharia de Dados.

## Participantes

- Anna Clara Teixeira de Medeiros - <https://github.com/annaclaratxm>
- Gabriel Antonin Pascoali - <https://github.com/lugialo>
- Vinicius Teixeira Colombo - <https://github.com/viniciuscolombo>

## Requisitos do Projeto

- Apache Spark
- Delta Lake
- Apache Iceberg
- Python 3.13
- UV

## 1. Clonando o repositório

```bash
git clone https://github.com/lugialo/eg-dados-apache-spark.git
cd eg-dados-apache-spark
```

## 2. Configurando o ambiente Python

No Linux, abrir o Terminal e executar o comando abaixo para instalar o UV (caso não tenha):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# caso você não tenha o curl instalado, pode usar o wget pra instalar também:
wget -qO- https://astral.sh/uv/install.sh | sh
```

## 3. Crie o ambiente com o venv

```bash
uv venv
```

## 4. Acesse o ambiente

```bash
source .venv/bin/activate
```

## 4. Instalando as dependencias

```bash
uv pip install .
```

## 5. Iniciando o Jupyter Lab

```bash
jupyter-lab
```

## 6. Estrutura do Projeto

```
eg-dados-apache-spark/
├── notebooks/
    ├── delta-lake.ipynb
    ├── pyspark-iceberg.ipynb
├── data/covid-19/vaccinations.csv
├── pyproject.toml
└── README.md
```

## 6. Fontes de dados

- [Dados públicos utilizados](https://raw.githubusercontent.com/owid/covid-19-data/refs/heads/master/public/data/vaccinations/vaccinations.csv)
- Estrutura das tabelas com modelo ER incluída nos notebooks.

## 7. Referências

- Canal [DataWay BR](https://www.youtube.com/@DataWayBR)
- Repositórios:
  - <https://github.com/jlsilva01/spark-delta>
  - <https://github.com/jlsilva01/spark-iceberg>
  - <https://github.com/owid/covid-19-data>
