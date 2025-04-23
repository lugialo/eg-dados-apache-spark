# Apache Iceberg

## O que é o Apache Iceberg?

Apache Iceberg é uma camada de abstração para gerenciamento de tabelas de dados. Ele é usado principalmente em ambientes de grandes volumes de dados e proporciona melhor gerenciamento de versões e eficiência em operações como leitura, escrita, e atualização de dados.

## Configuração do Apache Iceberg no Apache Spark

Para configurar o Iceberg no Apache Spark, você precisa criar uma **SparkSession** com as configurações necessárias, como mostrado abaixo. Esse código inicializa o ambiente local para o desenvolvimento com Iceberg:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .appName("IcebergLocalDevelopment") \
  .config("spark.driver.host", "localhost") \
  .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1') \
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
  .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
  .config("spark.sql.catalog.local.type", "hadoop") \
  .config("spark.sql.catalog.local.warehouse", "spark-warehouse/iceberg") \
  .config("spark.sql.catalog.local.create-namespace", "true") \
  .config("spark.driver.bindAddress", "localhost") \
  .getOrCreate()
```

Esse código garante que você tenha configurado corretamente a sessão do Spark com Iceberg, incluindo a dependência do Iceberg e o catálogo de dados necessário.

## Criando Tabelas Iceberg

Aqui está o código para criar uma tabela com dados de vacinação diários em diferentes países:

```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.iceberg.covid_daily_vaccinations (
        date DATE,
        location STRING,
        daily_vaccinations DOUBLE,
        people_fully_vaccinated DOUBLE
    ) USING iceberg PARTITIONED BY (year(date));
""")
```

Essa tabela é particionada pelo ano da data, o que facilita consultas rápidas com base em períodos.

## Inserindo Dados na Tabela Iceberg

Após criar a tabela, podemos inserir dados nela. Aqui está um exemplo de inserção de dados sobre as vacinas diárias para diferentes locais:

```python
spark.sql("""
    INSERT INTO local.iceberg.covid_daily_vaccinations
    VALUES
        (DATE('2025-04-21'), 'Brazil', 20000.0, 15000000.0),
        (DATE('2025-04-20'), 'USA', 30000.0, 25000000.0),
        (DATE('2025-04-19'), 'India', 40000.0, 35000000.0)
""")
```

Isso insere os dados diretamente na tabela Iceberg.

## Atualizando Dados na Tabela Iceberg

Em vez de sobrescrever dados antigos, você pode realizar atualizações diretamente na tabela. O exemplo abaixo mostra como atualizar os dados de vacinação diária para o Brasil:

```python
spark.sql("""
    UPDATE local.iceberg.covid_daily_vaccinations
    SET daily_vaccinations = 50000.0
    WHERE location = 'Brazil'
    AND date = DATE('2025-04-21')
""")
```

Isso ajusta a quantidade de vacinas administradas no Brasil na data especificada.

## Consultando Dados em Iceberg

Para visualizar os dados após a inserção ou atualização, utilize o comando `SELECT`. Aqui está um exemplo de consulta para mostrar os dados do Brasil em 21 de abril de 2025:

```python
spark.sql("""
    SELECT * FROM local.iceberg.covid_daily_vaccinations 
    WHERE location = 'Brazil' AND date = DATE('2025-04-21')
""").show()
```

Isso exibe os dados atualizados de vacinação para o Brasil.

## Deletando Dados em Iceberg

O Iceberg também permite excluir dados de maneira eficiente. O exemplo abaixo mostra como deletar os dados de vacinação para o Brasil em 21 de abril de 2025:

```python
spark.sql("""
    DELETE FROM local.iceberg.covid_daily_vaccinations
    WHERE location = 'Brazil' AND date = DATE('2025-04-21')
""")
```

Este comando remove a linha específica da tabela.

## Carregando e Manipulando Dados Externos

Aqui está um exemplo de como carregar dados de um arquivo CSV externo, ajustar tipos de dados e adicionar à tabela Iceberg:

```python
from pyspark.sql.functions import to_date, col

df = spark.read.csv("data/covid-19/vaccinations.csv", header=True)

df = df.withColumn("date", to_date(df["date"], "yyyy-MM-dd"))
df = df.withColumn("daily_vaccinations", col("daily_vaccinations").cast("double"))
df = df.withColumn("people_fully_vaccinated", col("people_fully_vaccinated").cast("double"))

df.writeTo("local.iceberg.covid_daily_vaccinations").append()
```

Este código converte as colunas para o tipo adequado e insere os dados na tabela Iceberg.

## Conclusão

O Apache Iceberg é uma excelente solução para trabalhar com grandes volumes de dados e realizar operações como leitura, escrita, atualização e exclusão de dados de maneira eficiente. Ele é uma escolha ideal quando se trabalha com Spark e grandes conjuntos de dados em ambientes de produção.
