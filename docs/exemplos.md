# Exemplos de Código

## Exemplo 1: Criando uma Tabela Delta

Para criar uma tabela Delta, podemos usar o comando SQL. O exemplo abaixo cria uma tabela chamada `location_delta`, com colunas para `location_id`, `country`, `continent`, e `population`.

```python
spark.sql("""
    CREATE TABLE location_delta (location_id INT, country STRING, continent STRING, population DOUBLE) 
    USING delta
""")
```

## Exemplo 2: Inserindo Dados na Tabela Delta

Uma vez que a tabela foi criada, podemos inserir dados nela. O exemplo abaixo insere informações sobre localidades na tabela `location_delta`:

```python
spark.sql("""
    INSERT INTO location_delta 
    VALUES (1, 'Afghanistan', 'Asia', 41450000)
""")
```

## Exemplo 3: Atualizando Dados em Tabela Delta

Para atualizar dados em uma tabela Delta, podemos usar uma consulta `UPDATE`. O exemplo abaixo altera o nome de um país na tabela `location_delta`.

```python
spark.sql("""
    UPDATE delta.`/path/to/delta_table`
    SET name = 'Jane'
    WHERE id = 1
""")
```

## Exemplo 4: Deletando Dados em Tabela Delta

O comando `DELETE` pode ser usado para excluir dados da tabela. O exemplo abaixo exclui um registro específico da tabela `location_delta`.

```python
spark.sql("""
    DELETE FROM delta.`/path/to/delta_table` WHERE id = 2
""")
```

## Exemplo 5: Criando uma Tabela Iceberg

A criação de tabelas no Iceberg também é realizada através de SQL. O exemplo abaixo cria uma tabela chamada `covid_daily_vaccinations`, que armazena dados sobre vacinação diária.

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

## Exemplo 6: Inserindo Dados na Tabela Iceberg

Depois de criar a tabela, podemos inserir dados nela. O exemplo abaixo insere informações sobre vacinação diária para diferentes países.

```python
spark.sql("""
    INSERT INTO local.iceberg.covid_daily_vaccinations
    VALUES
        (DATE('2025-04-21'), 'Brazil', 20000.0, 15000000.0),
        (DATE('2025-04-20'), 'USA', 30000.0, 25000000.0),
        (DATE('2025-04-19'), 'India', 40000.0, 35000000.0)
""")
```

## Exemplo 7: Atualizando Dados em Tabela Iceberg

Da mesma forma que com Delta, também podemos atualizar dados em uma tabela Iceberg. O exemplo abaixo atualiza a quantidade de vacinas diárias para o Brasil.

```python
spark.sql("""
    UPDATE local.iceberg.covid_daily_vaccinations
    SET daily_vaccinations = 50000.0
    WHERE location = 'Brazil'
    AND date = DATE('2025-04-21')
""")
```

## Exemplo 8: Deletando Dados em Tabela Iceberg

Se for necessário excluir um registro da tabela, podemos usar o comando `DELETE`. O exemplo abaixo deleta a linha de vacinação para o Brasil em 21 de abril de 2025.

```python
spark.sql("""
    DELETE FROM local.iceberg.covid_daily_vaccinations
    WHERE location = 'Brazil' AND date = DATE('2025-04-21')
""")
```

## Exemplo 9: Carregando Dados de Arquivo CSV para Iceberg

Você pode carregar dados externos, como um arquivo CSV, para uma tabela Iceberg, convertendo os tipos de dados conforme necessário. O exemplo abaixo lê um arquivo CSV de vacinação e carrega os dados na tabela `covid_daily_vaccinations`.

```python
from pyspark.sql.functions import to_date, col

df = spark.read.csv("data/covid-19/vaccinations.csv", header=True)

df = df.withColumn("date", to_date(df["date"], "yyyy-MM-dd"))
df = df.withColumn("daily_vaccinations", col("daily_vaccinations").cast("double"))
df = df.withColumn("people_fully_vaccinated", col("people_fully_vaccinated").cast("double"))

df.writeTo("local.iceberg.covid_daily_vaccinations").append()
```

## Exemplo 10: Consultando Dados de Tabelas Iceberg

Aqui está como consultar os dados de uma tabela Iceberg. O exemplo abaixo mostra as vacinas diárias para o Brasil em 21 de abril de 2025:

```python
spark.sql("""
    SELECT * FROM local.iceberg.covid_daily_vaccinations 
    WHERE location = 'Brazil' AND date = DATE('2025-04-21')
""").show()
```

## Conclusão

Esses exemplos ilustram as operações básicas de manipulação de dados tanto no **Delta Lake** quanto no **Apache Iceberg**, desde a criação de tabelas até operações de leitura, escrita, atualização e exclusão. O uso do Spark com essas camadas de armazenamento transacional garante que você possa manipular grandes volumes de dados de maneira eficiente e com integridade.
