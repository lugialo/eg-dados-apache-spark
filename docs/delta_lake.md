# Delta Lake

## O que é o Delta Lake?

O Delta Lake é uma camada de armazenamento transacional para o Apache Spark que permite usar operações ACID (Atomicidade, Consistência, Isolamento e Durabilidade) em cima de arquivos de dados. Ele melhora o gerenciamento de dados, lidando com problemas como corrupção de dados, leitura consistente, e melhora o desempenho em grandes volumes de dados.

## Configuração do Delta Lake no Apache Spark

Para configurar o Delta Lake no Apache Spark, você pode criar uma **SparkSession** com as configurações específicas do Delta, como mostrado abaixo:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

from delta import *

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
```

Essa configuração permite usar o Delta Lake e suas funcionalidades em um ambiente PySpark local.

## Criando Tabelas Delta

Com a configuração do SparkSession pronta, podemos criar uma tabela Delta. Aqui está um exemplo de como criar uma tabela chamada `location_delta` com informações sobre localidades:

```python
spark.sql("""
    CREATE TABLE location_delta (location_id INT, country STRING, continent STRING, population DOUBLE) 
    USING delta
""")
```

### Inserindo Dados na Tabela Delta

Agora que a tabela foi criada, você pode inserir dados nela. Um exemplo de inserção seria:

```python
spark.sql("""
    INSERT INTO location_delta 
    VALUES (1, 'Afghanistan', 'Asia', 41450000)
""")
```

## Consultando Dados em Delta

Você pode consultar os dados de uma tabela Delta como faria com qualquer outra tabela no Spark. Aqui está um exemplo para visualizar os dados da tabela `location_delta`:

```python
spark.sql("SELECT * FROM location_delta").show()
```

### Exibindo o Histórico de Operações

Uma das características poderosas do Delta Lake é a capacidade de registrar e acessar o histórico de operações em uma tabela. Isso é feito através do comando `history()`, que nos mostra as operações feitas sobre a tabela, como criação, atualização e exclusão de dados.

```python
from delta.tables import DeltaTable

location = DeltaTable.forPath(spark, "./spark-warehouse/location_delta")
location.history().show()
```

### Atualizando Dados

O Delta Lake também permite atualizar os dados de maneira eficiente. Por exemplo, se quisermos atualizar a população de um determinado país, podemos executar a seguinte operação:

```python
spark.sql("""
    UPDATE location_delta 
    SET population = 43000000 
    WHERE location_id = 1
""")
```

### Deletando Dados

Deletar dados também é simples com Delta Lake. Para excluir um registro específico, como o local com `location_id` igual a 1, use o seguinte código:

```python
spark.sql("""
    DELETE FROM location_delta 
    WHERE location_id = 1
""")
```

### Adicionando Colunas

Você pode adicionar novas colunas a uma tabela Delta usando o comando `ALTER TABLE`:

```python
spark.sql("""
    ALTER TABLE location_delta ADD COLUMN people_vaccinated DOUBLE
""")
```

### Visualizando Dados Após Alterações

Após a inserção de uma nova coluna, podemos visualizar os dados atualizados:

```python
spark.sql("""
    SELECT * FROM location_delta
""").show()
```

### Histórico Detalhado das Operações

Você pode ver o histórico completo de todas as operações realizadas na tabela Delta, incluindo os detalhes sobre o tipo de operação e os parâmetros envolvidos:

```python
location.history().show(truncate=False)
```

## Conclusão

Delta Lake permite um gerenciamento eficiente e seguro dos dados em um ambiente distribuído como o Apache Spark. Com as operações ACID e a capacidade de rastrear o histórico das tabelas, ele é uma excelente escolha para sistemas de dados de larga escala.

Esses exemplos demonstram as operações básicas de manipulação de dados com Delta Lake e como utilizá-lo para garantir consistência, desempenho e integridade de dados.
