# Instalação do Ambiente

Para configurar o ambiente necessário para este projeto, siga os passos abaixo:

## Pré-requisitos

- Python 3.13
- UV (gerenciador de pacotes)

## Passo a Passo

1. Clone o repositório:

   ```bash
   git clone https://github.com/lugialo/eg-dados-apache-spark.git
   cd eg-dados-apache-spark
   ```

2. Instale o UV:

   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

   Ou, se preferir instalar via pip:

   ```bash
   pip install uv
   ```

3. Inicialize o projeto com o UV:

   ```bash
   uv init
   ```

4. Adicione as dependências necessárias:

   ```bash
   uv add pyspark delta-spark iceberg-spark
   ```

5. Crie e ative o ambiente virtual:

   ```bash
   uv venv
   source .venv/bin/activate  # No Windows: .venv\Scripts\activate
   ```

6. Inicie o Jupyter Labs:

   ```bash
   jupyter-lab
   ```

Agora, você pode executar os notebooks `delta-lake.ipynb` e `pyspark-iceberg.ipynb` para testar as implementações.
