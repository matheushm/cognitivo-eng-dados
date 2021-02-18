# Teste de Engenharia de Dados - Matheus Oliveira
## Requisitos
1. Conversão do formato dos arquivos: Converter o arquivo CSV presente no diretório data/input/users/load.csv, para um formato colunar de alta performance de leitura de sua escolha. Justificar brevemente a escolha do formato;

2. Deduplicação dos dados convertidos: No conjunto de dados convertidos haverão múltiplas entradas para um mesmo registro, variando apenas os valores de alguns dos campos entre elas. Será necessário realizar um processo de deduplicação destes dados, a fim de apenas manter a última entrada de cada registro, usando como referência o id para identificação dos registros duplicados e a data de atualização (update_date) para definição do registro mais recente;

3. Conversão do tipo dos dados deduplicados: No diretório config haverá um arquivo JSON de configuração (types_mapping.json), contendo os nomes dos campos e os respectivos tipos desejados de output. Utilizando esse arquivo como input, realizar um processo de conversão dos tipos dos campos descritos, no conjunto de dados deduplicados;

## Justificativas

Nesse teste, o csv original sofreu as transformações propostas em /requirements/requirements.txt
e o resultado final foi exportado para .parquet (data/output/output.parquet)

A escolha do parquet se deve ao fato de ser um formato colunar de alta performance amplamente utilizado. Suas vantagens, no caso de um volume maior de dados, está na otimização das queries analíticas e redução do custo de armazenamento. As queries podem ler apenas as colunas utilizadas na consulta, o que gera resultados muito mais rápido, especialmente ao se tratar de Big Data.

Para realizar esse teste, resolvi utilizar um serviço Cloud da AWS. Utilizando o serviço AWS EMR, um cluster Spark foi criado e o processamento foi realizado através deste. Para que tudo estivesse na nuvem, os arquivos que foram copiados para esse repo também foram colocados em um bucket S3.

Além do core Hadoop/Spark, também adicionei o serviço de Jupyter no Cluster para que pudesse explorar os dados utilizando o PySpark e um notebook. Esse notebook exploratório foi a origem do Spark Job (src/spark_job.py) que foi submetido como um step nesse cluster EMR para ser executado.

O arquivo de bootstrap (bootstrap/bootstrap.sh) foi utilizado para instalar o boto3, utilizado para ler o json fornecido (config/types_mapping.json), pois também houve o upload deste no S3.
