# Extract_IBOV  
  
📊 Pipeline de Dados do IBOVESPA   
[site da extração](https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br)
<br>

✨ Arquitetura  
O nosso pipeline funciona em um fluxo contínuo e automatizado:

🐍 Extração <br>
Um script em Python faz a coleta dos dados diretamente do site da B3 .

       ▼

⚙️ Orquestração <br>
O GitHub Actions é acionado automaticamente, pegando o arquivo gerado e o enviando para a nuvem da AWS e salva em formato .parquet.

       ▼

📥 Dados Brutos no S3 <br>
O arquivo .parquet é armazenado na pasta raw/ de um Bucket S3.

       ▼

⚡️ Gatilho S3 com Lambda <br>
A chegada do novo arquivo no S3 dispara uma função AWS Lambda.

       ▼

🧑‍🍳 ETL com Glue <br>
A função Lambda inicia um Job no AWS Glue que limpa, transforma e enriquece os dados brutos.

       ▼

✅ Dados Refinados no S3 <br>
O Glue salva os dados já processados na pasta refined/ do mesmo Bucket S3 e cria uma table no default do Athena.


       ▼

🔍 Análise com Athena <br>
Os dados refinados são catalogados e podem ser consultados instantaneamente com SQL usando o Amazon Athena.

<br>
