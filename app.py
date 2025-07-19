from playwright.sync_api import sync_playwright
import pandas as pd
import boto3
from io import BytesIO
from datetime import datetime
import time


def lambda_handler(event, context):
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        page = browser.new_page()
        page.goto(
            "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
        )

        page.select_option("#selectPage", "120")

        rows = []
        for _ in range(15):
            rows = page.query_selector_all("table tbody tr")
            if len(rows) == 120:
                break
            time.sleep(1)

        data = []
        for row in rows:
            cols = row.query_selector_all("td")
            cleaned = [col.inner_text().strip() for col in cols]
            if len(cleaned) > 5:
                tipo = " ".join(cleaned[2:-2])
                linha = [cleaned[0], cleaned[1], tipo, cleaned[-2], cleaned[-1]]
            else:
                linha = cleaned
            data.append(linha)

        df = pd.DataFrame(
            data, columns=["Código", "Ação", "Tipo", "Qtde Teórica", "Part (%)"]
        )
        browser.close()

    hoje = datetime.now()
    ano = hoje.strftime("%Y")
    mes = hoje.strftime("%m")
    dia = hoje.strftime("%d")

    filename = f"ibov_{ano}{mes}{dia}.parquet"
    s3_path = f"ano={ano}/mes={mes}/dia={dia}/{filename}"

    buffer = BytesIO()
    df.to_parquet(buffer, engine="pyarrow")
    buffer.seek(0)

    s3 = boto3.client("s3")
    bucket_name = "daily-ibovespa-bucket"
    s3.upload_fileobj(buffer, bucket_name, s3_path)

    print(f"Arquivo enviado para s3://{bucket_name}/{s3_path}")

    return {
        "statusCode": 200,
        "body": f"Arquivo enviado para s3://{bucket_name}/{s3_path}",
    }
