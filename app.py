from playwright.sync_api import sync_playwright
import pandas as pd
import boto3
from io import BytesIO
from datetime import datetime
import time


def scrape_ibov() -> pd.DataFrame:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        page = browser.new_page()
        page.goto(
            "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
        )
        page.select_option("#selectPage", "120")

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

        browser.close()

    df = pd.DataFrame(
        data, columns=["codigo", "acao", "tipo", "qtd_teorica", "participacao_perc"]
    )
    
    df["qtd_teorica"] = df["qtd_teorica"].str.replace('.', '', regex=False).astype('int64')
    df["participacao_perc"] = df["participacao_perc"].str.replace(',', '.', regex=False).astype('float64')
    
    df["data"] = pd.to_datetime(datetime.now()).floor('ms') 
    df["data"] = df["data"].astype("datetime64[ms]")

    return df


def upload_parquet_to_s3(df: pd.DataFrame, bucket_name: str, s3_path: str) -> None:
    buffer = BytesIO()
    df.to_parquet(buffer, engine="fastparquet", index=False)
    buffer.seek(0)
    s3 = boto3.client("s3", region_name="sa-east-1")
    s3.upload_fileobj(buffer, bucket_name, s3_path)


def main():
    df = scrape_ibov()
    
    hoje = datetime.now()
    s3_path = f"raw/year={hoje:%Y}/month={hoje:%m}/day={hoje:%d}/ibov.parquet"
    bucket_name = "daily-ibovespa-bucket"
    upload_parquet_to_s3(df, bucket_name, s3_path)


if __name__ == "__main__":
    main()
