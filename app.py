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

    return pd.DataFrame(
        data, columns=["Código", "Ação", "Tipo", "Qtde Teórica", "Part (%)"]
    )


def upload_parquet_to_s3(df: pd.DataFrame, bucket_name: str, s3_path: str) -> None:
    buffer = BytesIO()
    df.to_parquet(buffer, engine="fastparquet")
    buffer.seek(0)
    s3 = boto3.client("s3", region_name="sa-east-1")
    s3.upload_fileobj(buffer, bucket_name, s3_path)


def main():
    df = scrape_ibov()

    hoje = datetime.now()
    s3_path = (
        f"raw/ano={hoje:%Y}/mes={hoje:%m}/dia={hoje:%d}/ibov_{hoje:%Y%m%d}.parquet"
    )
    bucket_name = "daily-ibovespa-bucket"
    upload_parquet_to_s3(df, bucket_name, s3_path)


if __name__ == "__main__":
    main()
