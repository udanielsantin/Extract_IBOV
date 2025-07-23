from playwright.sync_api import sync_playwright
import pandas as pd
from io import BytesIO
from datetime import datetime
import time


def main():
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
    filename = f"testes/ibov_{hoje.strftime('%Y%m%d')}.parquet"

    df.to_parquet(filename, engine="fastparquet")


if __name__ == "__main__":
    main()
