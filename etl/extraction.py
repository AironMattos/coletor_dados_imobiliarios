import cloudscraper
from bs4 import BeautifulSoup as Bs
import pandas as pd
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import os


class DataScraper:
    def __init__(self, property_type, business_type, city, uf):
        # Metódo construtor com url e header base
        self.base_url = "https://www.imovelweb.com.br"
        self.scraper = cloudscraper.create_scraper(browser={'custom': 'ScraperBot/1.0',})
        self.property_type = property_type
        self.business_type = business_type
        self.city = city
        self.uf = uf

    def fix_json(self, string_json):
        import json

        if string_json.endswith("}"):
            pass
        else:
            string_json += '"'

        chaves_abertas = string_json.count("{")
        chaves_fechadas = string_json.count("}")
        diferença = chaves_abertas - chaves_fechadas

        if diferença > 0:
            string_json += "}" * diferença

        string_json = json.loads(string_json)

        return string_json

    def format_publication_date(self, text):
        import datetime

        days = int(text.split(" ")[-2])
        publication_date = datetime.datetime.now() - datetime.timedelta(days=days)
        return publication_date.strftime("%Y-%m-%d")

    def page_limit(self):

        url = f"{self.base_url}/{self.property_type}-{self.business_type}-{self.city}-{self.uf}.html"
        req = self.scraper.get(url)
        soup_object = Bs(req.content, 'html.parser')
        soup_object = soup_object.find('h1', {'class': 'sc-1oqs0ed-0 guPmPw'}).text
        page_limit = soup_object.split(" ")[0].replace(".", "")
        page_limit = int(page_limit) // 20

        return int(page_limit)

    def fetch_page(self, page_number):
        if page_number == 1:
            url = f"{self.base_url}/{self.property_type}-{self.business_type}-{self.city}-{self.uf}.html"
        else:
            url = f"{self.base_url}/{self.property_type}-{self.business_type}-{self.city}-{self.uf}-pagina-{page_number}.html"
        req = self.scraper.get(url)
        return Bs(req.content, 'html.parser')

    def get_parsed_data(self):

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_results = [executor.submit(self.fetch_page, page_number) for page_number in range(1, self.page_limit())]
            soup_objects = [future.result() for future in future_results]

        return soup_objects

    def get_properties_urls(self, soup_objects):
        properties_urls = []

        for soup_object in soup_objects:
            boxes = soup_object.find_all("div", attrs={"data-to-posting": True})

            for box in boxes:
                properties_urls.append(f"{self.base_url}{box['data-to-posting']}")

        return properties_urls

    def get_property_data(self, property_url):

        property_data = {
            'negocio': '',
            'tipo_imovel': '',
            'municipio': '',
            'anunciante': '',
            'contato_anunciante': '',
            'preco': '',
            'endereco': '',
            'area': '',
            'quartos': '',
            'banheiros': '',
            'vagas': '',
            'data_publicacao': '',
            'link': ''
        }

        req = self.scraper.get(property_url)

        parsed_data = Bs(req.content, 'html.parser')

        # ===========================================================================

        # Getting business type
        property_data['negocio'] = self.business_type

        # Getting property type
        property_data['tipo_imovel'] = self.property_type

        # Getting city
        property_data['municipio'] = self.city

        # Getting property url
        property_data['link'] = property_url

        try:
            # Getting publisher
            js_text = parsed_data.find_all('script', {'type': 'text/javascript'})

            publisher_data = js_text[2].text

            pattern = re.compile(r'const POSTING = .*')

            result = re.search(pattern, publisher_data)

            raw_data = result.group(0).split("=")
            raw_data = raw_data[1].strip()

            final_data = self.fix_json(raw_data)

            publisher_url = f"{self.base_url}{final_data['publisher']['url']}"
            req = self.scraper.get(publisher_url)

            publisher_parsed_data = Bs(req.content, 'html.parser')

            property_data['anunciante'] = publisher_parsed_data.find('h1', {
                'class': 'sc-dzd39i-10 ePXjml'}).text.strip()
            property_data['contato_anunciante'] = publisher_parsed_data.find('span', {
                'class': 'sc-dzd39i-8 fIIMAS'}).text.strip()

            # Getting publish date
            property_data['data_publicacao'] = self.format_publication_date(final_data['antiquity'])

            # Getting prices
            price = parsed_data.find('div', {'class': 'price-items'}).text
            price = price.replace('R$', '').strip()
            property_data['preco'] = price

            # Getting address
            address = parsed_data.find('h2', {'class': 'title-location'}).text.strip()
            address = " ".join(address.split())
            address = address.rsplit(',', 1)[0]
            property_data['endereco'] = address

            # Living spaces and area variable
            icon_feature = parsed_data.find_all("li", class_="icon-feature")

            # Getting area
            property_data['area'] = int(icon_feature[1].text.strip().split("\n")[0])

            # Getting Bathrooms
            property_data['banheiros'] = icon_feature[2].text.strip().split("\n")[0]

            # Getting parking spaces
            property_data['vagas'] = icon_feature[3].text.strip().split("\n")[0]

            # Getting bedrooms
            property_data['quartos'] = icon_feature[4].text.strip().split("\n")[0]

        except:
            pass

        return property_data

    def get_properties_base_data(self, properties_urls, st):
        filename = f"{self.property_type}_{self.business_type}_{self.city}.csv"
        header = ['negocio', 'tipo_imovel', 'municipio', 'anunciante', 'contato_anunciante', 'preco', 'endereco',
                  'area', 'quartos', 'banheiros', 'vagas', 'data_publicacao', 'link']

        with concurrent.futures.ThreadPoolExecutor() as executor, open(filename, 'w', newline='',
                                                                       encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()

            files = [f for f in os.listdir('.') if os.path.isfile(f)]
            for file in files:
                if file.endswith('.csv') and file != filename:
                    os.remove(file)

            future_to_url = {executor.submit(self.get_property_data, property_url): property_url for property_url in
                             properties_urls}

            bar = st.progress(0)
            status_text = st.empty()

            for i, future in enumerate(as_completed(future_to_url)):
                property_data = future.result()
                writer.writerow(property_data)

                # Atualiza a barra de progresso
                bar.progress(i / len(properties_urls))
                status_text.write(f"Imóveis processados  {i + 1} de {len(properties_urls)}")
                time.sleep(0.1)

    def properties_df(self):
        # Método para transformar uma lista de dicionários em um dataframe pandas
        filename = f"{self.property_type}_{self.business_type}_{self.city}.csv"
        df = pd.read_csv(filename)
        df = df.dropna()
        df = df.reset_index(drop=True)

        df['preco'] = df['preco'].apply(lambda x: float(str(x).replace(".", "")))
        df['area'] = df['area'].astype(int)
        df['quartos'] = df['quartos'].astype(int)
        df['banheiros'] = df['banheiros'].astype(int)
        df['vagas'] = df['vagas'].astype(int)

        # Save dataframe as
        df.to_csv(filename, index=False)

        return df
