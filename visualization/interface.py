import streamlit as st
from etl.extraction import DataScraper

def app():

    # Streamlit interface
    st.title("Coleta de Dados de Imóveis")

    property_type = st.selectbox("Tipo do imóvel", ["apartamentos", "casas", "loft", "kitnet", "lote"])
    business_type = st.selectbox("Tipo do negócio", ["venda", "aluguel"])

    city = st.text_input("Cidade")
    state = st.text_input("Estado")

    # Main button to start data extraction
    if st.button("Coletar Dados"):
        st.write("Coletando dados, por favor aguarde...")
        with st.spinner("Coletando dados..."):

            ########### EXTRACT #######################################################
            # DataScraper class instance
            imovel_real = DataScraper(property_type, business_type, city, state)

            # Getting paserd data
            parsed_data = imovel_real.get_parsed_data()

            # Getting properties urls
            properties_urls = imovel_real.get_properties_urls(parsed_data)

            # Getting properties base data
            imovel_real.get_properties_base_data(properties_urls, st)

            # Getting properties dataframe
            properties_df = imovel_real.properties_df()
            st.success("Coleta de dados concluída!")
            st.dataframe(properties_df)

        filename = f"{property_type}_{business_type}_{city}.csv"
        with open(filename, encoding="utf-8") as f:
            st.download_button('Download CSV', f, file_name=filename)
