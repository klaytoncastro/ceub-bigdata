import streamlit as st
import matplotlib.pyplot as plt
import numpy as np

st.set_page_config(page_title="Aplicação Completa", layout="wide")

# Definindo o Menu Lateral
st.sidebar.title("Navegação")
pagina = st.sidebar.selectbox(
    "Ir para:",
    ["Início", "Gráfico", "Componentes"]
)

# Definindo Página Inicial
if pagina == "Início":
    st.title("Aplicação Streamlit Completa")
    st.header("Bem-vindo")
    st.write("Aplicação demonstrando os principais componentes e um gráfico.")

# Definindo um Gráfico
elif pagina == "Gráfico":
    st.title("Gráfico de Histograma")

    # Gerando dados
    data = np.random.randn(1000)

    # Criando figura
    fig, ax = plt.subplots()
    ax.hist(data, bins=20, color="steelblue", edgecolor="black")

    # Exibindo no Streamlit
    st.pyplot(fig)

# Explorando possibilidades de uso de componentes
elif pagina == "Componentes":
    st.title("Demonstração de Componentes")

    texto = st.text_input("Textbox")
    opcao = st.selectbox("Selecione uma opção", ["Opção 1", "Opção 2", "Opção 3"])
    check = st.checkbox("Ativar recurso")
    numero = st.number_input("Valor numérico", 0, 100, 10)
    slider = st.slider("Ajuste um nível", 0, 50, 25)

    if st.button("Enviar"):
        st.success(
            f"Texto: {texto} | Opção: {opcao} | Check: {check} | "
            f"Número: {numero} | Slider: {slider}"
        )
