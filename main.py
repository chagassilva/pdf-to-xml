import io
import pdfplumber
import pandas as pd
from fastapi import FastAPI, UploadFile, File
from typing import List
from fastapi.responses import HTMLResponse

app = FastAPI()

@app.get("/")
def home():
    return {"status": "Super API de Estoque Rodando 100%"}

@app.post("/processar-estoque-lote")
async def processar_lote(files: List[UploadFile] = File(...)):
    matriz_dados = []

    # 1. Loop por todos os arquivos enviados
    for file in files:
        if not file.filename.endswith(".pdf"):
            continue
            
        # Lê o arquivo direto na memória RAM (muito mais rápido e seguro no Docker)
        conteudo_pdf = await file.read()
        
        with pdfplumber.open(io.BytesIO(conteudo_pdf)) as pdf:
            # Identifica a Filial no topo da primeira página
            primeira_pagina = pdf.pages[0].extract_text()
            nome_filial = "Desconhecida"
            if primeira_pagina:
                for linha_texto in primeira_pagina.split('\n'):
                    if "Filial:" in linha_texto:
                        nome_filial = linha_texto.split("Filial:")[1].strip()
                        break
            
            # Extrai os dados das tabelas
            for pagina in pdf.pages:
                tabela = pagina.extract_table()
                if not tabela: continue
                
                for linha in tabela:
                    c = [str(i).strip() if i else "" for i in linha]
                    if not c: continue
                    
                    codigo = c[0].split('\n')[0]
                    if codigo.isdigit() and len(c) >= 6:
                        try:
                            produto = c[1].replace('\n', ' ').strip()
                            estoque = float(c[3].replace('.', '').replace(',', '.'))
                            
                            matriz_dados.append({
                                "Código": codigo,
                                "Produto": produto,
                                "Filial": nome_filial,
                                "Qtd": estoque
                            })
                        except ValueError:
                            continue

    # 2. Se não encontrou nada, avisa
    if not matriz_dados:
        return {"status": "erro", "mensagem": "Nenhum dado de estoque válido encontrado nos PDFs."}

    # 3. A Mágica do Pandas: Monta o Dashboard Consolidado
    df_base = pd.DataFrame(matriz_dados)
    
    dashboard = df_base.pivot_table(
        index=['Código', 'Produto'], 
        columns='Filial', 
        values='Qtd', 
        aggfunc='sum', 
        fill_value=0
    ).reset_index()
    
    # 4. Totalizador Final
    colunas_filiais = dashboard.columns[2:]
    dashboard['TOTAL GERAL'] = dashboard[colunas_filiais].sum(axis=1)
    dashboard = dashboard.sort_values(by='TOTAL GERAL', ascending=False)

    # 5. Prepara as saídas: JSON estruturado e HTML para visualização rápida
    json_dados = dashboard.to_dict(orient="records")
    html_tabela = dashboard.to_html(index=False, classes='table table-bordered table-striped', border=0)
    
    html_completo = f"""
    <div style="font-family: Arial, sans-serif; padding: 20px;">
        <h2>Dashboard de Estoque Consolidado</h2>
        <p><strong>Arquivos processados:</strong> {len(files)}</p>
        <p><strong>Total de itens únicos:</strong> {len(dashboard)}</p>
        <hr>
        {html_tabela}
    </div>
    """

    # Retorna o pacote completo para o n8n
    return {
        "status": "sucesso",
        "arquivos_lidos": len(files),
        "total_produtos_unicos": len(dashboard),
        "html_pronto": html_completo,
        "dados_brutos": json_dados
    }