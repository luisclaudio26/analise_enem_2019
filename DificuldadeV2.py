import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

class GlobalParameters:
    def __init__(self, n_chunks = 10, chunksize = 5000):
        self.n_chunks = n_chunks
        self.chunksize = chunksize

        self.total_estudantes = 0
        self.total_regular = 0
        self.total_reaplicacao = 0

class Prova:
    def __init__(self, respostas, gabarito, codigos, reaplicacao, adaptadas, offset, nome):
        self.col_respostas = respostas
        self.col_gabarito = gabarito
        self.col_codigo = codigos
        self.cod_reaplicacao = reaplicacao
        self.cod_adaptadas = adaptadas
        self.offset = offset
        self.nome = nome

#################################################
######## CONSTANTES E VARIÁVEIS GLOBAIS #########
#################################################

GP = GlobalParameters()
N_ITENS = 45
COL_LINGUA = "TP_LINGUA"
PATH = "DADOS/MICRODADOS_ENEM_2019.csv"
PATH_ITENS = "DADOS/ITENS_PROVA_2019.csv"
COLUMNS = ["TX_RESPOSTAS_MT", "TX_GABARITO_MT", "CO_PROVA_MT", 
           "TX_RESPOSTAS_CN", "TX_GABARITO_CN", "CO_PROVA_CN", 
           "TX_RESPOSTAS_CH", "TX_GABARITO_CH", "CO_PROVA_CH", 
           "TX_RESPOSTAS_LC", "TX_GABARITO_LC", "CO_PROVA_LC", COL_LINGUA] 

PROVAS_LC = [511, 512, 513, 514, 521, 525, 551, 552, 553, 554, 565]

EXAM_LC = Prova("TX_RESPOSTAS_LC", "TX_GABARITO_LC", "CO_PROVA_LC", [551, 552, 553, 554, 565], [521, 525, 565], 0*N_ITENS, "LC")
EXAM_CH = Prova("TX_RESPOSTAS_CH", "TX_GABARITO_CH", "CO_PROVA_CH", [547, 548, 549, 550, 564], [520, 524, 564], 1*N_ITENS, "CH")

# (codigo, posicao, lingua) -> cod_item
CPL_PARA_ITEM = dict()
ITEM_PARA_CPL = dict()
ITEM_PARA_N_ACERTOS = dict() # TODO não faz sentido isso ser global, porque
                             #      não é a mesma coisa para toda run

##################################
######### PROCEDIMENTOS ##########
##################################
def construir_mapas():
    itens = pd.read_csv(PATH_ITENS, sep = ';')

    for linha in itens.iterrows():
        dados_item = linha[1]

        item = dados_item["CO_ITEM"]
        ITEM_PARA_N_ACERTOS[item] = 0

        # como NaN != NaN, ele não pode ser usado para compor a chave, daí a gente troca por -1
        codigo, pos, ling_ = dados_item["CO_PROVA"], dados_item["CO_POSICAO"], dados_item["TP_LINGUA"]
        ling = -1 if ling_ != ling_ else ling_
        
        CPL_PARA_ITEM[codigo,pos, ling] = item

        if not item in ITEM_PARA_CPL:
            ITEM_PARA_CPL[item] = []
        ITEM_PARA_CPL[item].append( (codigo,pos,ling) )

def acumular_acertos(correcao, codigo, lingua, offset, GP):
    
    for pos in range(0, len(correcao)):
        if correcao[pos]:

            # Essa informação é extremamente "hardcoded" pro formato do ENEM atual;
            # se as questões de língua estrangeira não forem as cinco primeiras das
            # provas de Linguagens, aí não funciona
            ling = -1
            if pos < 5 and codigo in PROVAS_LC:
                ling = lingua

            ITEM_PARA_N_ACERTOS[CPL_PARA_ITEM[codigo, offset+pos+1, ling]] += 1

def processa_aluno(aluno, prova, codigo, GP):

    respostas, gabarito = aluno[prova.col_respostas], aluno[prova.col_gabarito]
    correcao = np.full(N_ITENS, fill_value = False)

    pos, n_acertos = 0, 0
    for (r, g) in zip(respostas, gabarito):

        # 9 significa que o item não existe -> só é aplicável na prova de linguagens,
        # que tem 50 itens dos quais 5 não são aplicáveis (inglês ou espanhol)
        if r == '9':
            continue

        correcao[pos] = (r == g)
        if correcao[pos]:
            n_acertos += 1

        pos += 1

    return correcao, aluno["TP_LINGUA"], n_acertos

def ler_provas(prova, GP):

    contador_chunk = 0
    for chunk in pd.read_csv(PATH, sep=';', chunksize=GP.chunksize, usecols = COLUMNS):
        
        # descarte as linhas que não têm respostas do aluno
        chunk_limpo = chunk.dropna(axis = 0, subset = [prova.col_respostas])
        
        cod_provas = chunk_limpo[prova.col_codigo].astype(np.int32).unique()
        for cod_prova in cod_provas:
            dados_prova = chunk_limpo[chunk_limpo[prova.col_codigo] == cod_prova]

            for aluno in dados_prova[[prova.col_respostas, prova.col_gabarito, "TP_LINGUA"]].iterrows():
                correcao, lingua, n_acertos = processa_aluno(aluno[1], prova, cod_prova, GP)

                acumular_acertos(correcao, cod_prova, lingua, prova.offset, GP)

                # TODO continuar daqui: contar alunos
                if cod_prova in prova.cod_reaplicacao:
                    GP.total_reaplicacao += 1
                else:
                    GP.total_regular += 1

        GP.total_estudantes += chunk_limpo.shape[0]
        contador_chunk += 1

        print("\r{0} blocos lidos ({1} provas - {2} regulares, {3} reaplicação)"
                    .format(contador_chunk, GP.total_estudantes, GP.total_regular, GP.total_reaplicacao), end = '')

        # if contador_chunk >= GP.n_chunks:
        #     break

#############################
######### EXECUÇÃO ##########
#############################
PROVA = EXAM_CH

construir_mapas()
ler_provas(PROVA, GP)

# TODO MOVER TUDO ISSO ABAIXO PRA UMA FUNÇÃO SÓ
# Agrupar itens por código
COD_PARA_ITENS = dict()
for idx in ITEM_PARA_CPL:
    for cpl in ITEM_PARA_CPL[idx]:
        cod, pos, ling = cpl

        if not cod in COD_PARA_ITENS:
            COD_PARA_ITENS[cod] = []
        COD_PARA_ITENS[cod].append( (idx, pos, ling) )

itens = pd.read_csv(PATH_ITENS, sep = ';')

CODIGOS_PROVA = itens[itens["SG_AREA"] == PROVA.nome]["CO_PROVA"].unique()
DF_COD_COR = itens[["CO_PROVA", "TX_COR"]].drop_duplicates()

# constroi a tabela pra prova regular e pra reaplicação
dict_df_regular = dict()
dict_df_reaplic = dict()
for cod in COD_PARA_ITENS:
    if not cod in CODIGOS_PROVA:
        continue

    # informação bem hardcoded; só dá pra saber a diferença entre
    # uma prova de reaplicação e uma da regular olhando o dicionário dos dados
    out = dict_df_regular
    if cod in PROVA.cod_reaplicacao:
        out = dict_df_reaplic

    cor = DF_COD_COR[DF_COD_COR["CO_PROVA"] == cod].iloc[0, 1]

    # estes dois debaixo precisam sempre ser percorridos na 
    # mesma ordem para a tabela sair correta no final!
    if not 'CO_ITEM' in out:
        out['CO_ITEM'] = []
    
        for item in COD_PARA_ITENS[cod]:
            idx, pos, ling = item
            out["CO_ITEM"].append(idx)

    out[cor] = []
    for item in COD_PARA_ITENS[cod]:
        idx, pos, ling = item
        out[cor].append(pos)

# finaliza construção da tabela adicionando a coluna de Língua e Acertos
for dict_df in [dict_df_regular, dict_df_reaplic]:
    prova = itens[itens['CO_ITEM'].isin(dict_df['CO_ITEM'])][['CO_ITEM', 'TP_LINGUA']].drop_duplicates()

    item_para_lingua = dict()
    for (idx, item) in prova.iterrows():
        co_item, lingua = int(item['CO_ITEM']), item['TP_LINGUA']
        
        if lingua == 0:
            item_para_lingua[co_item] = "Inglês"
        elif lingua == 1:
            item_para_lingua[co_item] = "Espanhol"
        else:
            item_para_lingua[co_item] = np.nan

    dict_df['TP_LINGUA'] = []
    dict_df['n_acertos'] = []
    for idx in dict_df['CO_ITEM']:
        dict_df['TP_LINGUA'].append(item_para_lingua[idx])
        dict_df['n_acertos'].append(ITEM_PARA_N_ACERTOS[idx])

df_regular = pd.DataFrame(dict_df_regular).sort_values(by = 'n_acertos')
df_regular.to_excel("acertos_{0}_regular_2019.xlsx".format(PROVA.nome), sheet_name = PROVA.nome)

df_reaplic = pd.DataFrame(dict_df_reaplic).sort_values(by = 'n_acertos')
df_reaplic.to_excel("acertos_{0}_reaplicacao_2019.xlsx".format(PROVA.nome), sheet_name = PROVA.nome)

print("\nProvas regulares: {0}; reaplicação: {1}".format(GP.total_regular, GP.total_reaplicacao))