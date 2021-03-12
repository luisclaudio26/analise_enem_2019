import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

class GlobalParameters:
    def __init__(self, n_chunks = 40, chunksize = 5000):
        self.count = 0
        self.n_chunks = n_chunks
        self.chunksize = chunksize
        self.total_non_nan = 0

        self.total_regular = 0
        self.total_reaplicacao = 0
GP = GlobalParameters()

N_ITENS = 45

# o prefixo do código da Prova diz se ela foi de aplicação regular, 
# reaplicação [dado tirado do dicionário dos microdados]
# TODO é necessário um dicionariozinho desse pra cada prova
pref_regular = [50, 51, 52]
pref_reaplicacao = [54, 55]
adaptadas = [519, 523, 522, 526] # isso tem que ser extraído a mão mesmo

def prefixo_prova(codigo):
    return codigo // 10

# Columns where Answers, Groundtruth and Exam code are located, and offset for CO_POSICAO
EXAM_MT = ("TX_RESPOSTAS_MT", "TX_GABARITO_MT", "CO_PROVA_MT", 3*N_ITENS)
EXAM_CN = ("TX_RESPOSTAS_CN", "TX_GABARITO_CN", "CO_PROVA_CN", 2*N_ITENS)
EXAM_CH = ("TX_RESPOSTAS_CH", "TX_GABARITO_CH", "CO_PROVA_CH", 1*N_ITENS)
EXAM_LC = ("TX_RESPOSTAS_LC", "TX_GABARITO_LC", "CO_PROVA_LC", 0, "TP_LINGUA")

columns = list(EXAM_CN[0:3]) + list(EXAM_MT[0:3]) + list(EXAM_CH[0:3]) + list(EXAM_LC[0:3]) + ["TP_LINGUA"]

# TODO não vai funcionar com EXAM_LC por causa das 5 
#      primeiras linhas que podem ser espanhol ou inglês
itens_provas = pd.read_csv("./DADOS/ITENS_PROVA_2019.csv", sep = ';')

codepos_to_idx = dict()
for row in itens_provas[['CO_ITEM', 'CO_PROVA', 'CO_POSICAO']].iterrows():
    item, exam, pos = row[1]['CO_ITEM'], row[1]['CO_PROVA'], row[1]['CO_POSICAO']
    codepos_to_idx[(exam,pos)] = item

code_to_color = dict()
for row in itens_provas[['CO_PROVA', 'TX_COR']].iterrows():
    code, color = row[1]['CO_PROVA'], row[1]['TX_COR']
    code_to_color[code] = color

#####
class RightAnswerRate:
    def __init__(self, GP, exam):
        self.col_answers = exam[0]
        self.col_gt = exam[1]
        self.exam_code = exam[2]
        self.offset = exam[3]
        self.right_answers = dict()

    def process_chunk(self, chunk, GP):

        # retorna uma Series com o vetor de respostas na primeira coluna e o codigo do exame na segunda
        def check_answers(row):
            answers = [g == r for (g,r) in zip(row[self.col_answers], row[self.col_gt])]
            return pd.Series([answers, row[self.exam_code]], index = ['answers', 'exam_code'])

        def conta_acertos(row):
            idx = 1
            answers, code = row['answers'], row['exam_code']
            
            # TODO mudar o nome da função pra deixar claro 
            #      que a gente tá fazendo isso
            if prefixo_prova(code) in pref_regular:
                GP.total_regular += 1
            else:
                GP.total_reaplicacao += 1

            for i in answers:
                if i:
                    item_idx = codepos_to_idx[(code,idx+self.offset)]

                    # TODO duas consultas aqui. Prealocar todos os IDX deve tornar isso mais rápido
                    if not (item_idx in self.right_answers):
                        self.right_answers[item_idx] = 0
                    self.right_answers[item_idx] += 1

                idx += 1

        check = chunk.loc[:, [self.col_answers,self.col_gt,self.exam_code]].apply(check_answers, axis = 1, result_type = 'expand')
        check.apply(conta_acertos, axis = 1)

    def output_result(self, GP):
        idx_to_codepos = dict()
        for (codepos, idx) in codepos_to_idx.items():
            if idx in idx_to_codepos:
                idx_to_codepos[idx].append(codepos)
            else:
                idx_to_codepos[idx] = [(codepos)]

        # TODO create output dataframe with less hardcoded values
        dict_data_out = dict()
        dict_data_out["n_acertos"] = []
        dict_data_out["Azul"] = []
        dict_data_out["Amarela"] = []
        dict_data_out["Cinza"] = []
        dict_data_out["Rosa"] = []
        dict_data_out["Branca"] = []
        # dict_data_out["indice"] = []
        dict_data_out["n_candidatos"] = []
        dict_data_out["taxa_acerto"] = []

        # exams orange and green are adapted versions of the gray one
        skip_colors = ["Laranja", "Verde"]

        for (idx, count) in self.right_answers.items():
            list_codepos = idx_to_codepos[idx]

            # intermediate storage, which will guarantee that all rows
            # are completely filled (even if with -1 values)
            color_to_pos = {"Azul" : 0, "Amarela" : 0, "Cinza" : 0, "Rosa" : 0, "Branca" : 0}

            reaplicacao = False
            for codepos in list_codepos:
                code, pos = codepos
                
                # *Em tese*, se um código é de reaplicação, todos os outros devem ser também
                if prefixo_prova(code) in pref_reaplicacao:
                    reaplicacao = True

                color = code_to_color[code]
                if color in skip_colors:
                    continue
                color_to_pos[color] = pos

            # TODO o correto é haver um mapeamento das questões adaptadas 
            #      pras questões regulares, de forma a não perder dados

            for key in color_to_pos:
                dict_data_out[key].append(color_to_pos[key])

            n_candidatos = GP.total_reaplicacao if reaplicacao else GP.total_regular
            dict_data_out["n_acertos"].append(count)
            dict_data_out["n_candidatos"].append(n_candidatos)
            dict_data_out["taxa_acerto"].append(count/n_candidatos * 100.0)
            # dict_data_out["indice"].append(idx)

        data_out = pd.DataFrame(data = dict_data_out)
        data_out = data_out.sort_values(by = 'taxa_acerto')
        data_out.to_excel("acertos_CH_enem_2019.xlsx", sheet_name = "Ciências Humanas")
        print(data_out.head())

#####################################
######### LOOP THROUGH DATA #########
#####################################
operators = [RightAnswerRate(GP, EXAM_CH)]

for chunk in pd.read_csv("DADOS/MICRODADOS_ENEM_2019.csv", sep=';', chunksize=GP.chunksize, usecols = columns):
    clean_chunk = chunk.dropna(axis = 0, subset = ['TX_RESPOSTAS_CH'])
    
    for op in operators:
        op_out = op.process_chunk(clean_chunk, GP)

        if GP.count % 15 == 1:
            op.output_result(GP)

    GP.total_non_nan += clean_chunk.shape[0]
    GP.count += 1

    print("\r{0} blocos lidos (total de {2} provas lidas; {3} regular, {4} reaplicação)".format(GP.count, GP.n_chunks, GP.total_non_nan, GP.total_regular, GP.total_reaplicacao), end = '')

    # if GP.count >= GP.n_chunks:
    #     break    

print("\n\n****************************")
print("********* Finished *********")
print("****************************")
for op in operators:
    op.output_result(GP)