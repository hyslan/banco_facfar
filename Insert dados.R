library("readxl")
library("odbc")
library("DBI")
library("dplyr")
library("lubridate")
library("stringr")

col_mapping_formularios <- c(
  NUM_OS = "Número OS"
  ,COD_TSS_OS_FISCALIZADA = "Cod. TSS OS Fiscalizada"
  ,CAUSA_RESULTADO_OS_FISCALIZADA = "Causa Resultado OS Fiscalizada"
  ,GRUPO = "Grupo"
  ,DESCRICAO = "Descrição"
  ,CODIGO = "Código"
  ,VALOR = "Valor"
)

col_mapping_relatorios <- c(
  NUM_OS = "Número OS"
  ,COD_UE = "Código UE"
  ,UN_EXECUTANTE = "Unidade Executante"
  ,ATC = "ATC"
  ,SF = "SF"
  ,ATO = "ATO"
  ,AS_OS = "AS"
  ,ST_OS = "Status"
  ,EQUIPE_TRABALHO = "Equipe de trabalho"
  ,COD_CONTRATO = "Código Contrato"
  ,DESC_CONTRATO = "Descrição Contrato"
  ,COD_TSS = "Código TSS"
  ,TSS = "TSS"
  ,CAUSA_RESULTADO = "Causa Resultado"
  ,COD_MUNICIPIO = "Código Município"
  ,MUNICIPIO = "Município...16"
  ,ENDERECO = "Endereço"
  ,PDE = "PDE"
  ,DT_COMPETENCIA = "Data de Competência"
  ,DT_PLANEJAMENTO = "Data de Planejamento"
  ,DT_INICIO_EXEC = "Data Início Execução"
  ,DT_FIM_EXEC_OS_FISCALIZADA = "Data Fim Execução"
  ,COD_TSS_OS_FISCALIZADA = "Cod. TSS OS Fiscalizada"
  ,COD_CONTRATO_OS_FISCALIZADA = "Código Contrato OS Fiscalizada"
  ,DESC_CONTRATO_OS_FISCALIZADA = "Descrição Contrato OS Fiscalizada"
  ,DT_COMPETENCIA_OS_FISCALIZADA = "Data de Competência OS Fiscalizada"
  ,DT_PLANEJAMENTO_OS_FISCALIZADA = "Data de Planejamento OS Fiscalizada"
  ,DT_INICIO_EXEC_OS_FISCALIZADA = "Data Início Execução OS Fiscalizada"
  ,DT_FIM_EXECUCAO = "Data Fim Execução OS Fiscalizada"
  ,MUNICIPIO_1 = "Município...30"
  ,SETOR = "Setor"
  ,ROTA = "Rota"
  ,QUADRA = "Quadra"
  ,LOCAL_OS = "Local"
  ,VILA = "Vila"
  ,SUB_LOCAL = "SubLocal"
  ,LONGITUDE = "Longitude"
  ,LATITUDE = "Latitude"
)


# Estabeleça a conexão com o banco de dados SQL Server usando Windows Authentication
conexao <- dbConnect(
  odbc::odbc(),
  .connection_string = paste0(
    "Driver={SQL Server};Server=10.66.9.46;Database=BD_ML;Trusted_Connection=yes;"))
# --------------------------------------------------------
# Sigla NOVA, apenas insert sem drop...
# caminho <- list.files(
#   "\\\\spo-leste60_fs\\FISCALIZAÇÃO\\FAC FAR\\BANCO DE DADOS ML\\OLM",
#   pattern = "*.xlsx", full.names = T)


# Arquivo atual
# último arquivo -> 17/06/2024
# ------------------------------
caminho <- "\\\\spo-leste60_fs\\FISCALIZAÇÃO\\FAC FAR\\BANCO DE DADOS ML\\OLM\\10 - 17 a 23.junho.24.xlsx"

dt1 <- lapply(caminho, function(x) read_excel(x, sheet = 1)) |> bind_rows()
dt2 <- lapply(caminho, function(x) read_excel(x, sheet = 2)) |> bind_rows()

mapped_relatorios <- dt1 %>%
  rename(!!!col_mapping_relatorios)

mapped_formularios <- dt2 %>%
  rename(!!!col_mapping_formularios)

formularios_filtrado <- mapped_formularios |> 
  filter(DESCRICAO == "UTILIZACAO CORRETA DO PDA" 
         | DESCRICAO == "PAVIMENTACAO DE ACORDO ESPECIFICACAO TEC"
  )


# Insert 
str_formulario <- as.data.frame(lapply(formularios_filtrado, as.character))
dbWriteTable(conexao,
             name = SQL('"LESTE_AD\\hcruz_novasp"."tbHyslancruz_Formularios_OLM_FACFAR"'),
             value = str_formulario,
             overwrite = FALSE,
             append = TRUE,
)


str_relatorio <- as.data.frame(lapply(mapped_relatorios, as.character))
str_relatorio <- str_relatorio |> 
  mutate(DT_COMPETENCIA = as.POSIXct(DT_COMPETENCIA, format = "%d/%m/%Y %H:%M"),
         DT_PLANEJAMENTO = as.POSIXct(DT_PLANEJAMENTO, format = "%d/%m/%Y %H:%M"),
         DT_INICIO_EXEC = as.POSIXct(DT_INICIO_EXEC, format = "%d/%m/%Y %H:%M"),
         DT_FIM_EXEC_OS_FISCALIZADA = as.POSIXct(DT_FIM_EXEC_OS_FISCALIZADA, format = "%d/%m/%Y %H:%M"),
         DT_COMPETENCIA_OS_FISCALIZADA = as.POSIXct(DT_COMPETENCIA_OS_FISCALIZADA, format = "%d/%m/%Y %H:%M"),
         DT_PLANEJAMENTO_OS_FISCALIZADA = as.POSIXct(DT_PLANEJAMENTO_OS_FISCALIZADA, format = "%d/%m/%Y %H:%M"),
         DT_INICIO_EXEC_OS_FISCALIZADA = as.POSIXct(DT_INICIO_EXEC_OS_FISCALIZADA, format = "%d/%m/%Y %H:%M"),
         DT_FIM_EXECUCAO = as.POSIXct(DT_FIM_EXECUCAO, format = "%d/%m/%Y %H:%M"))

dbWriteTable(conexao,
             name = SQL('"LESTE_AD\\hcruz_novasp"."tbHyslancruz_RelatorioOS_OLM_FACFAR"'),
             value = str_relatorio,
             overwrite = FALSE,
             append = TRUE,
)


# -----------------------------------------------------------

### Modelo antigo
# Executa Procedure
dbExecute(conexao,
          "
EXEC [LESTE_AD\\hcruz_novasp].[P_BANCO_FACFAR]
")

#Planilhas Excel
caminho_mlg <- "\\\\spo-leste60_fs\\FISCALIZAÇÃO\\FAC FAR\\BANCO DE DADOS ML\\MLG\\Relatório Combinado MLG.xlsx"
caminho_mln <- "\\\\spo-leste60_fs\\FISCALIZAÇÃO\\FAC FAR\\BANCO DE DADOS ML\\MLN - Alto Tietê\\Relatório Combinado MLN.xlsx"
caminho_mlq <- "\\\\spo-leste60_fs\\FISCALIZAÇÃO\\FAC FAR\\BANCO DE DADOS ML\\MLQ - Itaquera\\Relatório Combinado MLQ.xlsx"

relatorios <- "Relatório OS"
formularios <- "Relatório de Formulários"

relatorios_mlg <- read_excel(caminho_mlg, sheet = relatorios)
formularios_mlg <- read_excel(caminho_mlg, sheet = formularios)

relatorios_mln <- read_excel(caminho_mln, sheet = relatorios)
formularios_mln <- read_excel(caminho_mln, sheet = "Formulário")

relatorios_mlq <- read_excel(caminho_mlq, sheet = relatorios)
formularios_mlq <- read_excel(caminho_mlq, sheet = "Formulário")

# Mapeamento das colunas
col_mapping_formularios <- c(
  NUM_OS = "Número OS"
  ,COD_TSS_OS_FISCALIZADA = "Cod. TSS OS Fiscalizada"
  ,CAUSA_RESULTADO_OS_FISCALIZADA = "Causa Resultado OS Fiscalizada"
  ,GRUPO = "Grupo"
  ,DESCRICAO = "Descrição"
  ,CODIGO = "Código"
  ,VALOR = "Valor"
)

col_mapping_relatorios <- c(
   NUM_OS = "Número OS"
  ,COD_UE = "Código UE"
  ,UN_EXECUTANTE = "Unidade Executante"
  ,ATC = "ATC"
  ,SF = "SF"
  ,ATO = "ATO"
  ,AS_OS = "AS"
  ,ST_OS = "Status"
  ,EQUIPE_TRABALHO = "Equipe de trabalho"
  ,COD_CONTRATO = "Código Contrato"
  ,DESC_CONTRATO = "Descrição Contrato"
  ,COD_TSS = "Código TSS"
  ,TSS = "TSS"
  ,CAUSA_RESULTADO = "Causa Resultado"
  ,COD_MUNICIPIO = "Código Município"
  ,MUNICIPIO = "Município"
  ,ENDERECO = "Endereço"
  ,PDE = "PDE"
  ,DT_COMPETENCIA = "Data de Competência"
  ,DT_PLANEJAMENTO = "Data de Planejamento"
  ,DT_INICIO_EXEC = "Data Início Execução"
  ,DT_FIM_EXEC_OS_FISCALIZADA = "Data Fim Execução"
  ,COD_TSS_OS_FISCALIZADA = "Cod. TSS OS Fiscalizada"
  ,COD_CONTRATO_OS_FISCALIZADA = "Código Contrato OS Fiscalizada"
  ,DESC_CONTRATO_OS_FISCALIZADA = "Descrição Contrato OS Fiscalizada"
  ,DT_COMPETENCIA_OS_FISCALIZADA = "Data de Competência OS Fiscalizada"
  ,DT_PLANEJAMENTO_OS_FISCALIZADA = "Data de Planejamento OS Fiscalizada"
  ,DT_INICIO_EXEC_OS_FISCALIZADA = "Data Início Execução OS Fiscalizada"
  ,DT_FIM_EXECUCAO = "Data Fim Execução OS Fiscalizada"
  ,MUNICIPIO_1 = "Município_1"
  ,SETOR = "Setor"
  ,ROTA = "Rota"
  ,QUADRA = "Quadra"
  ,LOCAL_OS = "Local"
  ,VILA = "Vila"
  ,SUB_LOCAL = "SubLocal"
  ,LONGITUDE = "Longitude"
  ,LATITUDE = "Latitude"
)

mapped_formularios <- formularios_mlg %>%
  rename(!!!col_mapping_formularios)

mapped_relatorios <- relatorios_mlg %>%
  rename(!!!col_mapping_relatorios)

mapped_formularios_mln <- formularios_mln %>%
  rename(!!!col_mapping_formularios)

mapped_relatorios_mln <- relatorios_mln %>%
  rename(!!!col_mapping_relatorios)

mapped_formularios_mlq <- formularios_mlq %>%
  rename(!!!col_mapping_formularios)

mapped_relatorios_mlq <- relatorios_mlq %>%
  rename(!!!col_mapping_relatorios)

# Filtrando os formulários
formularios_mlg_filtrado <- mapped_formularios[,
                                               -which(
                                                 names(
                                                   mapped_formularios) == "ID_ORDEM"
                                                 )
                                               ] %>%
  filter(DESCRICAO == "UTILIZACAO CORRETA DO PDA" 
         | DESCRICAO == "PAVIMENTACAO DE ACORDO ESPECIFICACAO TEC"
         )

formularios_mln_filtrado <- mapped_formularios_mln[,
                                               -which(
                                                 names(
                                                   mapped_formularios) == "ID_ORDEM"
                                               )
] %>%
  filter(DESCRICAO == "UTILIZACAO CORRETA DO PDA" 
         | DESCRICAO == "PAVIMENTACAO DE ACORDO ESPECIFICACAO TEC"
  )

formularios_mlq_filtrado <- mapped_formularios_mlq[,
                                               -which(
                                                 names(
                                                   mapped_formularios) == "ID_ORDEM"
                                               )
] %>%
  filter(DESCRICAO == "UTILIZACAO CORRETA DO PDA" 
         | DESCRICAO == "PAVIMENTACAO DE ACORDO ESPECIFICACAO TEC"
  )


# Insert Formulários MLG
str_mlg_formulario <- as.data.frame(lapply(formularios_mlg_filtrado, as.character))
dbWriteTable(conexao,
             name = SQL('"LESTE_AD\\hcruz_novasp"."tbHyslancruz_Formularios_MLG_FACFAR"'),
             value = str_mlg_formulario,
             overwrite = FALSE,
             append = TRUE,
)

# Insert Relatórios MLG
str_mlg_relatorio <- as.data.frame(lapply(mapped_relatorios, as.character))
dbWriteTable(conexao,
             name = SQL('"LESTE_AD\\hcruz_novasp"."tbHyslancruz_RelatorioOS_MLG_FACFAR"'),
             value = str_mlg_relatorio,
             overwrite = FALSE,
             append = TRUE,
)

# Insert Formulários MLN
str_mln_formulario <- as.data.frame(lapply(formularios_mln_filtrado, as.character))
dbWriteTable(conexao,
             name = SQL('"LESTE_AD\\hcruz_novasp"."tbHyslancruz_Formularios_MLN_FACFAR"'),
             value = str_mln_formulario,
             overwrite = FALSE,
             append = TRUE,
)

# Insert Relatórios MLN
str_mln_relatorio <- as.data.frame(lapply(mapped_relatorios_mln, as.character))
dbWriteTable(conexao,
             name = SQL('"LESTE_AD\\hcruz_novasp"."tbHyslancruz_RelatorioOS_MLN_FACFAR"'),
             value = str_mln_relatorio,
             overwrite = FALSE,
             append = TRUE,
)

# Insert Formulários MLQ
str_mlq_formulario <- as.data.frame(lapply(formularios_mlq_filtrado, as.character))
dbWriteTable(conexao,
             name = SQL('"LESTE_AD\\hcruz_novasp"."tbHyslancruz_Formularios_MLQ_FACFAR"'),
             value = str_mlq_formulario,
             overwrite = FALSE,
             append = TRUE,
)

# Insert Relatórios MLQ
str_mlq_relatorio <- as.data.frame(lapply(mapped_relatorios_mlq, as.character))
dbWriteTable(conexao,
             name = SQL('"LESTE_AD\\hcruz_novasp"."tbHyslancruz_RelatorioOS_MLQ_FACFAR"'),
             value = str_mlq_relatorio,
             overwrite = FALSE,
             append = TRUE,
)
print("Dados inseridos com sucesso!")
