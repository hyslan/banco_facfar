# --------------------------------------------------------
# Sigla NOVA, apenas insert sem drop...
# caminho <- list.files(
#   "\\\\spo-leste60_fs\\FISCALIZAÇÃO\\FAC FAR\\BANCO DE DADOS ML\\OLM",
#   pattern = "*.xlsx", full.names = T)


# --- Arquivo atual ----------------------------------------
# * último arquivo -> 16-03-24
# Não de Crtl + Shift + Enter -> tem bugs pra resolver!
# -----------------------------------------------------------
library("readxl")
library("odbc")
library("DBI")
library("dplyr")
library("lubridate")
library("stringr")

arquivo <- "16-03-24.xlsx"

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
    "Driver={SQL Server};Server=10.66.12.185;Database=HARMONIA;UID=sa;PWD=S@besp&2024*Olmp"))


caminho <- paste(
  "C:\\Users\\hcruz.novasp.SBSP\\Documents\\FACFAR\\dados\\",
  arquivo, sep="")
print("Path:")
print(caminho)

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
             name = SQL('"FAR"."Formularios"'),
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

str_relatorio <- str_relatorio |> 
  mutate(DT_COMPETENCIA = as.POSIXct(DT_COMPETENCIA, format = "%d/%m/%Y %H:%M", 
                                     tz = "UTC", 
                                     optional = TRUE),
         DT_PLANEJAMENTO = as.POSIXct(DT_PLANEJAMENTO, format = "%d/%m/%Y %H:%M", 
                                      tz = "UTC", 
                                      optional = TRUE))

str_relatorio$ENDERECO <- iconv(str_relatorio$ENDERECO, from = "UTF-8", to = "latin1")
str_relatorio$ENDERECO <- iconv(str_relatorio$ENDERECO, from = "UTF-8", to = "latin1")
str_relatorio <- str_relatorio %>%
  mutate(across(contains("DT_"), as.POSIXct, format = "%Y-%m-%d %H:%M:%S"))
str_relatorio[is.na(str_relatorio)] <- NA

str_relatorio$UN_EXECUTANTE <- substr(str_relatorio$UN_EXECUTANTE, 1, 255)


dbWriteTable(conexao,
             name = SQL('"FAR"."RelatorioOs"'),
             value = str_relatorio,
             overwrite = FALSE,
             append = TRUE,
)

print(paste0("Dados inseridos com sucesso de: ", arquivo))
