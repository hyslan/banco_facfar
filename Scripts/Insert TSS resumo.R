library("readxl")
library("odbc")
library("DBI")
library("dplyr")

# Estabeleça a conexão com o banco de dados SQL Server usando Windows Authentication
conexao <- dbConnect(odbc::odbc(), .connection_string = paste0("Driver={SQL Server};Server=10.66.42.188;Database=BD_MLG;Trusted_Connection=yes;"))

caminho_tss <- "TSS resumo.xlsx"
tss <- read_excel(caminho_tss, sheet="Planilha1")

# Mapping
col_mapping <- c(
  COD_TSS = "COD TSS"
  ,DESC_TSS = "DESCRIÇÃO TSS"
  ,PROCESSO = "PROCESSO"
  ,CARAC = "CARAC (max 40)"
  ,NATUREZA_TSS = "NATUREZA TSS"
  , FAMILIA = "FAMILIA"
)
mapped_tss <- tss %>%
  rename(!!!col_mapping)

# Insert Formulários MLG
str_tss <- as.data.frame(lapply(mapped_tss, as.character))
dbWriteTable(conexao,
             name = SQL('"LESTE_AD\\hcruz_novasp"."tbHyslancruz_TSS_RESUMO"'),
             value = str_tss,
             overwrite = FALSE,
             append = TRUE,
)
