SELECT DISTINCT
       CD_ELEICAO AS idEleicao,
       ANO_ELEICAO AS nrAnoEleicao,
       CD_TIPO_ELEICAO AS codTipoEleicao,
       NM_TIPO_ELEICAO AS descTipoEleicao,
       DS_ELEICAO AS descEleicao,
       DT_ELEICAO AS dtEleicao,
       TP_ABRANGENCIA_ELEICAO AS descAbrangenciaEleicao

FROM bronze.tse.consulta_cand