WITH tb_cand AS (

    SELECT  DISTINCT
            t1.idCandidatura,
            t1.descUF,
            t1.descCargoCandidatura,
            t1.descSiglaPartido,
            t1.descNomePartido,
            t1.dtNascimentoCandidato,
            INT(months_between(NOW(), t1.dtNascimentoCandidato) / 12) AS nrIdade,
            t1.descGeneroCandidato,
            t1.descGrauInstrucaoCandidato,
            t1.descEstadoCivilCandidato,
            t1.descCorRacaCandidato,
            t1.descOcupacaoCandidato

    FROM silver.tse.candidaturas AS t1

    LEFT JOIN  silver.tse.eleicoes AS t2
    ON t1.idEleicao = t2.idEleicao
    
    WHERE t2.nrAnoEleicao = 2024
),

tb_total_bens AS (
    SELECT t1.idCandidatura,
           sum(double(replace( t1.vlBemCandidato, ',', '.'))) AS totalBens
    FROM silver.tse.bens_candidatos AS t1

    LEFT JOIN  silver.tse.eleicoes AS t2
    ON t1.idEleicao = t2.idEleicao

    WHERE NOT (t1.idCandidatura = 160002381679 AND t1.descBemCandidato = 'VEICULOS')
    GROUP BY 1
),

tb_info_completa_cand AS (

    SELECT t1.*,
        COALESCE(t2.totalBens, 0) AS totalBens        

    FROM tb_cand AS t1
    LEFT JOIN tb_total_bens AS t2
    ON t1.idCandidatura = t2.idCandidatura

),

tb_cube AS (
    SELECT
            t1.descSiglaPartido,
            t1.descCargoCandidatura,
            t1.descUF,
            AVG(CASE WHEN t1.descGeneroCandidato = 'FEMININO' THEN 1 ELSE 0 END) AS txGenFeminino,
            SUM(CASE WHEN t1.descGeneroCandidato = 'FEMININO' THEN 1 ELSE 0 END) AS totalGenFeminino,
            AVG(CASE WHEN t1.descCorRacaCandidato = 'PRETA' THEN 1 ELSE 0 END) AS txCorRacaPreta,
            SUM(CASE WHEN t1.descCorRacaCandidato = 'PRETA' THEN 1 ELSE 0 END) AS totalCorRacaPreta,
            AVG(CASE WHEN t1.descCorRacaCandidato IN ('PRETA', 'PARDA') THEN 1 ELSE 0 END) AS txCorRacaPretaParda,
            SUM(CASE WHEN t1.descCorRacaCandidato IN ('PRETA', 'PARDA') THEN 1 ELSE 0 END) AS totalCorRacaPretaParda,
            AVG(CASE WHEN t1.descCorRacaCandidato <> 'BRANCA' THEN 1 ELSE 0 END) AS txCorRacaNaoBranca,
            SUM(CASE WHEN t1.descCorRacaCandidato <> 'BRANCA' THEN 1 ELSE 0 END) AS totalCorRacaNaoBranca,
            SUM(totalBens) AS totalBens,
            AVG(totalBens) AS avgBens,
            COALESCE(AVG(case when totalBens > 1 then totalBens end),0) AS avgBensNotZero,
            MEDIAN(totalBens) AS medianBens,
            COALESCE(MEDIAN(case when totalBens > 1 then totalBens end),0) AS medianBensNotZero,
            1.0 * SUM(CASE WHEN t1.descEstadoCivilCandidato='CASADO(A)' THEN 1 ELSE 0 END) / count(*) AS txEstadoCivilCasado,
            1.0 * SUM(CASE WHEN t1.descEstadoCivilCandidato='SOLTEIRO(A)' THEN 1 ELSE 0 END) / count(*) AS txEstadoCivilSolteiro,
            1.0 * SUM(CASE WHEN t1.descEstadoCivilCandidato IN ('DIVORCIADO(A))', 'SEPARADO(A) JUDICIALMENTE') THEN 1 ELSE 0 END) / count(*) AS txEstadoCivilSeparadoDivorciado,
            AVG(t1.nrIdade) AS avgIdade,
            count(*) AS totalCandidaturas

    FROM tb_info_completa_cand AS t1

    GROUP BY t1.descSiglaPartido, t1.descCargoCandidatura, t1.descUF WITH CUBE
    ORDER BY t1.descSiglaPartido, t1.descCargoCandidatura, t1.descUF
),

tb_final AS (

    SELECT
        coalesce(t1.descSiglaPartido, 'GERAL') AS descSiglaPartido,
        coalesce(t1.descCargoCandidatura, 'GERAL') AS descCargoCandidatura,
        coalesce(t1.descUF, 'BR') AS descUF,
        txGenFeminino,
        totalGenFeminino,
        txCorRacaPreta,
        totalCorRacaPreta,
        txCorRacaPretaParda,
        totalCorRacaPretaParda,
        txCorRacaNaoBranca,
        totalCorRacaNaoBranca,
        totalBens,
        avgBens,
        avgBensNotZero,
        totalBens / 1000 AS totalBensPer1000,
        avgBens / 1000 AS avgBensPer1000,
        avgBensNotZero / 1000 AS avgBensPerNotZero1000,
        medianBens,
        medianBensNotZero,
        medianBens / 1000 AS medianBensPer1000,
        medianBensNotZero / 1000 AS medianBensNotZeroPer1000,
        txEstadoCivilCasado,
        txEstadoCivilSolteiro,
        txEstadoCivilSeparadoDivorciado,
        avgIdade,
        totalCandidaturas

    FROM tb_cube AS t1
)

SELECT *
FROM tb_final