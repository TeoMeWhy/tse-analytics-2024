WITH tb_cand AS (

    SELECT SQ_CANDIDATO,
            SG_UF,
            DS_CARGO,
            SG_PARTIDO,
            NM_PARTIDO,
            DT_NASCIMENTO,
            DS_GENERO,
            DS_GRAU_INSTRUCAO,
            DS_ESTADO_CIVIL,
            DS_COR_RACA,
            DS_OCUPACAO

    FROM tb_candidaturas
),

tb_total_bens AS (
    SELECT SQ_CANDIDATO,
           sum(cast(replace( VR_BEM_CANDIDATO, ',', '.') as DECIMAL(15,2))) AS totalBens
    FROM tb_bens
    GROUP BY 1
),

tb_info_completa_cand AS (

    SELECT t1.*,
        COALESCE(t2.totalBens, 0) AS totalBens

    FROM tb_cand AS t1
    LEFT JOIN tb_total_bens AS t2
    ON t1.SQ_CANDIDATO = t2.SQ_CANDIDATO

),

tb_group_uf AS (

    SELECT
        SG_PARTIDO,
        NM_PARTIDO,
        SG_UF,
        AVG(CASE WHEN DS_GENERO = 'FEMININO' THEN 1 ELSE 0 END) AS txGenFeminino,
        SUM(CASE WHEN DS_GENERO = 'FEMININO' THEN 1 ELSE 0 END) AS totalGenFeminino,
        AVG(CASE WHEN DS_COR_RACA = 'PRETA' THEN 1 ELSE 0 END) AS txCorRacaPreta,
        SUM(CASE WHEN DS_COR_RACA = 'PRETA' THEN 1 ELSE 0 END) AS totalCorRacaPreta,
        AVG(CASE WHEN DS_COR_RACA IN ('PRETA', 'PARDA') THEN 1 ELSE 0 END) AS txCorRacaPretaParda,
        SUM(CASE WHEN DS_COR_RACA IN ('PRETA', 'PARDA') THEN 1 ELSE 0 END) AS totalCorRacaPretaParda,
        AVG(CASE WHEN DS_COR_RACA <> 'BRANCA' THEN 1 ELSE 0 END) AS txCorRacaNaoBranca,
        SUM(CASE WHEN DS_COR_RACA <> 'BRANCA' THEN 1 ELSE 0 END) AS totalCorRacaNaoBranca,
        count(*) AS totalCandidaturas

    FROM tb_info_completa_cand AS t1

    GROUP BY 1,2,3

),

tb_group_br AS (

    SELECT
        SG_PARTIDO,
        NM_PARTIDO,
        'BR' AS SG_UF,
        1.0 * SUM(totalGenFeminino) / SUM(totalCandidaturas) AS txGenFeminino,
        SUM(totalGenFeminino) AS totalGenFeminino,

        1.0 * SUM(totalCorRacaPreta) / SUM(totalCandidaturas) AS txCorRacaPreta,
        SUM(totalCorRacaPreta) AS totalCorRacaPreta,

        1.0 * SUM(totalCorRacaPretaParda) / SUM(totalCandidaturas) AS txCorRacaPretaParda,
        SUM(totalCorRacaPretaParda) AS totalCorRacaPretaParda,

        1.0 * SUM(totalCorRacaNaoBranca) / SUM(totalCandidaturas) AS txCorRacaNaoBranca,
        SUM(totalCorRacaNaoBranca) AS totalCorRacaNaoBranca,

        SUM(totalCandidaturas) AS totalCandidaturas

    FROM tb_group_uf

    GROUP BY 1,2,3

),

tb_union_all AS (

    SELECT * FROM tb_group_br

    UNION ALL 

    SELECT * FROM tb_group_uf

)

SELECT *
FROM tb_union_all
