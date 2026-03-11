{{ config(materialized='table') }}

select
    p.nomor,
    p.judul,
    p.kategori,
    p.tahun,
    p.bulan,
    p.tingkat_proses,
    p.lembaga_peradilan,
    p.amar,
    p.gcs_uri,
    pdf.pdf_pages,
    pdf.pdf_pihak,
    pdf.pdf_isi_ringkas,
    pdf.pdf_amar_putusan,
    p.scraped_at,
    p.run_date,
    current_timestamp() as dbt_loaded_at
from {{ ref('stg_ma_putusan_list') }} p
left join {{ ref('stg_ma_pdf_detail') }} pdf
    on p.nomor = pdf.nomor
    and p.run_date = pdf.run_date
