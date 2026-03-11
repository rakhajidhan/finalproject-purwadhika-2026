

select
    nomor,
    gcs_uri,
    pdf_pages,
    pdf_pihak,
    pdf_isi_ringkas,
    pdf_dasar_hukum,
    pdf_amar_putusan,
    run_date
from `jcdeah-007.finalproject_rakhajidhan_mahkamahagung.putusan_pdf_detail`
where nomor is not null