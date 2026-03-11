{{ config(materialized='view') }}

select
    nomor,
    judul,
    url_detail,
    tahun,
    bulan,
    tingkat_proses,
    klasifikasi,
    kata_kunci,
    tahun_putusan,
    tanggal_register,
    lembaga_peradilan,
    jenis_lembaga_peradilan,
    hakim_ketua,
    hakim_anggota,
    panitera,
    amar,
    catatan_amar,
    tanggal_musyawarah,
    tanggal_dibacakan,
    kaidah,
    pdf_url,
    gcs_uri,
    kategori,
    scraped_at,
    run_date
from `jcdeah-007.finalproject_rakhajidhan_mahkamahagung.putusan_list`
where nomor is not null
