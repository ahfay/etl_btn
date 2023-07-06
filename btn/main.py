from btn.ekstrak import EkstrakPages
from btn.load import CreateDataframe
import pandas as pd
import os
from time import sleep
import requests
from bs4 import BeautifulSoup
from btn.ekstrak import EkstrakFeature

def ekstraktor():
    firt_running_program = True
    # Data berisi dafftar Provinsi dan kabupaten kotanya
    df = pd.read_csv("btn/Kode_wilayah.csv")
    subcity = pd.read_csv('btn/id_wilayah.csv')

    # Menentukan lokasi penyimpanan hasil scrapping
    path_indo = os.path.join('btn', 'Indonesia')
    path_prov = os.path.join('btn', 'Prov')
    path_ket = os.path.join(path_indo, 'KET')
    header = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 Edg/111.0.1661.54'
    }

    build_df = CreateDataframe() # Objek Untuk Membuat Dataframe

    # Membuat Folder Indonesia dan Prov
    try:
        os.mkdir(path_indo)
        os.mkdir(path_prov)
        os.mkdir(path_ket)
    except:
        skip = ''

    # Load Checkpoint
    try:
        checkpoint = pd.read_csv("btn/Indonesia/KET/CHECKPOINT.csv")
    except:
        skip = ''

    # List Seluruh Provinsi
    list_prov = df['Prov'].unique()
    # Load Checkpoint Provinsi
    try:
        list_prov = list_prov[checkpoint['PROV'][0]:]
    except:
        skip = ''

    for ai, a in enumerate(list_prov): # Ekstrak List Provinsi Beserta Indeksnya
        checkpoint_kab = 0
        print('====================')
        print('[Start] KODE PROV ', a)
        dir_prov = os.path.join(path_prov, str(a)) 
        try:
            os.mkdir(dir_prov) # Membuat Folder Nama Provinsi di dalam Folder Prov
        except:
            skip = ''
        list_kab = df[df['Prov'] == a ]['Kab'] # Membuat List Seluruh kabupaten di 1 Provinsi
        list_kab = list_kab.unique()

        # LOAD CHECKPOINT KABUPATEN
        try:
            if firt_running_program == True:
                list_kab = list_kab[checkpoint['KAB'][0]:]
        except:
            skip =''

        for bi, b in enumerate(list_kab): # Ekstrak List Kabupaten Beserta Indeksnya
            try:
                if firt_running_program == True:
                    checkpoint_kec = checkpoint['KEC'][0]
                    checkpoint_kab = checkpoint['KAB'][0]
                else:
                    checkpoint_kec = -1
            except:
                skip = ''

            # Variabel Untuk Menyiman Data yang Error
            error = []
            ket_error = []

            dir_kab = os.path.join(dir_prov, str(b)) 
            try:
                os.mkdir(dir_kab) # Membuat Folder Nama Kabupaten di dalam Folder Prov
            except:
                skip = ''
            print('[Start] KODE KAB ', b)

            # Membuat List Seluruh Kecamatan di 1 Kabupaten
            list_kec = None
            try:
                list_kec = df[df['Kab'] == b]['Kec']
                try:
                    if checkpoint_kec + 1 == len(list_kec):
                        list_kec = None
                    else:
                        list_kec = list_kec[checkpoint_kec + 1:]
                except:
                    print('[ERROR] UNKNOWN KAB ', b)
                    error.append(b)
                    ket_error.append('UNKNOWN')
            except:
                print('[ERROR] TYPO KAB ', b)
                error.append(b)
                ket_error.append('TYPO')
            if list_kec is None:
                try:
                    df_checkpoint = build_df.dataframe_checkpoint(checkpoint=checkpoint, path_indo=path_indo, ai=ai, bi=checkpoint_kab+bi, ci=0)
                    print(df_checkpoint)
                except:
                    checkpoint = None
                    df_checkpoint = build_df.dataframe_checkpoint(checkpoint=checkpoint, path_indo=path_indo, ai=ai, bi=checkpoint_kab+bi, ci=0)
                    print(df_checkpoint)
                pass
            elif len(list_kec) == 0:
                print('[ERROR] NOTHING KAB ', b)
                error.append(b)
                ket_error.append('TIDAK ADA')
                # Dataframe Check Point
                try:
                    df_checkpoint = build_df.dataframe_checkpoint(checkpoint=checkpoint, path_indo=path_indo, ai=ai, bi=checkpoint_kab+bi, ci=0)
                    print(df_checkpoint)
                except:
                    checkpoint = None
                    df_checkpoint = build_df.dataframe_checkpoint(checkpoint=checkpoint, path_indo=path_indo, ai=ai, bi=checkpoint_kab+bi, ci=0)
                    print(df_checkpoint)
                pass
            else:            
                for ci, c in enumerate(list_kec): # Ekstrak List Kecamatan Beserta Indeksnya
                
                    print('[Start] KODE KEC ', c)
                    data = [[None,None,None,None,None,None,None,None,None,None,None,None,None]]
                    broken_link = []
                    halaman = 0
                    url = "https://rumahmurah.btn.co.id/btn/search?provinsiid={}&kabupatenid={}&kecamatanid={}&hargaid=0&kode=&alamat=&sort=0&offset={}".format(a, b, c, halaman)
                    req = requests.get(url, headers=header)
                    soup = BeautifulSoup(req.text, 'html.parser')
                    stop = soup.find_all('div', {'class': 'col-md-4 col-sm-4'})
                    while stop != []:
                        sleep(0.15) # Jeda Waktu
                        
                        url = "https://rumahmurah.btn.co.id/btn/search?provinsiid={}&kabupatenid={}&kecamatanid={}&hargaid=0&kode=&alamat=&sort=0&offset={}".format(a, b, c, halaman)
                        req = requests.get(url, headers=header)
                        soup = BeautifulSoup(req.text, 'html.parser')
                        stop = soup.find_all('div', {'class': 'col-md-4 col-sm-4'})

                        if stop != []:
                            ekstrak_ = EkstrakPages(url)
                            link = ekstrak_.ekstrak_link() # Ekstrak Link
                            
                            # Scarpping per Link yang sudah di dapatkan dari setiap halaman
                            for index, url in enumerate(link):
                                print("[PROCESS] KODE Prov ", a, "KODE Kab/Kota", b, "KODE Kec ", c, ' Offset ', halaman, ' LINK ', index + 1, '/', len(link))
                                sleep(0.15) # Jeda Waktu
                                status = False
                                batas = 1
                                while status != True:
                                    row = []
                                    
                                    try:
                                        ekstrak = EkstrakFeature(url, subcity[subcity['Prov'] == a]['NamaProv'].unique(), subcity[subcity['Kab'] == b]['NamaKab'].unique(), subcity[subcity['Kec'] == c]['NamaKec'].unique())
                                        tanggal = ekstrak.ekstrak_date() # Ekstrak Tanggal Posting
                                        harga = ekstrak.ekstrak_price() # Ekstrak Harga
                                        luas_bangunan = ekstrak.ekstrak_lb() # Ekstrak Luas Bangunan
                                        luas_tanah = ekstrak.ekstrak_lt() # Ekstrak Luas Tanah
                                        sertifikat = ekstrak.ekstrak_certifikat() # Ekstrak Sertifikat
                                        kab = ekstrak.ekstrak_kab() # Ekstrak Kabupaten
                                        kec = ekstrak.ekstrak_kec() # Ekstrak Kecataman
                                        prov = ekstrak.ekstrak_prov() # Menambahkan provinsi
                                        type = ekstrak.ekstrak_type() # Menambahkan Tipe Properti
                                        link_id = url
                                        lebar_jalan = ekstrak.ekstrak_lj() # Menambahkan luas Lebar Jalan Depan Rumah
                                        alamat_lengkap = ekstrak.ekstrak_address() # Menambahkan Alamat Lengkap
                                        id = ekstrak.ekstrak_id() # Menambahkan Id Produk

                                        row.append(harga)
                                        row.append(luas_tanah)
                                        row.append(luas_bangunan )
                                        row.append(lebar_jalan)
                                        row.append(type)
                                        row.append(sertifikat)
                                        row.append(tanggal)
                                        row.append(kec)
                                        row.append(kab)
                                        row.append(prov)
                                        row.append(alamat_lengkap)
                                        row.append(id)
                                        row.append(link_id)
                                        data.append(row)
                                        status = True
                                    except:
                                        print('[ERROR] URL', url)
                                        sleep(1)
                                        
                                        if batas == 3:
                                            error.append(b)
                                            ket_error.append(url)
                                            batas += 1
                                            status = True
                                        else:
                                            batas += 1
                                
                            halaman += 9
                    else:
                        print('[End] KEC ', c, halaman - 1, 'Halaman')

                    # Dataframe KECAMATAN
                    df_kec = build_df.dataframe_kec(data=data, dir_kab=dir_kab, c=c)
                    print(df_kec)
                
                    # Dataframe Check Point
                    try:
                        if checkpoint_kec == -1:
                            df_checkpoint = build_df.dataframe_checkpoint(checkpoint=checkpoint, path_indo=path_ket, ai=ai, bi=checkpoint_kab+bi, ci=checkpoint_kec+1+ci)
                            print(df_checkpoint)
                        else:
                            df_checkpoint = build_df.dataframe_checkpoint(checkpoint=checkpoint, path_indo=path_ket, ai=ai, bi=checkpoint_kab+bi, ci=checkpoint_kec+1+ci)
                            print(df_checkpoint)
                    except:
                        checkpoint = zip([ai], [bi], [ci])
                        df_checkpoint = build_df.dataframe_checkpoint(checkpoint=checkpoint, path_indo=path_ket, ai=ai, bi=bi, ci=ci)
                        print(df_checkpoint)

                    firt_running_program = False

                # Dataframe KABUPATEN
                df_kab = build_df.dataframe_kab(dir_kab=dir_kab, dir_prov=dir_prov, b=b)
                print(df_kab)

            # Dataframe ERROR
            if error != []:
                build_df.dataframe_error(error=error, ket_error=ket_error, path_indo=path_ket, b=b)

        # Dataframe PROVINSI
        df_prov = build_df.dataframe_prov(dir_prov=dir_prov, path_indo=path_indo, a=a)
        print(df_prov)

    # DATAFRAME ALL PROVINSI
    build_df.dataframe_allprof(path_indo=path_indo)
