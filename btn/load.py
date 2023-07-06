import pandas as pd
import glob
import os

class CreateDataframe:

    def dataframe_kec(self, data, dir_kab, c):
        coloumn = ['harga','luas_tanah','luas_bangunan', 'lebar_jalan', 'type','sertifikat','tanggal','kec','kab/kota','prov','alamat_lengkap','id_rumah','link']
        data = data[1:]
        name_file = '{}.csv'.format(c)
        path_kec = os.path.join(dir_kab, name_file)
        dframe = pd.DataFrame(data, columns=coloumn)
        dframe = dframe.drop_duplicates()
        dframe.to_csv(path_kec, index=False)
        return '[Create] KEC {} Sudah Dibuat'.format(path_kec)

    def dataframe_kab(self, dir_kab, dir_prov, b):
        path_folder = dir_kab
        data_all_kec = glob.glob(os.path.join(path_folder, "*.csv"))
        data_all_kec = pd.concat([pd.read_csv(f) for f in data_all_kec], axis=0).reset_index(drop=True)
        data_all_kec = data_all_kec.drop_duplicates()
        name_kab = '{}.csv'.format(b)
        path_data_all = os.path.join(dir_prov, name_kab)
        data_all_kec.to_csv(path_data_all, index=False)
        return '[Create] KAB {} Sudah Dibuat'.format(name_kab)

    def dataframe_prov(self, dir_prov, path_indo, a):
        path_folder = dir_prov
        data_all_kab = glob.glob(os.path.join(path_folder, "*.csv"))
        data_all_kab = pd.concat([pd.read_csv(f) for f in data_all_kab], axis=0).reset_index(drop=True)
        data_all_kab = data_all_kab.drop_duplicates()
        name_prov = '{}.csv'.format(a)
        path_data_all = os.path.join(path_indo, name_prov)
        data_all_kab.to_csv(path_data_all, index=False)
        return '[Create] PROV {} Sudah Dibuat'.format(name_prov)

    def dataframe_error(self, error, ket_error, path_indo, b):
        col = ['ERROR KAB', 'KET']
        file_error = 'ERROR KAB {}.csv'.format(b)
        save_to = os.path.join(path_indo, file_error)
        data_error = zip(error, ket_error)
        tabel = pd.DataFrame(data=data_error, columns=col)
        tabel.to_csv(save_to, index=False)

    def dataframe_checkpoint(self, checkpoint, path_indo, ai, bi, ci):
        col = ['PROV', 'KAB', 'KEC']
        file_checkpont = 'CHECKPOINT.csv'
        save_to = os.path.join(path_indo, file_checkpont)
        try:
            indeks_prov = checkpoint['PROV'][0]+ai
            indeks_kab = bi
            indeks_kec = ci
            data_checkpont = zip([indeks_prov], [indeks_kab], [indeks_kec])
        except:
            indeks_prov = ai
            indeks_kab = bi
            indeks_kec = ci
            data_checkpont = zip([indeks_prov], [indeks_kab], [indeks_kec])
        tabel = pd.DataFrame(data=data_checkpont, columns=col)
        tabel.to_csv(save_to, index=False)
        return '[CHECKPOINT UPDATE] -><- {} {} {} -><-'.format(indeks_prov, indeks_kab, indeks_kec)

    def dataframe_allprof(self, path_indo):
        data_all_prov = glob.glob(os.path.join(path_indo, "*.csv"))
        data_all_prov = pd.concat([pd.read_csv(f) for f in data_all_prov], axis=0).reset_index(drop=True)
        data_all_prov = data_all_prov.drop_duplicates()
        name = 'ALL PROVINSI.csv'
        path_data_all = os.path.join(path_indo, name)
        data_all_prov.to_csv(path_data_all, index=False)
        print('[Create] ALL PROV ', name, 'Sudah Dibuat')
        print('[END] Ekstrak Selesai')
        print('*******:)********')
    
        