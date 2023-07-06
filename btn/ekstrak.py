from bs4 import BeautifulSoup
from datetime import datetime
import requests


class EkstrakFeature:
    def __init__(self, link, list_prov,list_kab,list_kec):
        header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 Edg/111.0.1661.54'
        }
        req = requests.get(link, headers=header)
        # bs4
        self.__soup = BeautifulSoup(req.text, 'html.parser')
        #find elements
        o = self.__soup.findAll('dt')
        io = self.__soup.findAll('dd')
        #extract elements
        self.__header = []
        self.__descr = []
        for o_ in o:
            self.__header.append(o_.text)
        for io_ in io:
            self.__descr.append(io_.text)        


        # Tanggal
        if 'Disclaimer:' in self.__header:
            index = self.__header.index('Disclaimer:')
            tgl = self.__descr[index].split(' ')
            tgl = ' '.join(tgl[-3:])
            tanggal = tgl.split(' ')
            if tanggal[1] == 'Januari':
                tanggal[1] = '01'
            elif tanggal[1] == 'Februari':
                tanggal[1] = '02'
            elif tanggal[1] == 'Maret':
                tanggal[1] = '03'
            elif tanggal[1] == 'April':
                tanggal[1] = '04'
            elif tanggal[1] == 'Mei':
                tanggal[1] = '05'
            elif tanggal[1] == 'Juni':
                tanggal[1] = '06'
            elif tanggal[1] == 'Juli':
                tanggal[1] = '07'
            elif tanggal[1] == 'Agustus':
                tanggal[1] = '08'
            elif tanggal[1] == 'September':
                tanggal[1] = '09'
            elif tanggal[1] == 'Oktober':
                tanggal[1] = '10'
            elif tanggal[1] == 'November':
                tanggal[1] = '11'
            elif tanggal[1] == 'Desember':
                tanggal[1] = '12'
            tanggal = '{}-{}-{}'.format(tanggal[2],tanggal[1],tanggal[0])
            self.__tanggal = datetime.strptime(tanggal,'%Y-%m-%d').date()
        else:
            self.__tanggal = '-'

        # Harga
        if 'Limit/Harga:' in self.__header:
            index = self.__header.index('Limit/Harga:')
            harga = self.__descr[index]
            if harga.count('Rp') > 1:
                harga = harga.split('Rp ')[2]
                self.__harga = int(harga.replace(',-','').replace('.',''))    
            else:
                self.__harga = int(harga.replace('Rp ', '').replace(',-','').replace('.',''))
        else:
            self.__harga = '-'
            
        # Luas Bangunan
        if 'Luas Bangunan (m2):' in self.__header:
            index = self.__header.index('Luas Bangunan (m2):')
            self.__luas_bangunan = int(self.__descr[index].replace('.',''))
        else:
            self.__luas_bangunan = 0

        # Luas Tanah
        if 'Luas Tanah (m2):' in self.__header:
            index = self.__header.index('Luas Tanah (m2):')
            self.__luas_tanah =  int(self.__descr[index].replace('.',''))
        else:
            self.__luas_tanah = 0

        # Sertifikat
        if 'Dokumen:' in self.__header:
            index = self.__header.index('Dokumen:')
            sertifikat = self.__descr[index].split(' ')
            self.__sertifikat = sertifikat[0]
        else:
            self.__sertifikat = '-'  

        # id
        if 'Kode Asset:' in self.__header:
            index = self.__header.index('Kode Asset:')
            self.__id = self.__descr[index]
        else:
            self.__id = '-'

        # lebar jalan
        if 'Lebar Jalan Depan (m):' in self.__header:
            index = self.__header.index('Lebar Jalan Depan (m):')
            self.__lj = int(self.__descr[index])
        else:
            self.__lj = 0     

        # alamat lengkap
        try:
            if 'Lokasi:' in self.__header:
                index = self.__header.index('Lokasi:')
                self.__alamat = self.__descr[index]
                # jalan
                self.__jalan = self.__alamat.split(', ')[-3]
            else:
                self.__alamat = '-'
                self.__jalan = '-'
        except:
            index = self.__header.index('Lokasi:')
            self.__alamat = self.__descr[index]
            self.__jalan = '-'
        # Kab
        try:
            kab = self.__alamat.split(', ')[-2]
            if kab.upper() in list_kab:
                kab = kab.replace('KAB. ','').replace('KOTA ','')
                self.__kab = kab.lower()
            else:
                self.__kab = list_kab[0].replace('KAB. ','').replace('KOTA ','').lower()
        except:
            self.__kab = list_kab[0].replace('KAB. ','').replace('KOTA ','').lower()

        # Kec
        try:
            x = 1
            z = False
            while z != True:    
                kec = self.__jalan.split(' ')[-1*x:]
                kec = ' '.join(kec)
                if kec in list_kec:
                    z = True
                    self.__kec = kec
                elif kec not in list_kec:
                    x += 1
                    if x == 4:
                        z = True
                        self.__kec = list_kec[0].lower()
                    
        except:
            self.__kec = list_kec[0].lower()

        # Prov
        try:
            prov = self.__alamat.split(', ')[-1]
            if prov.upper() in list_prov:
                self.__prov = prov.lower()
            else:
                self.__prov = list_prov[0].lower()
        except:
            self.__prov = list_prov[0].lower()

    
    def ekstrak_date(self):
        return self.__tanggal
    
    def ekstrak_price(self):
        return self.__harga
    
    def ekstrak_lb(self):
        return self.__luas_bangunan
    
    def ekstrak_lt(self):
        return self.__luas_tanah
    
    def ekstrak_certifikat(self):
        return self.__sertifikat
    
    def ekstrak_id(self):
        return self.__id
    
    def ekstrak_type(self):
        return 'rumah'

    def ekstrak_lj(self):       
        return self.__lj
    
    def ekstrak_kab(self):
        return self.__kab
    
    def ekstrak_kec(self):
        return self.__kec
    
    def ekstrak_prov(self):
        return self.__prov
    
    def ekstrak_address(self):
        return self.__alamat
    
    def ekstrak_jalan(self):
        return self.__jalan

class EkstrakPages:
    def __init__(self, link):
        header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 Edg/111.0.1661.54'
        }
        req = requests.get(link, headers=header)
        # bs4
        self.__soup = BeautifulSoup(req.text, 'html.parser')

        # Ekstrak Link
        self.__link = []
        stop = self.__soup.find_all('div', {'class': 'col-md-4 col-sm-4'})
        for i in stop:
            links = i.find_all('a','link-arrow')
            for p in links:
                if p != '#':
                    self.__link.append(p.get('href'))

    def ekstrak_link(self):
        return self.__link

