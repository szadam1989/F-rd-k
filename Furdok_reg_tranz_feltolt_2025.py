#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
import oracledb
import getpass
import numpy as np
import datetime


# In[2]:


def actual_time():
    f_now = datetime.datetime.now().strftime("%Y.%m.%d. %H:%M:%S")
    return f_now


# In[3]:


def regUpdate(tableName, attributeName, signal):
    value_one = regFurdok[(regFurdok[attributeName].str.contains(signal) == False)]
    value_one[attributeName] = value_one[attributeName].apply(lambda x: x.replace("{", ""))
    value_one[attributeName] = value_one[attributeName].apply(lambda x: x.replace("}", ""))
    
    for i in range(0, len(value_one)):
        #not (pd.isna(value_one.iloc[i][attributeName])) and 
        if (value_one.iloc[i][attributeName] != ""):
            update_sql = "UPDATE " + tableName + " SET " + attributeName + "_" + value_one.iloc[i][attributeName] + " = 1 where TEV = :TEV and MHO = :MHO and ID_SQ = :ID_SQ"
            cur.execute(update_sql, TEV = TEV, MHO = MHO, ID_SQ = value_one.iloc[i]["ID_SQ"].astype('float64'))
            cur.execute("commit")
            #print(value_one.iloc[i]["ID_SQ"], value_one.iloc[i][attributeName])
            # print(regFurdok.loc[:]["szolgaltatas_tipusok"])
    
    """
    value_none = regKF[(regKF[attributeName].isnull())]
    if not (value_none.empty):
        cur.executemany(output_insert_sql, value_none[["ID_SQ", attributeName]].values.tolist())
        cur.execute("commit")
    """
    
    value_more = regFurdok[(regFurdok[attributeName].str.contains(signal) == True)]
    split_value = value_more[attributeName].str.split(pat = signal, expand = True)

    oszlopok_szama = split_value.shape[1]
    print(f"{attributeName} oszlopainak száma: {oszlopok_szama}")

    for o in range(oszlopok_szama):
        print(o)
        value_more.drop([attributeName], axis = 1, inplace = True)
        value_more[attributeName] = split_value.loc[:, o]
        value_more[attributeName] = value_more[attributeName][(value_more[attributeName].str.contains("None") == False)].apply(lambda x: x.replace("{", ""))
        value_more[attributeName] = value_more[attributeName][(value_more[attributeName].str.contains("None") == False)].apply(lambda x: x.replace("}", ""))
        value_more[attributeName] = value_more[attributeName][(value_more[attributeName].str.contains("None") == False)].apply(lambda x: x.replace(" ", ""))
        #cur.executemany(output_insert_sql, value_more[["ID_SQ", attributeName]][(value_more[attributeName].str.contains("None") == False)].values.tolist())
        
        for i in range(0, len(value_more)):
            #print(value_more.iloc[i][attributeName])
            if not (pd.isna(value_more.iloc[i][attributeName])):
                update_sql = "UPDATE " + tableName + " SET " + attributeName + "_" + value_more.iloc[i][attributeName] + " = 1 where TEV = :TEV and MHO = :MHO and ID_SQ = :ID_SQ"
                cur.execute(update_sql, TEV = TEV, MHO = MHO, ID_SQ = value_more.iloc[i]["ID_SQ"].astype('float64'))
                cur.execute("commit")
    


# In[4]:


def makeInsert(number):
    valuesText = ""
    for row in range(number):
        valuesText = valuesText + ":" + str(row + 1) + ","

    valuesText = valuesText[:-1]

    return valuesText


# In[5]:


pd.options.mode.chained_assignment = None

username = getpass.getuser()
password = getpass.getpass(f"Kérlek, add meg a(z) {username} felhasználói nevedhez tartozó jelszót: ")


# In[6]:


database = oracledb.makedsn(host = "tesztdb.ksh.hu", port = "1522", service_name = "tesztdb.ksh.hu")
conn = oracledb.connect(user = username, password = password, dsn = database)
cur = conn.cursor()


# In[7]:


TEV = "2024"
MHO = "08"
OSAP = "2588"
EXP_DATE = actual_time()


# In[8]:


#Fürdőhelyek regisztrációs adatainak beolvasása Excel állományból
regFurdok = pd.read_excel(io = os.path.abspath(os.getcwd() + "\Excel_Files\Regisztrációs_adatok_KSH_attrakcio_kozfurdo_természetesfurdohely_202406_08ho.xlsx"), sheet_name = "regisztráció", header = 0)
#regFurdok.drop(regFurdok.tail(2).index, inplace = True) #csak júniusra és júliusra kell törölni, mert augusztusban lett két új fürdőhely
print(f"A fürdőhelyek regisztrációs adatait tartalmazó adatkeret sor- és oszlopszámai : {regFurdok.shape}")


# In[9]:


#W_ kezdetű tábla feltöltése
pd.set_option('display.precision', 0)
regFurdok["letrehozva"] = regFurdok["letrehozva"].astype("datetime64[ns]")
#print(regFurdok["letrehozva"].dtypes)
#print(regFurdok.loc[:]["letrehozva"])
regFurdok = regFurdok.replace({pd.NaT: None}).replace({"NaT": None}).replace({np.NaN: None})

regFurdok.insert(loc = 0, column = "MHO", value = MHO)
regFurdok.insert(loc = 0, column = "TEV", value = TEV)


# In[10]:


values = makeInsert(84)

outputName = "GOA24.W_VK_2588_REG_V24H9_V_V00"
attributesForInsert = """TEV, MHO, szolgaltatasi_hely_nev, szolgaltatasi_hely_regisztracios_szam, foszolgaltatas, 
szolgaltatas_tipusok, statusz, letrehozva, szolgaltatasi_hely_iranyitoszam, szolgaltatasi_hely_telepules, 
szolgaltatasi_hely_megye, szolgaltatasi_hely_kiemelt_terseg, szolgaltatasi_hely_kozterulet_neve, 
szolgaltatasi_hely_kozterulet_jellege, szolgaltatasi_hely_hazszam, szolgaltato_nev, szolgaltato_adoszam, 
szolgaltato_vallalkozas_tipus, szolgaltato_statisztikai_tevekenyseg, szolgaltato_iranyitoszam, 
szolgaltato_telepules, arbevetel_ev, arbevetel_osszeg, arbevetel, altalanos_beszeltnyelvek, 
altalanos_feliratoknyelvei, altalanos_helyszinjellege, altalanos_atlagostoltottido_hour, 
altalanos_atlagostoltottido_minute, altalanos_atlagostoltottido_second, altalanos_atlagostoltottido_nano, 
altalanos_latogatokszamarawifi, altalanos_ajandekboltshowvan, altalanos_mobiltelefonosappvan, 
altalanos_turisztikaiinformaciospontvan, altalanos_kotelezoidopontotfoglalni, 
altalanos_szemelyesfoglalaslehetosegek, altalanos_nyitvatartasszezonalitasa, altalanos_vonzeronyitvavan, 
akadalymentesseg_lift, akadalymentesseg_wc, akadalymentesseg_fizikaiakadalymentesites, 
akadalymentesseg_bejaratmegkozelitheto, akadalymentesseg_latasserultekszamara, 
akadalymentesseg_hallasserultekszamara, akadalymentesseg_kiseroszemelyzetrendelkezesreall, gazdasagi_utalvanyok, 
gazdasagi_szepkartyak, gazdasagi_vanbankkartya, gazdasagi_fizetoeszkozok, gazdasagi_viszonteladoiertekesites,  
gazdasagi_jutalekosfizetesirendszer, infrastruktura_latogatowc, infrastruktura_ruhatar, 
infrastruktura_csomagmegorzo, infrastruktura_kerekpartarolo, infrastruktura_parkolo, infrastruktura_buszparkolodb, 
infrastruktura_szemelygepkocsiparkolodb, infrastruktura_elektromosautotoltes, furdokozfurdo_kategoria, 
furdoterulete, zoldteruletnagysaga, elmenyelemekszamaosszesen, medencekszamaosszesen, medencekvizfeluleteosszesen,  
furdomegengedhetonapilegnagyobbterhelese, furdobeepitettosszesvizforgatasikapacitasa,  
furdomegengedettegyidejulegnagyobbterhelese, furdoknemzetitanusitovedjegyevelrendelkezik, 
furdonekszerzodeseskapcsolataegeszsegpenztarral, furdoegysegek, beautyszolgaltatasok, csaladbaratszolgaltatasok, 
egeszsegmegorzoszolgaltatasok, maxbefogadokepesseg, partszakashossza, kekhullamminosites, zuhanylehetoseg,  
mozgaskorlatozottbetudjutniavizbe, vizimentoszolgalat, vizeskapcsolatosuszoda, lehetkolcsonozni, 
kolcsonzesilehetosegek""" 


output_insert_sql = "INSERT INTO " + outputName + "(" + attributesForInsert + ") VALUES(" + values + ")"
cur.executemany(output_insert_sql, regFurdok[["TEV", "MHO", "szolgaltatasi_hely_nev", "szolgaltatasi_hely_regisztracios_szam", 
                                              "foszolgaltatas", "szolgaltatas_tipusok", "statusz", "letrehozva", 
                                              "szolgaltatasi_hely_iranyitoszam", "szolgaltatasi_hely_telepules", 
                                              "szolgaltatasi_hely_megye", "szolgaltatasi_hely_kiemelt_terseg", 
                                              "szolgaltatasi_hely_kozterulet_neve", "szolgaltatasi_hely_kozterulet_jellege", 
                                              "szolgaltatasi_hely_hazszam", "szolgaltato_nev", "szolgaltato_adoszam", 
                                              "szolgaltato_vallalkozas_tipus", "szolgaltato_statisztikai_tevekenyseg", 
                                              "szolgaltato_iranyitoszam", "szolgaltato_telepules", "arbevetel_ev", 
                                              "arbevetel_osszeg", "arbevetel", "altalanos_beszeltnyelvek", 
                                              "altalanos_feliratoknyelvei", "altalanos_helyszinjellege", 
                                              "altalanos_atlagostoltottido_hour", "altalanos_atlagostoltottido_minute", 
                                              "altalanos_atlagostoltottido_second", 
                                              "altalanos_atlagostoltottido_nano", "altalanos_latogatokszamarawifi", 
                                              "altalanos_ajandekboltshowvan", "altalanos_mobiltelefonosappvan", 
                                              "altalanos_turisztikaiinformaciospontvan", "altalanos_kotelezoidopontotfoglalni", 
                                              "altalanos_szemelyesfoglalaslehetosegek", "altalanos_nyitvatartasszezonalitasa", 
                                              "altalanos_vonzeronyitvavan", "akadalymentesseg_lift", "akadalymentesseg_wc", 
                                              "akadalymentesseg_fizikaiakadalymentesites", "akadalymentesseg_bejaratmegkozelitheto", 
                                              "akadalymentesseg_latasserultekszamara", "akadalymentesseg_hallasserultekszamara", 
                                              "akadalymentesseg_kiseroszemelyzetrendelkezesreall", "gazdasagi_utalvanyok", 
                                              "gazdasagi_szepkartyak", "gazdasagi_vanbankkartya", "gazdasagi_fizetoeszkozok", 
                                              "gazdasagi_viszonteladoiertekesites", "gazdasagi_jutalekosfizetesirendszer", 
                                              "infrastruktura_latogatowc", "infrastruktura_ruhatar", "infrastruktura_csomagmegorzo", 
                                              "infrastruktura_kerekpartarolo", "infrastruktura_parkolo", "infrastruktura_buszparkolodb", 
                                              "infrastruktura_szemelygepkocsiparkolodb", "infrastruktura_elektromosautotoltes", 
                                              "furdokozfurdo_kategoria", "furdoterulete", "zoldteruletnagysaga", "elmenyelemekszamaosszesen", 
                                              "medencekszamaosszesen", "medencekvizfeluleteosszesen", "furdomegengedhetonapilegnagyobbterhelese", 
                                              "furdobeepitettosszesvizforgatasikapacitasa", "furdomegengedettegyidejulegnagyobbterhelese", 
                                              "furdoknemzetitanusitovedjegyevelrendelkezik", "furdonekszerzodeseskapcsolataegeszsegpenztarral", 
                                              "furdoegysegek", "beautyszolgaltatasok", "csaladbaratszolgaltatasok", 
                                              "egeszsegmegorzoszolgaltatasok", "maxbefogadokepesseg", "partszakashossza", "kekhullamminosites", 
                                              "zuhanylehetoseg", "mozgaskorlatozottbetudjutniavizbe", "vizimentoszolgalat", 
                                              "vizeskapcsolatosuszoda", "lehetkolcsonozni", "kolcsonzesilehetosegek" ]].values.tolist())

cur.execute("commit")


# In[10]:


#Nem W_ kezdetű regisztrációs tábla feltöltése
regFurdok.insert(loc = 0, column = "MC01", value = OSAP)
regFurdok.insert(loc = 0, column = "EXP_DATE", value = EXP_DATE)
regFurdok["EXP_DATE"] = regFurdok["EXP_DATE"].astype("datetime64[ns]")


# In[12]:


values = makeInsert(65)

outputName = "GOA24.VK_2588_REG_V24H9_V_V00"
attributesForInsert = """TEV, MHO, MC01, szolgaltatasi_hely_nev, szolgaltatasi_hely_regisztracios_szam, 
foszolgaltatas, statusz, letrehozva, szolgaltatasi_hely_iranyitoszam, 
szolgaltatasi_hely_telepules, szolgaltatasi_hely_megye, szolgaltatasi_hely_kiemelt_terseg, 
szolgaltatasi_hely_kozterulet_neve, szolgaltatasi_hely_kozterulet_jellege, szolgaltatasi_hely_hazszam, 
szolgaltato_nev, szolgaltato_adoszam, szolgaltato_vallalkozas_tipus, szolgaltato_statisztikai_tevekenyseg, 
szolgaltato_iranyitoszam, szolgaltato_telepules, arbevetel_ev, arbevetel_osszeg, arbevetel, 
altalanos_atlagostoltottido_hour, altalanos_atlagostoltottido_minute, altalanos_atlagostoltottido_second, 
altalanos_atlagostoltottido_nano, altalanos_latogatokszamarawifi, altalanos_ajandekboltshowvan, 
altalanos_mobiltelefonosappvan, altalanos_turisztikaiinformaciospontvan, altalanos_kotelezoidopontotfoglalni, 
altalanos_nyitvatartasszezonalitasa, akadalymentesseg_lift, akadalymentesseg_wc, 
akadalymentesseg_fizikaiakadalymentesites, akadalymentesseg_bejaratmegkozelitheto, 
akadalymentesseg_latasserultekszamara, akadalymentesseg_hallasserultekszamara, 
akadalymentesseg_kiseroszemelyzetrendelkezesreall, gazdasagi_vanbankkartya, gazdasagi_fizetoeszkozok, 
gazdasagi_viszonteladoiertekesites, gazdasagi_jutalekosfizetesirendszer, infrastruktura_latogatowc, 
infrastruktura_ruhatar, infrastruktura_csomagmegorzo, infrastruktura_kerekpartarolo, infrastruktura_parkolo, 
infrastruktura_buszparkolodb, infrastruktura_szemelygepkocsiparkolodb, infrastruktura_elektromosautotoltes, 
furdokozfurdo_kategoria, furdoterulete, zoldteruletnagysaga, elmenyelemekszamaosszesen, medencekszamaosszesen, 
medencekvizfeluleteosszesen, furdomegengedhetonapilegnagyobbterhelese, furdobeepitettosszesvizforgatasikapacitasa, 
furdomegengedettegyidejulegnagyobbterhelese, furdoknemzetitanusitovedjegyevelrendelkezik, 
furdonekszerzodeseskapcsolataegeszsegpenztarral, EXP_DATE""" 

output_insert_sql = "INSERT INTO " + outputName + "(" + attributesForInsert + ") VALUES(" + values + ")"
cur.executemany(output_insert_sql, 
                regFurdok[["TEV", "MHO", "MC01", "szolgaltatasi_hely_nev", 
                       "szolgaltatasi_hely_regisztracios_szam", "foszolgaltatas", "statusz", 
                       "letrehozva", "szolgaltatasi_hely_iranyitoszam", 
                       "szolgaltatasi_hely_telepules", "szolgaltatasi_hely_megye", 
                       "szolgaltatasi_hely_kiemelt_terseg", "szolgaltatasi_hely_kozterulet_neve", 
                       "szolgaltatasi_hely_kozterulet_jellege", "szolgaltatasi_hely_hazszam", 
                       "szolgaltato_nev", "szolgaltato_adoszam", "szolgaltato_vallalkozas_tipus", 
                       "szolgaltato_statisztikai_tevekenyseg", "szolgaltato_iranyitoszam", 
                       "szolgaltato_telepules", "arbevetel_ev", "arbevetel_osszeg", "arbevetel", 
                       "altalanos_atlagostoltottido_hour", "altalanos_atlagostoltottido_minute", 
                       "altalanos_atlagostoltottido_second", "altalanos_atlagostoltottido_nano", 
                       "altalanos_latogatokszamarawifi", "altalanos_ajandekboltshowvan", 
                       "altalanos_mobiltelefonosappvan", "altalanos_turisztikaiinformaciospontvan", 
                       "altalanos_kotelezoidopontotfoglalni", "altalanos_nyitvatartasszezonalitasa", 
                       "akadalymentesseg_lift", "akadalymentesseg_wc", 
                       "akadalymentesseg_fizikaiakadalymentesites", "akadalymentesseg_bejaratmegkozelitheto", 
                       "akadalymentesseg_latasserultekszamara", "akadalymentesseg_hallasserultekszamara", 
                       "akadalymentesseg_kiseroszemelyzetrendelkezesreall", "gazdasagi_vanbankkartya", 
                       "gazdasagi_fizetoeszkozok", "gazdasagi_viszonteladoiertekesites", 
                       "gazdasagi_jutalekosfizetesirendszer", "infrastruktura_latogatowc", 
                       "infrastruktura_ruhatar", "infrastruktura_csomagmegorzo", 
                       "infrastruktura_kerekpartarolo", "infrastruktura_parkolo", 
                       "infrastruktura_buszparkolodb", "infrastruktura_szemelygepkocsiparkolodb", 
                       "infrastruktura_elektromosautotoltes", "furdokozfurdo_kategoria", "furdoterulete", 
                       "zoldteruletnagysaga", "elmenyelemekszamaosszesen", "medencekszamaosszesen", 
                       "medencekvizfeluleteosszesen", "furdomegengedhetonapilegnagyobbterhelese", 
                       "furdobeepitettosszesvizforgatasikapacitasa", 
                       "furdomegengedettegyidejulegnagyobbterhelese", 
                       "furdoknemzetitanusitovedjegyevelrendelkezik", 
                       "furdonekszerzodeseskapcsolataegeszsegpenztarral", "EXP_DATE"]].values.tolist())
cur.execute("commit")


# In[11]:


select_ID_SQ = "SELECT ID_SQ FROM GOA24.VK_2588_REG_V24H9_V_V00 where TEV = :TEV and MHO = :MHO order by ID_SQ"
#and szolgaltatasi_hely_regisztracios_szam like 'KF%' 
cur.execute(select_ID_SQ, TEV = TEV, MHO = MHO)
ID_SQ_Values = cur.fetchall()
ID_SQ_df = pd.DataFrame(ID_SQ_Values, columns = ["ID_SQ"])
#print(ID_SQ_df.loc[0])
regFurdok.insert(loc = 0, column = "ID_SQ", value = ID_SQ_df)


# In[12]:


regUpdate("GOA24.VK_2588_REG_V24H9_V_V00", "szolgaltatas_tipusok", ";")#szolgaltatas_tipusok
regUpdate("GOA24.VK_2588_REG_V24H9_V_V00", "altalanos_beszeltnyelvek", ",")#altalanos_beszeltnyelvek 
regUpdate("GOA24.VK_2588_REG_V24H9_V_V00", "altalanos_feliratoknyelvei", ",")#altalanos_feliratoknyelvei
regUpdate("GOA24.VK_2588_REG_V24H9_V_V00", "altalanos_helyszinjellege", ",")#altalanos_helyszinjellege
regUpdate("GOA24.VK_2588_REG_V24H9_V_V00", "altalanos_szemelyesfoglalaslehetosegek", ",")#altalanos_szemelyesfoglalaslehetosegek
regUpdate("GOA24.VK_2588_REG_V24H9_V_V00", "altalanos_vonzeronyitvavan", ",")#altalanos_vonzeronyitvavan
regUpdate("GOA24.VK_2588_REG_V24H9_V_V00", "gazdasagi_utalvanyok", ",")#gazdasagi_utalvanyok
regUpdate("GOA24.VK_2588_REG_V24H9_V_V00", "gazdasagi_szepkartyak", ",")#gazdasagi_szepkartyak
regUpdate("GOA24.VK_2588_REG_V24H9_V_V00", "furdoegysegek", ",")#furdoegysegek
regUpdate("GOA24.VK_2588_REG_V24H9_V_V00", "beautyszolgaltatasok", ",")#beautyszolgaltatasok
regUpdate("GOA24.VK_2588_REG_V24H9_V_V00", "csaladbaratszolgaltatasok", ",")#csaladbaratszolgaltatasok
regUpdate("GOA24.VK_2588_REG_V24H9_V_V00", "egeszsegmegorzoszolgaltatasok", ",")#egeszsegmegorzoszolgaltatasok
regUpdate("GOA24.VK_2588_REG_V24H9_V_V00", "zuhanylehetosegek", ",")#zuhanylehetosegek
regUpdate("GOA24.VK_2588_REG_V24H9_V_V00", "kolcsonzesilehetosegek", ",")#kolcsonzesilehetosegek


# In[20]:


#Tranzakciós adatok fürdőhelyek
kozFurdok = pd.read_excel(io = os.path.abspath(os.getcwd() + "\Excel_Files\Regisztrációs_adatok_KSH_attrakcio_kozfurdo_természetesfurdohely_202406_08ho.xlsx"), sheet_name = "közfürdő", header = 0)
print(f"A közfürdők sor- és oszlopszámai : {kozFurdok.shape}")

termeszetesFurdok = pd.read_excel(io = os.path.abspath(os.getcwd() + "\Excel_Files\Regisztrációs_adatok_KSH_attrakcio_kozfurdo_természetesfurdohely_202406_08ho.xlsx"), sheet_name = "természetes fürdőhely", header = 0)
print(f"A természetes fürdőhelyek sor- és oszlopszámai : {termeszetesFurdok.shape}")

tranzFurdok = pd.concat([kozFurdok, termeszetesFurdok], ignore_index = True)
tranzFurdok = tranzFurdok.replace({pd.NaT: None}).replace({"NaT": None}).replace({np.NaN: None})

tranzFurdok = tranzFurdok[tranzFurdok.honap == 8]#június, július vagy augusztus


# In[21]:


#W_ kezdetű tranzakciós tábla feltöltése
values = makeInsert(44)

outputName = "GOA24.W_VK_2588_TRANZ_V24H9_V_V00"
attributesForInsert = """evszam, honap, szolg_hely_regisztracios_szam, afa_kategoria, azonnal_felhasznalt, 
egyeb_etel, egyeb_ital, egyeb_kedvezmeny, egyeb_szolgaltatas, egyeb_termek, ertekesitesi_csatorna, ertekesitve, 
fizetes_atutalas, fizetes_bankkartya, fizetes_egyeb, fizetes_kerekites, fizetes_keszpenzeur, fizetes_keszpenzhuf, 
fizetes_szepkartya, fizetes_szobahitel, fizetes_voucher, helyszin, jegyek_szama, jegy_megnevezes, 
jegy_ervenyesseg_tipusa, kedvezmenyek, korcsoport, ntak_rendszer_kategoria, szemelyek_szama, kulfoldi, 
latogatok_lakohelye, program_alkategoria, program_fokategoria, program_gyakorisaga, program_neve, 
program_tipusa, programsorozat_neve, online_program, szolgaltatasihely_nev, szolgaltatasihely_varos, 
szolgaltatasihely_megye, szolgaltatasihely_kiemelt_terseg, szolgaltato_nev, tranzakciok_szama"""


output_insert_sql = "INSERT INTO " + outputName + "(" + attributesForInsert + ") VALUES(" + values + ")"
cur.executemany(output_insert_sql, 
                tranzFurdok[["evszam", "honap", "szolg_hely_regisztracios_szam", "afa_kategoria", 
                            "azonnal_felhasznalt", "egyeb_etel", "egyeb_ital", "egyeb_kedvezmeny", 
                            "egyeb_szolgaltatas", "egyeb_termek", "ertekesitesi_csatorna", "ertekesitve",
                             "fizetes_atutalas", "fizetes_bankkartya", "fizetes_egyeb", "fizetes_kerekites", 
                            "fizetes_keszpenzeur", "fizetes_keszpenzhuf", "fizetes_szepkartya", "fizetes_szobahitel", 
                            "fizetes_voucher", "helyszin", "jegyek_szama", "jegy_megnevezes", 
                            "jegy_ervenyesseg_tipusa", "kedvezmenyek", "korcsoport", "ntak_rendszer_kategoria", 
                            "szemelyek_szama", "kulfoldi", "latogatok_lakohelye", "program_alkategoria", 
                            "program_fokategoria", "program_gyakorisaga", "program_neve", "program_tipusa", 
                            "programsorozat_neve", "online_program", "szolgaltatasihely_nev", 
                            "szolgaltatasihely_varos", "szolgaltatasihely_megye", "szolgaltatasihely_kiemelt_terseg", 
                            "szolgaltato_nev", "tranzakciok_szama"
                            ]].values.tolist())
cur.execute("commit")


# In[22]:


#Nem W_ kezdetű tranzakciós tábla feltöltése
tranzFurdok.insert(loc = 0, column = "EXP_DATE", value = EXP_DATE)
tranzFurdok["EXP_DATE"] = tranzFurdok["EXP_DATE"].astype("datetime64[ns]")

tranzFurdok.drop(["szolg_hely_program_azonosito", "helyszin", "szolgaltatasihely_nev", "szolgaltatasihely_varos", "szolgaltatasihely_megye", "szolgaltatasihely_kiemelt_terseg", "szolgaltato_nev"], axis = 1, inplace = True)#7 oszlop törlése
print(f"A tranzakciós adatok oszloptörlés utáni sor- és oszlopszámai : {tranzFurdok.shape}")

tranzFurdok.insert(loc = 0, column = "REGKGYFURDO_ID", value = 0)


# In[23]:


tranzFurdok.rename(columns = {'evszam': 'TEV', 'honap': 'MHO'}, inplace = True)
tranzFurdok['MHO'] = tranzFurdok['MHO'].astype(str)
tranzFurdok.MHO = tranzFurdok.MHO.str.rjust(2, '0')


# In[24]:


for i in range(regFurdok.shape[0]):
    #print(i)
    regszam = regFurdok.loc[i]["szolgaltatasi_hely_regisztracios_szam"]
    ertek = regFurdok.loc[i]["ID_SQ"]
    #print(regszam)
    #print(ertek)
    #result["REGKGYFURDO_ID"] = np.where(result['szolg_hely_regisztracios_szam'] == regszam, ertek, 0)
    #result["REGKGYFURDO_ID"] = result["szolg_hely_regisztracios_szam"].where(result["szolg_hely_regisztracios_szam"] == regszam, ertek)
    tranzFurdok.loc[tranzFurdok["szolg_hely_regisztracios_szam"] == regszam, "REGKGYFURDO_ID"] = ertek


# In[25]:


values = makeInsert(40)

outputName = "GOA24.VK_2588_TRANZ_V24H9_V_V00"
attributesForInsert = """REGKGYFURDO_ID, TEV, MHO, szolg_hely_regisztracios_szam, afa_kategoria, 
azonnal_felhasznalt, egyeb_etel, egyeb_ital, egyeb_kedvezmeny, egyeb_szolgaltatas, egyeb_termek, 
ertekesitesi_csatorna, ertekesitve, fizetes_atutalas, fizetes_bankkartya, fizetes_egyeb, fizetes_kerekites, 
fizetes_keszpenzeur, fizetes_keszpenzhuf, fizetes_szepkartya, fizetes_szobahitel, fizetes_voucher, 
jegyek_szama, jegy_megnevezes, jegy_ervenyesseg_tipusa, kedvezmenyek, korcsoport, ntak_rendszer_kategoria, szemelyek_szama, 
kulfoldi, latogatok_lakohelye, program_alkategoria, program_fokategoria, program_gyakorisaga, 
program_neve, program_tipusa, programsorozat_neve, online_program, tranzakciok_szama, EXP_DATE"""

output_insert_sql = "INSERT INTO " + outputName + "(" + attributesForInsert + ") VALUES(" + values + ")"
cur.executemany(output_insert_sql, 
                tranzFurdok[["REGKGYFURDO_ID", "TEV", "MHO", "szolg_hely_regisztracios_szam", "afa_kategoria", "azonnal_felhasznalt", 
                        "egyeb_etel", "egyeb_ital", "egyeb_kedvezmeny", "egyeb_szolgaltatas", "egyeb_termek", 
                        "ertekesitesi_csatorna", "ertekesitve", "fizetes_atutalas", "fizetes_bankkartya", 
                        "fizetes_egyeb", "fizetes_kerekites", "fizetes_keszpenzeur", "fizetes_keszpenzhuf", 
                        "fizetes_szepkartya", "fizetes_szobahitel", "fizetes_voucher", "jegyek_szama", 
                        "jegy_megnevezes", "jegy_ervenyesseg_tipusa", "kedvezmenyek", "korcsoport", "ntak_rendszer_kategoria", 
                        "szemelyek_szama", "kulfoldi", "latogatok_lakohelye", "program_alkategoria", 
                        "program_fokategoria", "program_gyakorisaga", "program_neve", "program_tipusa", 
                        "programsorozat_neve", "online_program", "tranzakciok_szama", "EXP_DATE"]].values.tolist())

cur.execute("commit")


# In[26]:


cur.close()


# In[ ]:




