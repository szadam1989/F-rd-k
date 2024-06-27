import pandas as pd
import os
import oracledb
import getpass
import numpy as np
import datetime

def actual_time():
    f_now = datetime.datetime.now().strftime("%Y.%m.%d. %H:%M:%S")
    return f_now
    
def regkfSidetable(tableName, attributeName, insertValues, signal):
    outputName = tableName 
    attributesForInsert = f"TEV, MHO, szolgaltatasi_hely_regisztracios_szam, {attributeName}, EXP_DATE" 
    values = insertValues
    
    output_insert_sql = "INSERT INTO " + outputName + "(" + attributesForInsert + ") VALUES(" + values + ")"
    value_one = regKF[(regKF[attributeName].str.contains(signal) == False)]
    value_one[attributeName] = value_one[attributeName].apply(lambda x: x.replace("{", ""))
    value_one[attributeName] = value_one[attributeName].apply(lambda x: x.replace("}", ""))
    cur.executemany(output_insert_sql, value_one[["TEV", "MHO",  "szolgaltatasi_hely_regisztracios_szam", attributeName, "EXP_DATE"]].values.tolist())
    cur.execute("commit")
    
    value_none = regKF[(regKF[attributeName].isnull())]
    if not (value_none.empty):
        cur.executemany(output_insert_sql, value_none[["TEV", "MHO",  "szolgaltatasi_hely_regisztracios_szam", attributeName, "EXP_DATE"]].values.tolist())
        cur.execute("commit")

    value_more = regKF[(regKF[attributeName].str.contains(signal) == True)]
    split_value = value_more[attributeName].str.split(pat = signal, expand = True)

    oszlopok_szama = split_value.shape[1]
    print(f"{attributeName} oszlopainak száma: {oszlopok_szama}")

    for i in range(oszlopok_szama):
        print(i)
        value_more.drop([attributeName], axis = 1, inplace = True)
        value_more[attributeName] = split_value.loc[:, i]
        value_more[attributeName] = value_more[attributeName][(value_more[attributeName].str.contains("None") == False)].apply(lambda x: x.replace("{", ""))
        value_more[attributeName] = value_more[attributeName][(value_more[attributeName].str.contains("None") == False)].apply(lambda x: x.replace("}", ""))
        value_more[attributeName] = value_more[attributeName][(value_more[attributeName].str.contains("None") == False)].apply(lambda x: x.replace(" ", ""))
        cur.executemany(output_insert_sql, value_more[["TEV", "MHO",  "szolgaltatasi_hely_regisztracios_szam", attributeName, "EXP_DATE"]][(value_more[attributeName].str.contains("None") == False)].values.tolist())
        cur.execute("commit")

def makeInsert(number):
    valuesText = ""
    for row in range(number):
        valuesText = valuesText + ":" + str(row + 1) + ","

    valuesText = valuesText[:-1]

    return valuesText

pd.options.mode.chained_assignment = None

inputData1 = pd.read_excel(io = os.path.abspath(os.getcwd() + "\Excel_Files\969_KSH_attrakcio_minta_pinot_kozfurdok_20240415_0430.xlsx"), sheet_name = "Adatok", header = 0)
#inputData1 = pd.read_excel(io = os.path.abspath(os.getcwd() + "\Excel_Files\969_KSH_attrakcio_minta_pinot_kozfurdok_20240501_0515.xlsx"), sheet_name = "Munka1", header = 0)
print(f"Az adatkeret sor- és oszlopszámai : {inputData1.shape}")

#result = pd.concat([inputData1, inputData2], ignore_index = True)
result = inputData1
result.drop(["helyszin", "szolgaltatasihely_nev", "szolgaltatasihely_varos", "szolgaltatasihely_megye", "szolgaltatasihely_kiemelt_terseg", "szolgaltato_nev"], axis = 1, inplace = True)#6 oszlop törlése
print(f"Az adatkeret oszloptörlés utáni sor- és oszlopszámai : {result.shape}")

result["kedvezmenyek"] = result["kedvezmenyek"].str.replace("[", "").str.replace("]", "").str.replace("'", "")
result = result.replace({pd.NaT: None}).replace({"NaT": None}).replace({np.NaN: None})

username = getpass.getuser()
password = getpass.getpass(f"Kérlek, add meg a(z) {username} felhasználói nevedhez tartozó jelszót: ")

database = oracledb.makedsn(host = "tesztdb.ksh.hu", port = "1522", service_name = "tesztdb.ksh.hu")
conn = oracledb.connect(user = username, password = password, dsn = database)
cur = conn.cursor()

TEV = "2024"
MHO = "04"
#MHO = "05"
OSAP = "OSAP"
EXP_DATE = actual_time()

result.insert(loc = 0, column = "EXP_DATE", value = EXP_DATE)
result.insert(loc = 0, column = "MC01", value = OSAP)
result.insert(loc = 0, column = "MHO", value = MHO)
result.insert(loc = 0, column = "TEV", value = TEV)
result["EXP_DATE"] = result["EXP_DATE"].astype("datetime64[ns]")

values = makeInsert(27)

outputName = "GOA24.W_VK_OSAP_TRANZ_V24H9_V_V00"
attributesForInsert = "TEV, MHO, MC01, szolg_hely_regisztracios_szam, idopont, afa_kategoria, azonnal_felhasznalt, ertekesitesi_csatorna, jegyek_szama, jegy_megnevezes, jegy_ervenyesseg_tipusa, korcsoport, ntak_rendszer_kategoria, szemelyek_szama, kulfoldi, latogatok_lakohelye, program_alkategoria, program_fokategoria, program_gyakorisaga, program_kezdete, program_vege, program_neve, program_tipusa, programsorozat_neve, online_program, tranzakciok_szama, EXP_DATE" 
output_insert_sql = "INSERT INTO " + outputName + "(" + attributesForInsert + ") VALUES(" + values + ")"
cur.executemany(output_insert_sql, result[["TEV", "MHO", "MC01", "szolg_hely_regisztracios_szam", "idopont", "afa_kategoria", "azonnal_felhasznalt", "ertekesitesi_csatorna", "jegyek_szama", "jegy_megnevezes", "jegy_ervenyesseg_tipusa", "korcsoport", "ntak_rendszer_kategoria", "szemelyek_szama", "kulfoldi", "latogatok_lakohelye", "program_alkategoria", "program_fokategoria", "program_gyakorisaga", "program_kezdete", "program_vege", "program_neve", "program_tipusa", "programsorozat_neve", "online_program", "tranzakciok_szama", "EXP_DATE"]].values.tolist())
cur.execute("commit")

select_ID_SQ = "SELECT ID_SQ FROM GOA24.W_VK_OSAP_TRANZ_V24H9_V_V00 where TEV = :TEV and MHO = :MHO order by ID_SQ"
cur.execute(select_ID_SQ, TEV = TEV, MHO = MHO)
ID_SQ_Values = cur.fetchall()
ID_SQ_df = pd.DataFrame(ID_SQ_Values, columns = ["ID_SQ"])
#print(ID_SQ_df.loc[0])
result.insert(loc = 0, column = "ID_SQ", value = ID_SQ_df)

values = makeInsert(10)

outputName = "GOA24.W_VK_OSAP_TRANZVASAR_V24H9_V_V00"
attributesForInsert = "ID_SQ, TEV, MHO, egyeb_etel, egyeb_ital, egyeb_kedvezmeny, egyeb_szolgaltatas, egyeb_termek, ertekesitve, EXP_DATE" 
output_insert_sql = "INSERT INTO " + outputName + "(" + attributesForInsert + ") VALUES(" + values + ")"
cur.executemany(output_insert_sql, result[["ID_SQ", "TEV", "MHO",  "egyeb_etel", "egyeb_ital", "egyeb_kedvezmeny", "egyeb_szolgaltatas", "egyeb_termek", "ertekesitve", "EXP_DATE"]].values.tolist())
cur.execute("commit")

values = makeInsert(13)

outputName = "GOA24.W_VK_OSAP_TRANZFIZET_V24H9_V_V00"
attributesForInsert = "ID_SQ, TEV, MHO, fizetes_atutalas, fizetes_bankkartya, fizetes_egyeb, fizetes_kerekites, fizetes_keszpenzeur, fizetes_keszpenzhuf, fizetes_szepkartya, fizetes_szobahitel, fizetes_voucher, EXP_DATE" 
output_insert_sql = "INSERT INTO " + outputName + "(" + attributesForInsert + ") VALUES(" + values + ")"
cur.executemany(output_insert_sql, result[["ID_SQ", "TEV", "MHO",  "fizetes_atutalas", "fizetes_bankkartya", "fizetes_egyeb", "fizetes_kerekites", "fizetes_keszpenzeur", "fizetes_keszpenzhuf", "fizetes_szepkartya", "fizetes_szobahitel", "fizetes_voucher", "EXP_DATE"]].values.tolist())
cur.execute("commit")

values = makeInsert(5)

outputName = "GOA24.W_VK_OSAP_TRANZKEDV_V24H9_V_V00"
attributesForInsert = "ID_SQ, TEV, MHO, kedvezmenyek, EXP_DATE" 
output_insert_sql = "INSERT INTO " + outputName + "(" + attributesForInsert + ") VALUES(" + values + ")"
kedv_one = result[(result["kedvezmenyek"].str.contains("null") == False) & (result["kedvezmenyek"].str.contains(",") == False)]
kedv_one["kedvezmenyek"] = kedv_one["kedvezmenyek"].replace('"', '', regex = True)
cur.executemany(output_insert_sql, kedv_one[["ID_SQ", "TEV", "MHO",  "kedvezmenyek", "EXP_DATE"]].values.tolist())
cur.execute("commit")

kedv_none = result[(result["kedvezmenyek"].str.contains("null") == True)]
kedv_none["kedvezmenyek"] = np.NaN
kedv_none["kedvezmenyek"] = kedv_none["kedvezmenyek"].replace({pd.NaT: None}).replace({"NaT": None}).replace({np.NaN: None})
cur.executemany(output_insert_sql, kedv_none[["ID_SQ", "TEV", "MHO",  "kedvezmenyek", "EXP_DATE"]].values.tolist())
cur.execute("commit")

kedv_more = result[(result["kedvezmenyek"].str.contains(",") == True)]
values_kedv = kedv_more["kedvezmenyek"].str.split(pat = ",", expand = True)

print(f"Kedvezmények oszlopainak száma: {values_kedv.shape[1]}")
oszlopok_szama = values_kedv.shape[1]

for i in range(oszlopok_szama):
    print(i)
    kedv_more.drop(["kedvezmenyek"], axis = 1, inplace = True)
    kedv_more["kedvezmenyek"] = values_kedv.loc[:, i]
    kedv_more["kedvezmenyek"][(kedv_more["kedvezmenyek"].str.contains("None") == False)] = kedv_more["kedvezmenyek"][(kedv_more["kedvezmenyek"].str.contains("None") == False)].replace('"', '', regex = True)
    kedv_more["kedvezmenyek"][(kedv_more["kedvezmenyek"].str.contains("None") == False)] = kedv_more["kedvezmenyek"][(kedv_more["kedvezmenyek"].str.contains("None") == False)].apply(lambda x: x.replace(" ", ""))
    cur.executemany(output_insert_sql, kedv_more[["ID_SQ", "TEV", "MHO", "kedvezmenyek", "EXP_DATE"]][(kedv_more["kedvezmenyek"].str.contains("None") == False)].values.tolist())
    cur.execute("commit")


#Közfürdők regisztrációs adatainak beolvasása Excel állományból
regKF = pd.read_excel(io = os.path.abspath(os.getcwd() + "\Excel_Files\Közfürdő_természetes fürdőhely_20240418_KSHatadas.xlsx"), sheet_name = "közfürdő, gyógyfürdő", header = 0)
print(f"A közfürdők regisztrációs adatait tartalmazó adatkeret sor- és oszlopszámai : {regKF.shape}")

values = makeInsert(66)

regKF.insert(loc = 0, column = "EXP_DATE", value = EXP_DATE)
regKF.insert(loc = 0, column = "MC01", value = OSAP)
regKF.insert(loc = 0, column = "MHO", value = MHO)
regKF.insert(loc = 0, column = "TEV", value = TEV)
regKF["EXP_DATE"] = regKF["EXP_DATE"].astype("datetime64[ns]")

regKF["tss_utolso_adatkuldes"] = regKF["tss_utolso_adatkuldes"].astype("datetime64[ns]")
#print(regKF["tss_utolso_adatkuldes"].dtypes)
#print(regKF["letrehozva"].dtypes)
regKF = regKF.replace({pd.NaT: None}).replace({"NaT": None}).replace({np.NaN: None})
#print(regKF.loc[:]["tss_utolso_adatkuldes"])

outputName = "GOA24.W_VK_OSAP_REGKF_V24H9_V_V00"
attributesForInsert = "TEV, MHO, MC01, szolgaltatasi_hely_nev, szolgaltatasi_hely_regisztracios_szam, foszolgaltatas, statusz, tss_utolso_adatkuldes, letrehozva, szolgaltatasi_hely_iranyitoszam, szolgaltatasi_hely_telepules, szolgaltatasi_hely_megye, szolgaltatasi_hely_kiemelt_terseg, szolgaltatasi_hely_kozterulet_neve, szolgaltatasi_hely_kozterulet_jellege, szolgaltatasi_hely_hazszam, szolgaltato_nev, szolgaltato_adoszam, szolgaltato_vallakozas_tipus, szolgaltato_statisztikai_tevekenyseg, szolgaltato_iranyitoszam, szolgaltato_telepules, arbevetel_ev, arbevetel_osszeg, arbevetel, altalanos_atlagostoltottido_hour, altalanos_atlagostoltottido_minute, altalanos_atlagostoltottido_second, altalanos_atlagostoltottido_nano, altalanos_latogatokszamarawifi, altalanos_ajandekboltshowvan, altalanos_mobiltelefonosappvan, altalanos_turisztikaiinformaciospontvan, altalanos_kotelezoidopontotfoglalni, altalanos_nyitvatartasszezonalitasa, akadalymentesseg_lift, akadalymentesseg_wc, akadalymentesseg_fizikaiakadalymentesites, akadalymentesseg_bejaratmegkozelitheto, akadalymentesseg_latasserultekszamara, akadalymentesseg_hallasserultekszamara, akadalymentesseg_kiseroszemelyzetrendelkezesreall, gazdasagi_vanbankkartya, gazdasagi_fizetoeszkozok, gazdasagi_viszonteladoiertekesites, gazdasagi_jutalekosfizetesirendszer, infrastruktura_latogatowc, infrastruktura_ruhatar, infrastruktura_csomagmegorzo, infrastruktura_kerekpartarolo, infrastruktura_parkolo, infrastruktura_buszparkolodb, infrastruktura_szemelygepkocsiparkolodb, infrastruktura_elektromosautotoltes, furdokozfurdo_kategoria, furdoterulete, zoldteruletnagysaga, elmenyelemekszamaosszesen, medencekszamaosszesen, medencekvizfeluleteosszesen, furdomegengedhetonapilegnagyobbterhelese, furdobeepitettosszesvizforgatasikapacitasa, furdomegengedettegyidejulegnagyobbterhelese, furdoknemzetitanusitovedjegyevelrendelkezik, furdonekszerzodeseskapcsolataegeszsegpenztarral, EXP_DATE" 
output_insert_sql = "INSERT INTO " + outputName + "(" + attributesForInsert + ") VALUES(" + values + ")"
cur.executemany(output_insert_sql, regKF[["TEV", "MHO", "MC01", "szolgaltatasi_hely_nev", "szolgaltatasi_hely_regisztracios_szam", "foszolgaltatas", "statusz", "tss_utolso_adatkuldes", "letrehozva", "szolgaltatasi_hely_iranyitoszam", "szolgaltatasi_hely_telepules", "szolgaltatasi_hely_megye", "szolgaltatasi_hely_kiemelt_terseg", "szolgaltatasi_hely_kozterulet_neve", "szolgaltatasi_hely_kozterulet_jellege", "szolgaltatasi_hely_hazszam", "szolgaltato_nev", "szolgaltato_adoszam", "szolgaltato_vallakozas_tipus", "szolgaltato_statisztikai_tevekenyseg", "szolgaltato_iranyitoszam", "szolgaltato_telepules", "arbevetel_ev", "arbevetel_osszeg", "arbevetel", "altalanos_atlagostoltottido_hour", "altalanos_atlagostoltottido_minute", "altalanos_atlagostoltottido_second", "altalanos_atlagostoltottido_nano", "altalanos_latogatokszamarawifi", "altalanos_ajandekboltshowvan", "altalanos_mobiltelefonosappvan", "altalanos_turisztikaiinformaciospontvan", "altalanos_kotelezoidopontotfoglalni", "altalanos_nyitvatartasszezonalitasa", "akadalymentesseg_lift", "akadalymentesseg_wc", "akadalymentesseg_fizikaiakadalymentesites", "akadalymentesseg_bejaratmegkozelitheto", "akadalymentesseg_latasserultekszamara", "akadalymentesseg_hallasserultekszamara", "akadalymentesseg_kiseroszemelyzetrendelkezesreall", "gazdasagi_vanbankkartya", "gazdasagi_fizetoeszkozok", "gazdasagi_viszonteladoiertekesites", "gazdasagi_jutalekosfizetesirendszer", "infrastruktura_latogatowc", "infrastruktura_ruhatar", "infrastruktura_csomagmegorzo", "infrastruktura_kerekpartarolo", "infrastruktura_parkolo", "infrastruktura_buszparkolodb", "infrastruktura_szemelygepkocsiparkolodb", "infrastruktura_elektromosautotoltes", "furdokozfurdo_kategoria", "furdoterulete", "zoldteruletnagysaga", "elmenyelemekszamaosszesen", "medencekszamaosszesen", "medencekvizfeluleteosszesen", "furdomegengedhetonapilegnagyobbterhelese", "furdobeepitettosszesvizforgatasikapacitasa", "furdomegengedettegyidejulegnagyobbterhelese", "furdoknemzetitanusitovedjegyevelrendelkezik", "furdonekszerzodeseskapcsolataegeszsegpenztarral", "EXP_DATE"]].values.tolist())
cur.execute("commit")

values = makeInsert(5)

regkfSidetable("GOA24.W_VK_OSAP_REGKFSZOLGTIP_V24H9_V_V00", "szolgaltatas_tipusok", values, ";")#szolgaltatas_tipusok  
regkfSidetable("GOA24.W_VK_OSAP_REGKFTSSREND_V24H9_V_V00", "tss_rendszerek", values, ";")#tss_rendszerek    
regkfSidetable("GOA24.W_VK_OSAP_REGKFBESZNYELV_V24H9_V_V00", "altalanos_beszeltnyelvek", values, ",")#altalanos_beszeltnyelvek    
regkfSidetable("GOA24.W_VK_OSAP_REGKFFELIRNYELV_V24H9_V_V00", "altalanos_feliratoknyelvei", values, ",")#altalanos_feliratoknyelvei
regkfSidetable("GOA24.W_VK_OSAP_REGKFHELYJELLEG_V24H9_V_V00", "altalanos_helyszinjellege", values, ",")#altalanos_helyszinjellege
regkfSidetable("GOA24.W_VK_OSAP_REGKFFOGLALAS_V24H9_V_V00", "altalanos_szemelyesfoglalaslehetosegek", values, ",")#altalanos_szemelyesfoglalaslehetosegek
regkfSidetable("GOA24.W_VK_OSAP_REGKFNYITVA_V24H9_V_V00", "altalanos_vonzeronyitvavan", values, ",")#altalanos_vonzeronyitvavan
regkfSidetable("GOA24.W_VK_OSAP_REGKFGAZDUTAL_V24H9_V_V00", "gazdasagi_utalvanyok", values, ",")#gazdasagi_utalvanyok
regkfSidetable("GOA24.W_VK_OSAP_REGKFSZEPKAR_V24H9_V_V00", "gazdasagi_szepkartyak", values, ",")#gazdasagi_szepkartyak
regkfSidetable("GOA24.W_VK_OSAP_REGKFFURDO_V24H9_V_V00", "furdoegysegek", values, ",")#furdoegysegek
regkfSidetable("GOA24.W_VK_OSAP_REGKFBEAUTY_V24H9_V_V00", "beautyszolgaltatasok", values, ",")#beautyszolgaltatasok
regkfSidetable("GOA24.W_VK_OSAP_REGKFBARAT_V24H9_V_V00", "csaladbaratszolgaltatasok", values, ",")#csaladbaratszolgaltatasok
regkfSidetable("GOA24.W_VK_OSAP_REGKFMEGORZO_V24H9_V_V00", "egeszsegmegorzoszolgaltatasok", values, ",")#egeszsegmegorzoszolgaltatasok

cur.close()

