from time import sleep
from bs4 import BeautifulSoup
from selenium import webdriver


def imagini(supa):  # functie pentru a extrage imagini de pe un site si a le scrie intr-un fisier cu append ca sa nu
    # suprascriem fisierul daca ar fi doar cu write
    img = supa.find_all('img')
    with open('imagini.txt', 'a') as file:
        for i in img:
            if i.get('src').startswith('https'): # nu am pus conditie sa inceapa cu  http://nume-domeniu.ro pentru ca
                # site-ul pe care am testat aveau imaginile c
                file.write(i.get('src') + '\n')


def linkuri(s):  # functie pentru a gasi doar linkuri unice care contin  http://nume-domeniu.ro primit ca parametru
    # de functie
    links = soup.find_all('a')
    l = set()
    for el in links:
        try:
            if el.get('href').startswith(s):
                l.add(el.get('href'))
        except Exception:
            continue
    return l


def links100():
    links = linkuri(site)
    counter = 0
    for e in links:
        driver.get(e)
        sleep(5)
        driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);")  # dăm scroll down pentru a se încărca toată pagina și
        # a avea  codul html complet
        sleep(5)  # așteptăm 10 sec pentru a se încărca pagina completă
        html_page = driver.page_source.encode('utf-8')
        supa = BeautifulSoup(html_page, 'lxml')
        imagini(supa)
        counter += 1
        if counter == 100:
            break


driver = webdriver.Chrome('./chromedriver.exe')
site = 'https://www.emag.ro/'
driver.get(site)
sleep(3)  # așteptăm 3 sec pentru a se încărca pagina
driver.refresh()  # dăm refresh la pagină
sleep(5)  # așteptăm 5 sec pentru a se încărca pagina
driver.execute_script(
    "window.scrollTo(0, document.body.scrollHeight);")  # dăm scroll down pentru a se încărca toată pagina și a avea
# codul html complet
sleep(5)  # așteptăm 5 sec pentru a se încărca pagina completă
html = driver.page_source.encode('utf-8')
soup = BeautifulSoup(html, 'lxml')

imagini(soup)
links100()
driver.quit()  # dupa ce am parcurs linkurile unice de pe prima pagina( maxim 100) din multime atunci se opreste
# executia programului si inchidem driverul
