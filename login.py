from kiteconnect import KiteConnect
from selenium import webdriver
from jproperties import Properties
import time


configs = Properties()
with open("app-config.properties", 'rb') as config_file:
    configs.load(config_file)
    ChromeDriverLoc = configs.get("ProjectLocation").data

token_path = "login_info.txt"
key_secret = open(token_path, 'r').read().split()

def autologin():
    kite = KiteConnect(api_key=key_secret[0])
    driver = webdriver.Chrome(executable_path=ChromeDriverLoc + 'chromedriver.exe')
    driver.get((kite.login_url()))
    driver.implicitly_wait(5)
    username = driver.find_element_by_css_selector('#userid')
    password = driver.find_element_by_css_selector('#password')
    username.send_keys(key_secret[2])
    password.send_keys(key_secret[3])
    driver.implicitly_wait(10)
    driver.find_element_by_xpath(
         '/html[1]/body[1]/div[1]/div[1]/div[2]/div[1]/div[1]/div[1]/div[2]/form[1]/div[4]/button[1]').click()
    pin = driver.find_element_by_css_selector('#pin')
    pin.send_keys(key_secret[4])
    driver.find_element_by_xpath("//button[contains(text(),'Continue')]").click()
    time.sleep(10)
    request_token = driver.current_url.split('request_token=')[1].split('&action')[0]
    print(request_token)
    with open('request_token.txt', 'w') as the_file:
        the_file.write(request_token)
    data = kite.generate_session(request_token, api_secret=key_secret[1])
    access_token = data["access_token"]
    print(access_token)
    with open('access_token.txt', 'w') as the_file:
        the_file.write(access_token)
    driver.quit()


def set_kite_obj():
    token_path = "login_info.txt"
    key_secret = open(token_path, 'r').read().split()
    api_key = key_secret[0]
    # api_secret = key_secret[0]
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(open('access_token.txt', 'r').read())
    return kite

access_token = open('access_token.txt', 'r').read()

#autologin()
#set_kite_obj()