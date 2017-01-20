import datetime
import time
import os


today = datetime.datetime.today().strftime('%Y-%m-%d')
os.system("date 2016/5/9")
os.startfile(r"C:\Program Files (x86)\PremiumSoft\Navicat Premium\navicat.exe")
time.sleep(1)
try:
    import win32api
    win32api.keybd_event(9,0,0,0)
    win32api.keybd_event(13,0,0,0)
except e:
    print e.message
finally:
    pass
os.system("date "+today)


