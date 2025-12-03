import requests
import os
print('开始下载数据文件...')
url1='https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query=select+*+from+q1_q17_dr25_koi&format=csv'
save_path1=r'D:\u盘\u盘\大学\手打代码\统计学习\koi.csv'
os.makedirs(os.path.dirname(save_path1), exist_ok=True)
response1=requests.get(url1)
with open(save_path1,'wb') as f:
    f.write(response1.content)
print('koi.csv downloaded successfully.')
url2='https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query=select+*+from+k2pandc&format=csv'
save_path2=r'D:\u盘\u盘\大学\手打代码\统计学习\k2.csv'
os.makedirs(os.path.dirname(save_path2), exist_ok=True)
response2=requests.get(url2)
with open(save_path2,'wb') as f:
    f.write(response2.content)
print('k2.csv downloaded successfully.')
url3='https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query=select+*+from+toi&format=csv'
save_path3=r'D:\u盘\u盘\大学\手打代码\统计学习\toi.csv'
os.makedirs(os.path.dirname(save_path3), exist_ok=True)
response3=requests.get(url3)
with open(save_path3,'wb') as f:
    f.write(response3.content)
print('toi.csv downloaded successfully.')