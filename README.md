# Convert data from Hyperion.


1.Install python: https://www.python.org/downloads/  
2. install vscode: https://code.visualstudio.com/download
3. install git:  https://git-scm.com/download/win
4. git clone https://github.com/nvetrov/DP.gitto D:\HyperPy\
####Creating a virtual environment
https://mothergeo-py.readthedocs.io/en/latest/development/how-to/venv-win.html

![image](https://user-images.githubusercontent.com/17742497/209914640-709a366f-042a-4020-aa5a-5351c1dd3d4e.png)




<code>
D:\HyperPy\DP>py -m venv env
.\env\Scripts\activate.bat
pip install -r .\requirements.txt
<code/>
####выдать права на папки: 
Дата: D:\Export\ 
Запуск: D:\HyperCmd\
Лог: D:\HyperLog\


### Загрузить изменения с сервера
<code> git pull  </code> 
### Установить dask
python -m pip install "dask[complete]"
